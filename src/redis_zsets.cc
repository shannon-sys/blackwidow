//  Copyright (c) 2017-present The blackwidow Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.


#include "src/redis_zsets.h"

#include <limits>
#include <unordered_map>

#include <iostream>
#include "blackwidow/util.h"
#include "src/zsets_filter.h"
#include "src/scope_record_lock.h"
#include "src/scope_snapshot.h"
#include "src/unordered_map_cache_lock.h"
#include "src/skip_list.h"

using namespace std;
#define assert(x) { if (x) {} else { \
            printf("file:%s str:%s line:%d\n", __FILE__, __STRING(x), __LINE__); \
            exit(-1); \
        }}
namespace blackwidow {
RedisZSets::RedisZSets(BlackWidow* const bw, const DataType& type)
    : Redis(bw, type) {
}

shannon::Comparator* ZSetsScoreKeyComparator() {
  static ZSetsScoreKeyComparatorImpl zsets_score_key_compare;
  return &zsets_score_key_compare;
}

RedisZSets::~RedisZSets() {
  std::vector<shannon::ColumnFamilyHandle*> tmp_handles = handles_;
  handles_.clear();
  for (auto handle : tmp_handles) {
    delete handle;
  }
}

Status RedisZSets::Open(const BlackwidowOptions& bw_options,
                        const std::string& db_path) {
  statistics_store_.max_size_ = bw_options.statistics_max_size;
  small_compaction_threshold_ = bw_options.small_compaction_threshold;

  shannon::Options ops(bw_options.options);
  Status s = shannon::DB::Open(ops, db_path, default_device_name_, &db_);
  if (s.ok()) {
    shannon::ColumnFamilyHandle *dcf = nullptr, *scf = nullptr , *tcf = nullptr;
    s = db_->CreateColumnFamily(shannon::ColumnFamilyOptions(), "data_cf", &dcf);
    if (!s.ok()) {
      return s;
    }

    s = db_->CreateColumnFamily(shannon::ColumnFamilyOptions(), "timeout_cf", &tcf);

    if (!s.ok()) {
      return s;
    }
    delete scf;
    delete dcf;
    delete tcf;
    delete db_;
  }

  shannon::DBOptions db_ops(bw_options.options);
  shannon::ColumnFamilyOptions meta_cf_ops(bw_options.options);
  shannon::ColumnFamilyOptions data_cf_ops(bw_options.options);
  shannon::ColumnFamilyOptions score_cf_ops(bw_options.options);
  shannon::ColumnFamilyOptions timeout_cf_ops(bw_options.options);
  meta_cf_ops.compaction_filter_factory =
    std::make_shared<ZSetsMetaFilterFactory>();
  data_cf_ops.compaction_filter_factory =
    std::make_shared<ZSetsDataFilterFactory>(&db_, &handles_);
  score_cf_ops.compaction_filter_factory =
    std::make_shared<ZSetsScoreFilterFactory>(&db_, &handles_);

  //use the bloom filter policy to reduce disk reads
  std::vector<shannon::ColumnFamilyDescriptor> column_families;
  column_families.push_back(shannon::ColumnFamilyDescriptor(
        shannon::kDefaultColumnFamilyName, meta_cf_ops));
  column_families.push_back(shannon::ColumnFamilyDescriptor(
        "data_cf", data_cf_ops));
  column_families.push_back(shannon::ColumnFamilyDescriptor(
        "timeout_cf", shannon::ColumnFamilyOptions()));
  s = shannon::DB::Open(db_ops, db_path, default_device_name_, column_families, &handles_, &db_);
  if (s.ok()) {
    vdb_ = new VDB(db_);
  }
  return s;
}

Status RedisZSets::CompactRange(const shannon::Slice* begin,
                                const shannon::Slice* end,
                                const ColumnFamilyType& type) {
  if (type == kMeta || type == kMetaAndData) {
    db_->CompactRange(default_compact_range_options_, handles_[0], begin, end);
  }
  if (type == kData || type == kMetaAndData) {
    db_->CompactRange(default_compact_range_options_, handles_[1], begin, end);
  }
  return Status::OK();
}

Status RedisZSets::AddDelKey (BlackWidow * bw,const string & str){
  Status s =   bw->AddDelKey(db_, str, handles_[1]);
  return s;
};

Status RedisZSets::GetProperty(const std::string& property, uint64_t* out) {
  std::string value;
  db_->GetProperty(handles_[0], property, &value);
  *out = std::strtoull(value.c_str(), NULL, 10);
  db_->GetProperty(handles_[1], property, &value);
  *out += std::strtoull(value.c_str(), NULL, 10);
  return Status::OK();
}

Status RedisZSets::ScanKeyNum(KeyInfo* key_info) {
  uint64_t keys = 0;
  uint64_t expires = 0;
  uint64_t ttl_sum = 0;
  uint64_t invaild_keys = 0;

  shannon::ReadOptions iterator_options;
  const shannon::Snapshot* snapshot;
  ScopeSnapshot ss(db_, &snapshot);
  iterator_options.snapshot = snapshot;
  iterator_options.fill_cache = false;

  int64_t curtime;
  shannon::Env::Default()->GetCurrentTime(&curtime);

  shannon::Iterator* iter = db_->NewIterator(iterator_options, handles_[0]);
  for (iter->SeekToFirst();
       iter->Valid();
       iter->Next()) {
    ParsedZSetsMetaValue parsed_zsets_meta_value(iter->value());
    if (parsed_zsets_meta_value.IsStale()
      || parsed_zsets_meta_value.count() == 0) {
      invaild_keys++;
    } else {
      keys++;
      if (!parsed_zsets_meta_value.IsPermanentSurvival()) {
        expires++;
        ttl_sum += parsed_zsets_meta_value.timestamp() - curtime;
      }
    }
  }
  delete iter;

  key_info->keys = keys;
  key_info->expires = expires;
  key_info->avg_ttl = (expires != 0) ? ttl_sum / expires : 0;
  key_info->invaild_keys = invaild_keys;
  return Status::OK();
}

Status RedisZSets::ScanKeys(const std::string& pattern,
                            std::vector<std::string>* keys) {

  std::string key;
  shannon::ReadOptions iterator_options;
  const shannon::Snapshot* snapshot;
  ScopeSnapshot ss(db_, &snapshot);
  iterator_options.snapshot = snapshot;
  iterator_options.fill_cache = false;

  shannon::Iterator* iter = db_->NewIterator(iterator_options, handles_[0]);
  for (iter->SeekToFirst();
       iter->Valid();
       iter->Next()) {
    ParsedZSetsMetaValue parsed_zsets_meta_value(iter->value());
    if (!parsed_zsets_meta_value.IsStale()
      && parsed_zsets_meta_value.count() != 0) {
      key = iter->key().ToString();
      if (StringMatch(pattern.data(), pattern.size(), key.data(), key.size(), 0)) {
        keys->push_back(key);
      }
    }
  }
  delete iter;
  return Status::OK();
}

Status RedisZSets::ZAdd(const Slice& key,
                        const std::vector<ScoreMember>& score_members,
                        int32_t* ret) {
  *ret = 0;
  uint32_t statistic = 0;
  std::unordered_set<std::string> unique;
  std::vector<ScoreMember> filtered_score_members;
  for (const auto& sm : score_members) {
    if (unique.find(sm.member) == unique.end()) {
      unique.insert(sm.member);
      filtered_score_members.push_back(sm);
    }
  }

  std::string meta_value;
  std::string score_value;
  VWriteBatch batch;
  ScopeRecordLock l(lock_mgr_, key);
  Status s;
  // get meta value
  s = db_->Get(shannon::ReadOptions(), handles_[0], key, &meta_value);
  if (s.ok()) {
    // get score value
    s = db_->Get(shannon::ReadOptions(), handles_[1], key, &score_value);
    assert(s.ok());
    bool valid = true;
    ParsedZSetsMetaValue parsed_zsets_meta_value(&meta_value);
    if (parsed_zsets_meta_value.IsStale()
      || parsed_zsets_meta_value.count() == 0) {
      valid = false;
      // add overdue data flag
    } else {
      valid = true;
    }
    //
    SkipList skiplist_member_score(&meta_value, ZSET_PREFIX_LENGTH);
    SkipList skiplist_score_member(&score_value, ZSET_PREFIX_LENGTH);
    assert(skiplist_member_score.count() == parsed_zsets_meta_value.count());
    SkipList::Iterator *member_score_iter = skiplist_member_score.NewIterator();
    int32_t cnt = 0;
    std::string data_value;
    for (const auto& sm : filtered_score_members) {
      bool not_found = true;
      // have been data
      if (valid) {
        member_score_iter->Seek(sm.member);
        if (member_score_iter->Valid()) {
          not_found = false;
          ParsedZSetsMemberScore parsed_zsets_member_score(member_score_iter->key());
          double old_score = parsed_zsets_member_score.score();
          if (old_score == sm.score) {
            continue;
          } else {
            // member exists and del it
            ZSetsKey old_zsets_key(old_score, sm.member);
            assert(skiplist_member_score.del(old_zsets_key.EncodeMemberScore()));
            assert(skiplist_score_member.del(old_zsets_key.EncodeScoreMember()));
            // delete old zsets_score_key and overwirte zsets_member_key
            // but in different column_families so we accumulative 1
            statistic++;
          }
        }
      }
      ZSetsKey zsets_key(sm.score, sm.member);
      assert(skiplist_member_score.insert(zsets_key.EncodeMemberScore()));
      assert(skiplist_score_member.insert(zsets_key.EncodeScoreMember()));
      if (not_found) {
        cnt++;
      }
    }
    delete member_score_iter;
    assert(skiplist_member_score.count() == skiplist_score_member.count());
    assert(skiplist_member_score.count() == parsed_zsets_meta_value.count() + cnt);
    parsed_zsets_meta_value.ModifyCount(cnt);
    memcpy(const_cast<char*>(score_value.data()), const_cast<char*>(meta_value.data()), ZSET_PREFIX_LENGTH);
    batch.Put(handles_[0], key, meta_value);
    batch.Put(handles_[1], key, score_value);
    *ret = cnt;
  } else {
    SkipList skiplist_member_score(&meta_value, ZSET_PREFIX_LENGTH, true);
    SkipList skiplist_score_member(&score_value, ZSET_PREFIX_LENGTH, true);
    for (const auto& sm : filtered_score_members) {
      ZSetsKey zsets_key(sm.score, sm.member);
      skiplist_member_score.insert(zsets_key.EncodeMemberScore());
      skiplist_score_member.insert(zsets_key.EncodeScoreMember());
    }
    ParsedZSetsMetaValue parsed_zsets_meta_value(&meta_value);
    parsed_zsets_meta_value.InitialMetaValue();
    assert(skiplist_member_score.count() == skiplist_score_member.count());
    parsed_zsets_meta_value.set_count(skiplist_member_score.count());
    memcpy(const_cast<char*>(score_value.data()), const_cast<char*>(meta_value.data()), ZSET_PREFIX_LENGTH);
    assert(skiplist_member_score.count() == parsed_zsets_meta_value.count());
    batch.Put(handles_[0], key, meta_value);
    batch.Put(handles_[1], key, score_value);
    *ret = filtered_score_members.size();
  }
  s = vdb_->Write(default_write_options_, &batch);
  UpdateSpecificKeyStatistics(key.ToString(), statistic);
  return s;
}


Status RedisZSets::ZCard(const Slice& key, int32_t* card) {
  *card = 0;
  std::string meta_value;
  Status s;
  s = db_->Get(shannon::ReadOptions(), handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedZSetsMetaValue parsed_zsets_meta_value(&meta_value);
    if (parsed_zsets_meta_value.IsStale()) {
      *card = 0;
      return Status::NotFound("Stale");
    } else if (parsed_zsets_meta_value.count() == 0) {
      *card = 0;
      return Status::NotFound();
    } else {
      *card = parsed_zsets_meta_value.count();
    }
  } else {
    return Status::NotFound();
  }
  return s;
}

Status RedisZSets::ZCount(const Slice& key,
                          double min,
                          double max,
                          bool left_close,
                          bool right_close,
                          int32_t* ret) {
  *ret = 0;
  shannon::ReadOptions read_options;
  const shannon::Snapshot* snapshot = nullptr;

  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;
  Status s;
  std::string score_value;
  s = db_->Get(read_options, handles_[1], key, &score_value);
  if (s.ok()) {
    ParsedZSetsMetaValue parsed_zsets_meta_value(&score_value);
    if (parsed_zsets_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if (parsed_zsets_meta_value.count() == 0) {
      return Status::NotFound();
    } else {
      SkipList skiplist_score_member(&score_value, ZSET_PREFIX_LENGTH);
      int32_t cnt = 0;
      // ZSetsKey zsets_key_min(min, "");
      // ZSetsKey zsets_key_max(max, "");
      // shannon::Slice key_min = zsets_key_min.EncodeScoreMember();
      // shannon::Slice key_max = zsets_key_max.EncodeScoreMember();
      SkipList::Iterator* iter = skiplist_score_member.NewIterator();
      for (iter->SeekToFirst();
           iter->Valid();
           iter->Next()) {
          bool left_pass = false;
          bool right_pass = false;
          ParsedZSetsScoreMember parsed_zsets_score_key(iter->key());
          if ((left_close && min <= parsed_zsets_score_key.score())
            || (!left_close && min < parsed_zsets_score_key.score())) {
            left_pass = true;
          }
          if ((right_close && parsed_zsets_score_key.score() <= max)
            || (!right_close && parsed_zsets_score_key.score() < max)) {
            right_pass = true;
          }
          if (left_pass && right_pass) {
            cnt++;
          } else if (!right_pass) {
            break;
          }
      }
      delete iter;
      *ret = cnt;
    }
  } else {
      s = Status::NotFound();
  }
  return s;
}

Status RedisZSets::ZIncrby(const Slice& key,
                           const Slice& member,
                           double increment,
                           double* ret) {
  *ret = 0;
  uint32_t statistic = 0;
  double score = 0;
  std::string meta_value;
  std::string score_value;
  VWriteBatch batch;
  ScopeRecordLock l(lock_mgr_, key);
  Status s;
  s = db_->Get(default_read_options_, handles_[0], key, &meta_value);
  if (s.ok()) {
    s = db_->Get(default_read_options_, handles_[1], key, &score_value);
    assert(s.ok());
    ParsedZSetsMetaValue parsed_zsets_meta_value(&meta_value);
    if (parsed_zsets_meta_value.IsStale()
      || parsed_zsets_meta_value.count() == 0) {
      parsed_zsets_meta_value.InitialMetaValue();
    } else {
      parsed_zsets_meta_value.version();
    }
    SkipList skiplist_member_score(&meta_value, ZSET_PREFIX_LENGTH, false);
    SkipList skiplist_score_member(&score_value, ZSET_PREFIX_LENGTH, false);
    std::string data_value;
    SkipList::Iterator *iter = skiplist_member_score.NewIterator();
    iter->Seek(member);
    if (iter->Valid()) {
      std::string key_member_score(iter->key().data(), iter->key().size());
      ParsedZSetsMemberScore parsed_zsets_member_score(&key_member_score);
      ZSetsKey old_zsets_key(member, parsed_zsets_member_score.score());
      ZSetsKey new_zsets_key(parsed_zsets_member_score.member(),
                         parsed_zsets_member_score.score() + increment);
      assert(skiplist_member_score.del(old_zsets_key.EncodeMemberScore()));
      assert(skiplist_score_member.del(old_zsets_key.EncodeScoreMember()));
      assert(skiplist_member_score.insert(new_zsets_key.EncodeMemberScore()));
      assert(skiplist_score_member.insert(new_zsets_key.EncodeScoreMember()));
      score = parsed_zsets_member_score.score() + increment;
      // delete old zsets_score_key and overwrite zsets_member_key
      // but in different column_families so we accumulative 1
      statistic++;
    } else {    // not found
      ZSetsKey zsets_key(member, increment);
      assert(skiplist_member_score.insert(zsets_key.EncodeMemberScore()));
      assert(skiplist_score_member.insert(zsets_key.EncodeScoreMember()));
      parsed_zsets_meta_value.set_count(skiplist_member_score.count());
      score = increment;
    }
    delete iter;
  } else {
    SkipList skiplist_member_score(&meta_value, ZSET_PREFIX_LENGTH, true);
    SkipList skiplist_score_member(&score_value, ZSET_PREFIX_LENGTH, true);
    ParsedZSetsMetaValue parsed_zsets_meta_value(&meta_value);
    parsed_zsets_meta_value.InitialMetaValue();
    parsed_zsets_meta_value.set_count(1);
    ZSetsKey zsets_key(member, increment);
    assert(skiplist_member_score.insert(zsets_key.EncodeMemberScore()));
    assert(skiplist_score_member.insert(zsets_key.EncodeScoreMember()));
    score = increment;
  }
  memcpy(const_cast<char*>(score_value.data()), const_cast<char*>(meta_value.data()), ZSET_PREFIX_LENGTH);
  batch.Put(handles_[0], key, meta_value);
  batch.Put(handles_[1], key, score_value);
  *ret = score;
  s = vdb_->Write(default_write_options_, &batch);
  UpdateSpecificKeyStatistics(key.ToString(), statistic);
  return s;
}

Status RedisZSets::ZRange(const Slice& key,
                          int32_t start,
                          int32_t stop,
                          std::vector<ScoreMember>* score_members) {
  score_members->clear();
  shannon::ReadOptions read_options;
  const shannon::Snapshot* snapshot = nullptr;

  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;
  Status s;
  std::string score_value;

  s = db_->Get(read_options, handles_[1], key, &score_value);
  if (s.ok()) {
    ParsedZSetsMetaValue parsed_zsets_meta_value(&score_value);
    if (parsed_zsets_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if (parsed_zsets_meta_value.count() == 0) {
      return Status::NotFound();
    } else {
      int32_t count = parsed_zsets_meta_value.count();
      int32_t start_index = start >= 0 ? start : count + start;
      int32_t stop_index  = stop  >= 0 ? stop  : count + stop;
      start_index = start_index <= 0 ? 0 : start_index;
      stop_index = stop_index >= count ? count - 1 : stop_index;
      if (start_index > stop_index
        || start_index >= count
        || stop_index < 0) {
        return s;
      }
      int32_t cur_index = 0;
      ScoreMember score_member;
      SkipList skiplist(&score_value, ZSET_PREFIX_LENGTH);
      assert(parsed_zsets_meta_value.count() == skiplist.count());
      SkipList::Iterator* iter = skiplist.NewIterator();
      for (iter->SeekToFirst();
           iter->Valid() && cur_index <= stop_index;
           iter->Next(), ++cur_index) {
        if (cur_index >= start_index) {
          ParsedZSetsScoreMember parsed_zsets_score_member(iter->key());
          score_member.score = parsed_zsets_score_member.score();
          score_member.member = parsed_zsets_score_member.member().ToString();
          score_members->push_back(score_member);
        }
      }
      delete iter;
    }
  } else {
      s = Status::NotFound();
  }
  return s;
}

Status RedisZSets::ZRangebyscore(const Slice& key,
                                 double min,
                                 double max,
                                 bool left_close,
                                 bool right_close,
                                 std::vector<ScoreMember>* score_members) {
  score_members->clear();
  shannon::ReadOptions read_options;
  const shannon::Snapshot* snapshot = nullptr;

  std::string score_value;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;
  Status s;

  s = db_->Get(read_options, handles_[1], key, &score_value);
  if (s.ok()) {
    ParsedZSetsMetaValue parsed_zsets_meta_value(&score_value);
    if (parsed_zsets_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if (parsed_zsets_meta_value.count() == 0) {
      return Status::NotFound();
    } else {
      SkipList skiplist(&score_value, ZSET_PREFIX_LENGTH, false);
      assert(skiplist.count() == parsed_zsets_meta_value.count());
      int32_t index = 0;
      int32_t stop_index = parsed_zsets_meta_value.count() - 1;
      SkipList::Iterator *iter = skiplist.NewIterator();
      for (iter->SeekToFirst();
           iter->Valid() && index <= stop_index;
           iter->Next(), ++index) {
        bool left_pass = false;
        bool right_pass = false;
        ScoreMember score_member;
        ParsedZSetsScoreMember parsed_zsets_score_member(iter->key());
        if ((left_close && min <= parsed_zsets_score_member.score())
          || (!left_close && min < parsed_zsets_score_member.score())) {
          left_pass = true;
        }
        if ((right_close && parsed_zsets_score_member.score() <= max)
          || (!right_close && parsed_zsets_score_member.score() < max)) {
          right_pass = true;
        }
        if (left_pass && right_pass) {
          score_member.score = parsed_zsets_score_member.score();
          score_member.member = parsed_zsets_score_member.member().ToString();
          score_members->push_back(score_member);
        }
        if (!right_pass) {
          break;
        }
      }
      delete iter;
    }
  } else {
    s = Status::NotFound();
  }
  return s;
}

Status RedisZSets::ZRank(const Slice& key,
                         const Slice& member,
                         int32_t* rank) {
  *rank = -1;
  shannon::ReadOptions read_options;
  const shannon::Snapshot* snapshot = nullptr;

  std::string meta_value;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;
  Status s;
  s = db_->Get(read_options, handles_[1], key, &meta_value);
  if (s.ok()) {
    ParsedZSetsMetaValue  parsed_zsets_meta_value(&meta_value);
    if (parsed_zsets_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if(parsed_zsets_meta_value.count() == 0) {
      return Status::NotFound();
    } else {
      SkipList skiplist(&meta_value, ZSET_PREFIX_LENGTH, false);
      bool found = false;
      int32_t index = 0;
      int32_t stop_index = parsed_zsets_meta_value.count() - 1;
      ScoreMember score_member;
      SkipList::Iterator* iter = skiplist.NewIterator();
      for (iter->SeekToFirst();
           iter->Valid() && index <= stop_index;
           iter->Next(), ++index) {
          ParsedZSetsScoreMember parsed_zsets_score_member(iter->key());
          if (!parsed_zsets_score_member.member().compare(member)) {
            found = true;
            break;
          }
      }
      delete iter;
      if (found) {
        *rank = index;
        return Status::OK();
      } else {
        return Status::NotFound();
      }
    }
  } else {
    s = Status::NotFound();
  }
  return s;
}

Status RedisZSets::ZRem(const Slice& key,
                        std::vector<std::string> members,
                        int32_t* ret) {
  *ret = 0;
  uint32_t statistic = 0;
  std::unordered_set<std::string> unique;
  std::vector<std::string> filtered_members;
  for (const auto& member : members) {
    if (unique.find(member) == unique.end()) {
      unique.insert(member);
      filtered_members.push_back(member);
    }
  }

  std::string meta_value;
  std::string score_value;
  VWriteBatch batch;
  ScopeRecordLock l(lock_mgr_, key);
  Status s;

  s = db_->Get(shannon::ReadOptions(), handles_[0], key, &meta_value);
  if (s.ok()) {
    s = db_->Get(shannon::ReadOptions(), handles_[1], key, &score_value);
    assert(s.ok());
    ParsedZSetsMetaValue parsed_zsets_meta_value(&meta_value);
    if (parsed_zsets_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if (parsed_zsets_meta_value.count() == 0) {
      return Status::NotFound();
    } else {
      SkipList skiplist_meta(&meta_value, ZSET_PREFIX_LENGTH, false);
      SkipList skiplist_score(&score_value, ZSET_PREFIX_LENGTH, false);
      assert(skiplist_meta.count() == skiplist_score.count());
      assert(meta_value.size() == score_value.size());
      int32_t del_cnt = 0;
      std::string data_value;
      SkipList::Iterator *iter = skiplist_meta.NewIterator();
      // delete iter;
      for (const auto& member : filtered_members) {
        iter->Seek(member);
        if (iter->Valid()) {
          del_cnt++;
          statistic++;
          std::string key(iter->key().data(), iter->key().size());
          ParsedZSetsMemberScore parsed_zsets_member_score(key);
          ZSetsKey zsets_key(parsed_zsets_member_score.score(), parsed_zsets_member_score.member());
          assert(iter->key().size() == zsets_key.EncodeMemberScore().size());
          assert(skiplist_meta.del(key));
          assert(skiplist_score.del(zsets_key.EncodeScoreMember()));
        } else {
          delete iter;
          return Status::NotFound();
        }
      }
      delete iter;
      *ret = del_cnt;
      parsed_zsets_meta_value.ModifyCount(-del_cnt);
      assert(parsed_zsets_meta_value.count() == skiplist_meta.count());
      memcpy(const_cast<char*>(score_value.data()), meta_value.data(), ZSET_PREFIX_LENGTH);
      batch.Put(handles_[0], key, meta_value);
      batch.Put(handles_[1], key, score_value);
    }
  } else {
    return Status::NotFound();
  }
  s = vdb_->Write(default_write_options_, &batch);
  UpdateSpecificKeyStatistics(key.ToString(), statistic);
  return s;
}

Status RedisZSets::ZRemrangebyrank(const Slice& key,
                                   int32_t start,
                                   int32_t stop,
                                   int32_t* ret) {
  *ret = 0;
  uint32_t statistic = 0;
  std::string score_value;
  std::string meta_value;
  VWriteBatch batch;
  ScopeRecordLock l(lock_mgr_, key);
  Status s;

  s = db_->Get(default_read_options_, handles_[1], key, &score_value);
  if (s.ok()) {
    ParsedZSetsMetaValue parsed_zsets_score_value(&score_value);
    if (parsed_zsets_score_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if (parsed_zsets_score_value.count() == 0) {
      return Status::NotFound();
    } else {
      std::string member;
      int32_t del_cnt = 0;
      int32_t cur_index = 0;
      int32_t count = parsed_zsets_score_value.count();
      int32_t start_index = start >= 0 ? start : count + start;
      int32_t stop_index  = stop  >= 0 ? stop  : count + stop;
      // 判断要删除的rank排名区间是否在范围内，如果不在，则不用读取第二个了
      if (start_index >= parsed_zsets_score_value.count()) {
        return s;
      }
      s = db_->Get(default_read_options_, handles_[0], key, &meta_value);
      assert(s.ok());
      ParsedZSetsMetaValue parsed_zsets_meta_value(&meta_value);
      start_index = start_index <= 0 ? 0 : start_index;
      stop_index = stop_index >= count ? count - 1 : stop_index;
      SkipList skiplist_score_member(&score_value, ZSET_PREFIX_LENGTH, false);
      SkipList skiplist_member_score(&meta_value, ZSET_PREFIX_LENGTH, false);
      SkipList::Iterator* iter = skiplist_score_member.NewIterator();
      std::vector<std::string> del_keys;
      for (iter->SeekToFirst();
           iter->Valid() && cur_index <= stop_index;
           iter->Next(), ++cur_index) {
        if (cur_index >= start_index) {
          del_keys.push_back(std::string(iter->key().data(), iter->key().size()));
          del_cnt++;
          statistic++;
        }
      }
      for (auto key : del_keys) {
        ParsedZSetsScoreMember parsed_zsets_score_member(key);
        ZSetsKey zsets_key(parsed_zsets_score_member.score(), parsed_zsets_score_member.member());
        assert(skiplist_score_member.del(zsets_key.EncodeScoreMember()));
        assert(skiplist_member_score.del(zsets_key.EncodeMemberScore()));
      }
      delete iter;
      *ret = del_cnt;
      parsed_zsets_meta_value.ModifyCount(-del_cnt);
      memcpy(const_cast<char*>(score_value.data()), const_cast<char*>(meta_value.data()), ZSET_PREFIX_LENGTH);
      batch.Put(handles_[0], key, meta_value);
      batch.Put(handles_[1], key, score_value);
      s = vdb_->Write(default_write_options_, &batch);
    }
  } else {
    return Status::NotFound();
  }
  // UpdateSpecificKeyStatistics(key.ToString(), statistic);
  return s;
}

Status RedisZSets::ZRemrangebyscore(const Slice& key,
                                    double min,
                                    double max,
                                    bool left_close,
                                    bool right_close,
                                    int32_t* ret) {
  *ret = 0;
  uint32_t statistic = 0;
  std::string score_value;
  std::string meta_value;
  VWriteBatch batch;
  ScopeRecordLock l(lock_mgr_, key);
  Status s;

  s = db_->Get(shannon::ReadOptions(), handles_[1], key, &score_value);
  if (s.ok()) {
    ParsedZSetsMetaValue parsed_zsets_score_value(&score_value);
    if (parsed_zsets_score_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if (parsed_zsets_score_value.count() == 0) {
      return Status::NotFound();
    } else {
      std::string member;
      int32_t del_cnt = 0;
      int32_t cur_index = 0;
      int32_t stop_index = parsed_zsets_score_value.count() - 1;
      SkipList skiplist_score_member(&score_value, ZSET_PREFIX_LENGTH, false);
      SkipList::Iterator* iter = skiplist_score_member.NewIterator();
      std::vector<std::string> del_keys;
      for (iter->SeekToFirst();
           iter->Valid() && cur_index <= stop_index;
           iter->Next(), ++cur_index) {
        bool left_pass = false;
        bool right_pass = false;
        ParsedZSetsScoreMember parsed_zsets_score_key(iter->key());
        if ((left_close && min <= parsed_zsets_score_key.score())
          || (!left_close && min < parsed_zsets_score_key.score())) {
          left_pass = true;
        }
        if ((right_close && parsed_zsets_score_key.score() <= max)
          || (!right_close && parsed_zsets_score_key.score() < max)) {
          right_pass = true;
        }
        if (left_pass && right_pass) {
          del_keys.push_back(std::string(iter->key().data(), iter->key().size()));
          del_cnt++;
          statistic++;
        }
        if (!right_pass) {
          break;
        }
      }
      delete iter;
      if (del_keys.size() > 0) {
        s = db_->Get(default_read_options_, handles_[0], key, &meta_value);
        assert(s.ok());
        ParsedZSetsMetaValue parsed_zsets_meta_value(&meta_value);
        SkipList skiplist_member_score(&meta_value, ZSET_PREFIX_LENGTH, false);
        for (auto key : del_keys) {
          ParsedZSetsScoreMember score_member(key);
          ZSetsKey zsets_key(score_member.score(), score_member.member());
          assert(skiplist_member_score.del(zsets_key.EncodeMemberScore()));
          assert(skiplist_score_member.del(zsets_key.EncodeScoreMember()));
        }
        parsed_zsets_meta_value.ModifyCount(-del_cnt);
      }
      *ret = del_cnt;
      memcpy(const_cast<char*>(score_value.data()), const_cast<char*>(meta_value.data()), ZSET_PREFIX_LENGTH);
      batch.Put(handles_[0], key, meta_value);
      batch.Put(handles_[1], key, score_value);
    }
  } else {
    return Status::NotFound();
  }
  s = vdb_->Write(default_write_options_, &batch);
  UpdateSpecificKeyStatistics(key.ToString(), statistic);
  return s;
}

Status RedisZSets::ZRevrange(const Slice& key,
                             int32_t start,
                             int32_t stop,
                             std::vector<ScoreMember>* score_members) {
  score_members->clear();
  shannon::ReadOptions read_options;
  const shannon::Snapshot* snapshot = nullptr;

  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;
  Status s;
  std::string score_value;
  s = db_->Get(read_options, handles_[1], key, &score_value);
  if (s.ok()) {
    ParsedZSetsMetaValue parsed_zsets_meta_value(&score_value);
    if (parsed_zsets_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if (parsed_zsets_meta_value.count() == 0) {
      return Status::NotFound();
    } else {
      int32_t count = parsed_zsets_meta_value.count();
      int32_t start_index = stop >= 0 ? count - stop - 1 : -stop - 1;
      int32_t stop_index  = start >= 0 ? count- start - 1 : -start - 1;
      start_index = start_index <= 0 ? 0 : start_index;
      stop_index = stop_index >= count ? count - 1 : stop_index;
      if (start_index > stop_index
        || start_index >= count
        || stop_index < 0) {
        return s;
      }
      int32_t cur_index = count - 1;
      ScoreMember score_member;
      SkipList skiplist(&score_value, ZSET_PREFIX_LENGTH, false);
      SkipList::Iterator* iter = skiplist.NewIterator();
      for (iter->SeekToLast();
           iter->Valid() && cur_index >= start_index;
           iter->Prev(), --cur_index) {
        if (cur_index <= stop_index) {
          ParsedZSetsScoreMember parsed_zsets_score_member(iter->key());
          score_member.score = parsed_zsets_score_member.score();
          score_member.member = parsed_zsets_score_member.member().ToString();
          score_members->push_back(score_member);
        }
      }
      delete iter;
    }
  } else {
      s = Status::NotFound();
  }
  return s;
}

Status RedisZSets::ZRevrangebyscore(const Slice& key,
                                    double min,
                                    double max,
                                    bool left_close,
                                    bool right_close,
                                    std::vector<ScoreMember>* score_members) {
  score_members->clear();
  shannon::ReadOptions read_options;
  const shannon::Snapshot* snapshot = nullptr;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;
  std::string score_value;
  Status s;

  s = db_->Get(read_options, handles_[1], key, &score_value);
  if (s.ok()) {
    ParsedZSetsMetaValue parsed_zsets_meta_value(&score_value);
    if (parsed_zsets_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if (parsed_zsets_meta_value.count() == 0) {
      return Status::NotFound();
    } else {
      int32_t left = parsed_zsets_meta_value.count();
      ScoreMember score_member;
      SkipList skiplist(&score_value, ZSET_PREFIX_LENGTH, false);
      SkipList::Iterator* iter = skiplist.NewIterator();
      for (iter->SeekToLast();
           iter->Valid();
           iter->Prev(), --left) {
        bool left_pass = false;
        bool right_pass = false;
        ParsedZSetsScoreMember parsed_zsets_score_member(iter->key());
        if ((left_close && min <= parsed_zsets_score_member.score())
          || (!left_close && min < parsed_zsets_score_member.score())) {
          left_pass = true;
        }
        if ((right_close && parsed_zsets_score_member.score() <= max)
          || (!right_close && parsed_zsets_score_member.score() < max)) {
          right_pass = true;
        }
        if (left_pass && right_pass) {
          score_member.score = parsed_zsets_score_member.score();
          score_member.member = parsed_zsets_score_member.member().ToString();
          score_members->push_back(score_member);
        }
        if (!left_pass) {
          break;
        }
      }
      delete iter;
      assert(left == 0);
    }
  } else {
      s = Status::NotFound();
  }
  return s;
}

Status RedisZSets::ZRevrank(const Slice& key,
                            const Slice& member,
                            int32_t* rank) {
  *rank = -1;
  shannon::ReadOptions read_options;
  const shannon::Snapshot* snapshot = nullptr;

  std::string score_value;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;
  Status s;

  s = db_->Get(read_options, handles_[1], key, &score_value);
  if (s.ok()) {
    ParsedZSetsMetaValue parsed_zsets_meta_value(&score_value);
    if (parsed_zsets_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if (parsed_zsets_meta_value.count() == 0) {
      return Status::NotFound();
    } else {
      bool found = false;
      int32_t rev_index = 0;
      int32_t left = parsed_zsets_meta_value.count();
      SkipList skiplist(&score_value, ZSET_PREFIX_LENGTH, false);
      SkipList::Iterator* iter = skiplist.NewIterator();
      for (iter->SeekToLast();
           iter->Valid();
           iter->Prev(), --left, ++rev_index) {
        ParsedZSetsScoreMember parsed_zsets_score_member(iter->key());
        if (!parsed_zsets_score_member.member().compare(member)) {
          found = true;
          break;
        }
      }
      delete iter;
      if (found) {
        *rank = rev_index;
        assert(rev_index < parsed_zsets_meta_value.count());
      } else {
        return Status::NotFound();
      }
    }
  } else {
      s = Status::NotFound();
  }
  return s;
}

Status RedisZSets::ZScore(const Slice& key, const Slice& member, double* score) {

  *score = 0;
  shannon::ReadOptions read_options;
  const shannon::Snapshot* snapshot = nullptr;

  std::string meta_value;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;
  Status s;

  s = db_->Get(read_options, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedZSetsMetaValue parsed_zsets_meta_value(&meta_value);
    if (parsed_zsets_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if (parsed_zsets_meta_value.count() == 0) {
      return Status::NotFound();
    } else {
      SkipList skiplist(&meta_value, ZSET_PREFIX_LENGTH, false);
      SkipList::Iterator *iter = skiplist.NewIterator();
      iter->Seek(member);
      if (iter->Valid()) {
        ParsedZSetsMemberScore parsed_zsets_member_score(iter->key());
        double tmp = parsed_zsets_member_score.score();
        *score = tmp;
      } else {
        delete iter;
        return s;
      }
      delete iter;
    }
  } else {
    return Status::NotFound();
  }
  return s;
}

Status RedisZSets::ZUnionstore(const Slice& destination,
                               const std::vector<std::string>& keys,
                               const std::vector<double>& weights,
                               const AGGREGATE agg,
                               int32_t* ret) {
  *ret = 0;
  uint32_t statistic = 0;
  VWriteBatch batch;
  shannon::ReadOptions read_options;
  const shannon::Snapshot* snapshot = nullptr;

  std::string score_value;
  ScoreMember sm;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;
  ScopeRecordLock l(lock_mgr_, destination);
  std::map<std::string, double> member_score_map;

  Status s;
  for (size_t idx = 0; idx < keys.size(); ++idx) {
    s = db_->Get(read_options, handles_[1], keys[idx], &score_value);
    if (s.ok()) {
      ParsedZSetsMetaValue parsed_zsets_meta_value(&score_value);
      if (!parsed_zsets_meta_value.IsStale()
        && parsed_zsets_meta_value.count() != 0) {
        int32_t cur_index = 0;
        int32_t stop_index = parsed_zsets_meta_value.count() - 1;
        double score = 0;
        double weight = idx < weights.size() ? weights[idx] : 1;
        parsed_zsets_meta_value.version();
        SkipList skiplist(&score_value, ZSET_PREFIX_LENGTH, false);
        SkipList::Iterator* iter = skiplist.NewIterator();
        for (iter->SeekToFirst();
             iter->Valid() && cur_index <= stop_index;
             iter->Next(), ++cur_index) {
          ParsedZSetsScoreMember parsed_zsets_score_member(iter->key());
          sm.score = parsed_zsets_score_member.score();
          sm.member = parsed_zsets_score_member.member().ToString();
          if (member_score_map.find(sm.member) == member_score_map.end()) {
            score = weight * sm.score;
            member_score_map[sm.member] = (score == -0.0) ? 0 : score;
          } else {
            score = member_score_map[sm.member];
            switch (agg) {
              case SUM: score += weight * sm.score; break;
              case MIN: score  = std::min(score, weight * sm.score); break;
              case MAX: score  = std::max(score, weight * sm.score); break;
            }
            member_score_map[sm.member] = (score == -0.0) ? 0 : score;
          }
        }
        delete iter;
      }
    } else {
      return Status::NotFound();
    }
  }

  std::string destina_meta_value;
  std::string destina_score_value;
  SkipList skiplist_member_score(&destina_meta_value, ZSET_PREFIX_LENGTH, true);
  SkipList skiplist_score_member(&destina_score_value, ZSET_PREFIX_LENGTH, true);
  ParsedZSetsMetaValue parsed_zsets_meta_value(&destina_meta_value);
  parsed_zsets_meta_value.InitialMetaValue();
  for (const auto& sm : member_score_map) {
    ZSetsKey zsets_key(sm.first, sm.second);
    skiplist_member_score.insert(zsets_key.EncodeMemberScore());
    skiplist_score_member.insert(zsets_key.EncodeScoreMember());
  }
  assert(skiplist_member_score.count() == skiplist_score_member.count());
  parsed_zsets_meta_value.set_count(skiplist_member_score.count());
  memcpy(const_cast<char*>(destina_score_value.data()), const_cast<char*>(destina_meta_value.data()), ZSET_PREFIX_LENGTH);
  *ret = member_score_map.size();
  batch.Put(handles_[0], destination, destina_meta_value);
  batch.Put(handles_[1], destination, destina_score_value);
  s = vdb_->Write(default_write_options_, &batch);
  UpdateSpecificKeyStatistics(destination.ToString(), statistic);
  return s;
}

Status RedisZSets::ZInterstore(const Slice& destination,
                               const std::vector<std::string>& keys,
                               const std::vector<double>& weights,
                               const AGGREGATE agg,
                               int32_t* ret) {
  if (keys.size() <= 0) {
    return Status::Corruption("ZInterstore invalid parameter, no keys");
  }

  *ret = 0;
  uint32_t statistic = 0;
  VWriteBatch batch;
  shannon::ReadOptions read_options;
  const shannon::Snapshot* snapshot = nullptr;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;
  ScopeRecordLock l(lock_mgr_, destination);
  Status s;


  bool have_invalid_zsets = false;
  ScoreMember item;
  std::vector<KeyVersion> vaild_zsets;
  std::vector<ScoreMember> score_members;
  std::vector<ScoreMember> final_score_members;
  std::vector<std::string*> score_values;

  int32_t cur_index = 0;
  for (size_t idx = 0; idx < keys.size(); ++idx) {
    std::string* score_value = new std::string();
    if (idx == 0) {
      s = db_->Get(read_options, handles_[1], keys[idx], score_value);
    } else {
      s = db_->Get(read_options, handles_[0], keys[idx], score_value);
    }
    if (s.ok()) {
      ParsedZSetsMetaValue parsed_zsets_meta_value(score_value);
      if (parsed_zsets_meta_value.IsStale()
        || parsed_zsets_meta_value.count() == 0) {
        have_invalid_zsets = true;
      } else {
        vaild_zsets.push_back({keys[idx], parsed_zsets_meta_value.version()});
        score_values.push_back(score_value);
      }
    } else if (s.IsNotFound()) {
      have_invalid_zsets = true;
    } else {
      return s;
    }
  }

  if (!have_invalid_zsets) {
    SkipList skiplist(score_values[0], ZSET_PREFIX_LENGTH, false);
    SkipList::Iterator* iter = skiplist.NewIterator();
    for (iter->SeekToFirst();
         iter->Valid();
         iter->Next(), ++cur_index) {
      ParsedZSetsScoreMember parsed_zsets_score_member(iter->key());
      double score = parsed_zsets_score_member.score();
      std::string member = parsed_zsets_score_member.member().ToString();
      score_members.push_back({score, member});
    }
    delete iter;
    assert(cur_index == skiplist.count());
    std::string data_value;
    for (const auto& sm : score_members) {
      bool reliable = true;
      item.member = sm.member;
      item.score = sm.score * (weights.size() > 0 ? weights[0] : 1);
      for (size_t idx = 1; idx < vaild_zsets.size(); ++idx) {
        double weight = idx < weights.size() ? weights[idx] : 1;
        SkipList skiplist(score_values[idx], ZSET_PREFIX_LENGTH, false);
        SkipList::Iterator *iter = skiplist.NewIterator();
        iter->Seek(item.member);
        if (iter->Valid()) {
          ParsedZSetsMemberScore parsed_zsets_member_score(iter->key());
          double score = parsed_zsets_member_score.score();
          switch (agg) {
            case SUM: item.score += weight * score; break;
            case MIN: item.score  = std::min(item.score, weight * score); break;
            case MAX: item.score  = std::max(item.score, weight * score); break;
          }
        } else {
          // It's not found to
          reliable = false;
          break;
        }
        delete iter;
      }
      if (reliable) {
        final_score_members.push_back(item);
      }
    }
  }
  for (auto score_value : score_values) {
    delete score_value;
  }
  std::string destina_member_score;
  std::string destina_score_member;
  SkipList skiplist_member_score(&destina_member_score, ZSET_PREFIX_LENGTH, true);
  SkipList skiplist_score_member(&destina_score_member, ZSET_PREFIX_LENGTH, true);
  ParsedZSetsMetaValue parsed_zsets_meta_value(&destina_member_score);
  parsed_zsets_meta_value.InitialMetaValue();
  for (const auto& sm : final_score_members) {
    ZSetsKey zsets_key(sm.score, sm.member);
    assert(skiplist_member_score.insert(zsets_key.EncodeMemberScore()));
    assert(skiplist_score_member.insert(zsets_key.EncodeScoreMember()));
  }
  *ret = final_score_members.size();
  assert(skiplist_member_score.count() == skiplist_score_member.count());
  parsed_zsets_meta_value.set_count(skiplist_member_score.count());
  memcpy(const_cast<char*>(destina_score_member.data()), const_cast<char*>(destina_member_score.data()), ZSET_PREFIX_LENGTH);
  batch.Put(handles_[0], destination, destina_member_score);
  batch.Put(handles_[1], destination, destina_score_member);
  s = vdb_->Write(default_write_options_, &batch);
  UpdateSpecificKeyStatistics(destination.ToString(), statistic);
  return s;
}

Status RedisZSets::ZRangebylex(const Slice& key,
                               const Slice& min,
                               const Slice& max,
                               bool left_close,
                               bool right_close,
                               std::vector<std::string>* members) {
  members->clear();
  shannon::ReadOptions read_options;
  const shannon::Snapshot* snapshot = nullptr;

  ScopeSnapshot ss(db_, &snapshot);
  std::string meta_value;
  read_options.snapshot = snapshot;
  Status s;

  bool left_no_limit = !min.compare("-");
  bool right_not_limit = !max.compare("+");

  s = db_->Get(read_options, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedZSetsMetaValue parsed_zsets_meta_value(&meta_value);
    if (parsed_zsets_meta_value.IsStale()
      || parsed_zsets_meta_value.count() == 0) {
      return Status::NotFound();
    } else {
      int32_t cur_index = 0;
      SkipList skiplist(&meta_value, ZSET_PREFIX_LENGTH, false);
      SkipList::Iterator* iter = skiplist.NewIterator();
      for (iter->SeekToFirst();
           iter->Valid();
           iter->Next(), ++cur_index) {
        bool left_pass = false;
        bool right_pass = false;
        ParsedZSetsMemberScore parsed_zsets_member_score(iter->key());
        Slice member = parsed_zsets_member_score.member();
        if (left_no_limit
          || (left_close && min.compare(member) <= 0)
          || (!left_close && min.compare(member) < 0)) {
          left_pass = true;
        }
        if (right_not_limit
          || (right_close && max.compare(member) >= 0)
          || (!right_close && max.compare(member) > 0)) {
          right_pass = true;
        }
        if (left_pass && right_pass) {
          members->push_back(member.ToString());
        }
        if (!right_pass) {
          break;
        }
      }
      if (!iter->Valid()) {
        assert(cur_index == parsed_zsets_meta_value.count());
      }
      delete iter;
    }
  } else {
      s = Status::NotFound();
  }
  return s;
}

Status RedisZSets::ZLexcount(const Slice& key,
                             const Slice& min,
                             const Slice& max,
                             bool left_close,
                             bool right_close,
                             int32_t* ret) {
  std::vector<std::string> members;
  Status s = ZRangebylex(key, min, max, left_close, right_close, &members);
  *ret = members.size();
  return s;
}

Status RedisZSets::ZRemrangebylex(const Slice& key,
                                  const Slice& min,
                                  const Slice& max,
                                  bool left_close,
                                  bool right_close,
                                  int32_t* ret) {
  *ret = 0;
  uint32_t statistic = 0;
  VWriteBatch batch;
  shannon::ReadOptions read_options;
  const shannon::Snapshot* snapshot = nullptr;

  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;
  ScopeRecordLock l(lock_mgr_, key);

  bool left_no_limit = !min.compare("-");
  bool right_not_limit = !max.compare("+");

  int32_t del_cnt = 0;
  std::string meta_value;
  std::string score_value;
  std::vector<std::string> del_keys;
  Status s;
  s = db_->Get(read_options, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedZSetsMetaValue parsed_zsets_meta_value(&meta_value);
    if (parsed_zsets_meta_value.IsStale()
      || parsed_zsets_meta_value.count() == 0) {
      return Status::NotFound();
    } else {
      int32_t cur_index = 0;
      int32_t stop_index = parsed_zsets_meta_value.count() - 1;
      SkipList skiplist_member_score(&meta_value, ZSET_PREFIX_LENGTH, false);
      SkipList::Iterator* iter = skiplist_member_score.NewIterator();
      for (iter->SeekToFirst();
           iter->Valid() && cur_index <= stop_index;
           iter->Next(), ++cur_index) {
        bool left_pass = false;
        bool right_pass = false;
        ParsedZSetsMemberScore parsed_zsets_member_score(iter->key());
        Slice member = parsed_zsets_member_score.member();
        if (left_no_limit
          || (left_close && min.compare(member) <= 0)
          || (!left_close && min.compare(member) < 0)) {
          left_pass = true;
        }
        if (right_not_limit
          || (right_close && max.compare(member) >= 0)
          || (!right_close && max.compare(member) > 0)) {
          right_pass = true;
        }
        if (left_pass && right_pass) {
          batch.Delete(handles_[1], iter->key());
          del_keys.push_back(std::string(iter->key().data(), iter->key().size()));
          del_cnt++;
          statistic++;
        }
        if (!right_pass) {
          break;
        }
      }
      delete iter;
    }
    if (del_cnt > 0) {
      s = db_->Get(read_options, handles_[1], key, &score_value);
      assert(s.ok());
      SkipList skiplist_member_score(&meta_value, ZSET_PREFIX_LENGTH, false);
      SkipList skiplist_score_member(&score_value, ZSET_PREFIX_LENGTH, false);
      for (auto del_key : del_keys) {
        ParsedZSetsMemberScore parsed_zsets_member_score(&del_key);
        ZSetsKey zsets_key(parsed_zsets_member_score.score(), parsed_zsets_member_score.member());
        assert(skiplist_member_score.del(zsets_key.EncodeMemberScore()));
        assert(skiplist_score_member.del(zsets_key.EncodeScoreMember()));
      }
      parsed_zsets_meta_value.ModifyCount(-del_cnt);
      assert(skiplist_member_score.count() == parsed_zsets_meta_value.count());
      memcpy(const_cast<char*>(score_value.data()), const_cast<char*>(meta_value.data()), ZSET_PREFIX_LENGTH);
      batch.Put(handles_[0], key, meta_value);
      batch.Put(handles_[1], key, score_value);
      *ret = del_cnt;
    }
  } else {
    return Status::NotFound();
  }
  s = vdb_->Write(default_write_options_, &batch);
  UpdateSpecificKeyStatistics(key.ToString(), statistic);
  return s;
}

Status RedisZSets::Expire(const Slice& key, int32_t ttl) {
  std::string meta_value;
  std::string score_value;
  ScopeRecordLock l(lock_mgr_, key);
  Status s;

  s = db_->Get(default_read_options_, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedZSetsMetaValue parsed_zsets_meta_value(&meta_value);
    if (parsed_zsets_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if (parsed_zsets_meta_value.count() == 0) {
      return Status::NotFound();
    }

    if (ttl > 0) {
      parsed_zsets_meta_value.SetRelativeTimestamp(ttl);
      s = db_->Get(default_read_options_, handles_[1], key, &score_value);
      assert(s.ok());
      memcpy(const_cast<char*>(score_value.data()), const_cast<char*>(meta_value.data()), ZSET_PREFIX_LENGTH);
      if (parsed_zsets_meta_value.timestamp() != 0) {
        char str[sizeof(int32_t) + key.size() + 1];
        str[sizeof(int32_t)+key.size()] = '\0';
        EncodeFixed32(str,parsed_zsets_meta_value.timestamp());
        memcpy(str + sizeof(int32_t), key.data(), key.size());
        vdb_->Put(default_write_options_, handles_[3], {str, sizeof(int32_t) + key.size()}, "1");
      }
    } else {
      // meta_value.clear();
      // parsed_zsets_meta_value.InitialMetaValue();
      vdb_->Delete(default_write_options_, handles_[0], key);
      vdb_->Delete(default_write_options_, handles_[1], key);
      return s;
    }
    VWriteBatch batch;
    batch.Put(handles_[0], key, meta_value);
    batch.Put(handles_[1], key, score_value);
    s = vdb_->Write(default_write_options_, &batch);
  } else {
    s = Status::NotFound();
  }
  return s;
}

Status RedisZSets::Del(const Slice& key) {

  std::string meta_value;
  std::string score_value;
  ScopeRecordLock l(lock_mgr_, key);
  Status s;

  s = db_->Get(default_read_options_, handles_[0], key, &meta_value);
  if (s.ok()) {
    s = db_->Get(default_read_options_, handles_[1], key, &score_value);
    assert(s.ok());
    ParsedZSetsMetaValue parsed_zsets_meta_value(&meta_value);
    if (parsed_zsets_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if (parsed_zsets_meta_value.count() == 0) {
      return Status::NotFound();
    } else {
      uint32_t statistic = parsed_zsets_meta_value.count();
      parsed_zsets_meta_value.InitialMetaValue();
      VWriteBatch batch;
      batch.Delete(handles_[0], key);
      batch.Delete(handles_[1], key);
      s = vdb_->Write(default_write_options_, &batch);
      UpdateSpecificKeyStatistics(key.ToString(), statistic);
    }
  } else {
    s = Status::NotFound();
  }
  return s;
}

bool RedisZSets::Scan(const std::string& start_key,
                      const std::string& pattern,
                      std::vector<std::string>* keys,
                      int64_t* count,
                      std::string* next_key) {
  std::string meta_key;
  bool is_finish = true;
  shannon::ReadOptions iterator_options;
  const shannon::Snapshot* snapshot;
  ScopeSnapshot ss(db_, &snapshot);
  iterator_options.snapshot = snapshot;
  iterator_options.fill_cache = false;

  shannon::Iterator* it = db_->NewIterator(iterator_options, handles_[0]);

  it->Seek(start_key);
  while (it->Valid() && (*count) > 0) {
    ParsedZSetsMetaValue parsed_zsets_meta_value(it->value());
    if (parsed_zsets_meta_value.IsStale()
      || parsed_zsets_meta_value.count() == 0) {
      it->Next();
      continue;
    } else {
      meta_key = it->key().ToString();
      if (StringMatch(pattern.data(), pattern.size(),
                         meta_key.data(), meta_key.size(), 0)) {
        keys->push_back(meta_key);
      }
      (*count)--;
      it->Next();
    }
  }

  std::string prefix = isTailWildcard(pattern) ?
    pattern.substr(0, pattern.size() - 1) : "";
  if (it->Valid()
    && (it->key().compare(prefix) <= 0 || it->key().starts_with(prefix))) {
    *next_key = it->key().ToString();
    is_finish = false;
  } else {
    *next_key = "";
  }
  delete it;
  return is_finish;
}

Status RedisZSets::Expireat(const Slice& key, int32_t timestamp) {
  std::string meta_value;
  ScopeRecordLock l(lock_mgr_, key);
  Status s;
  s = db_->Get(default_read_options_, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedZSetsMetaValue parsed_zsets_meta_value(&meta_value);
    if (parsed_zsets_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if (parsed_zsets_meta_value.count() == 0) {
      return Status::NotFound();
    } else {
      if (timestamp > 0) {
        parsed_zsets_meta_value.set_timestamp(timestamp);
        char str[sizeof(int32_t) + key.size() + 1];
        str[sizeof(int32_t) + key.size()] = '\0';
        EncodeFixed32(str, parsed_zsets_meta_value.timestamp());
        memcpy(str + sizeof(int32_t), key.data(), key.size());
        vdb_->Put(default_write_options_, handles_[3], {str, sizeof(int32_t) + key.size()}, "1");
        std::string score_value;
        s = db_->Get(default_read_options_, handles_[1], key, &score_value);
        assert(s.ok());
        memcpy(const_cast<char*>(score_value.data()), const_cast<char*>(meta_value.data()), ZSET_PREFIX_LENGTH);
        VWriteBatch batch;
        batch.Put(handles_[0], key, meta_value);
        batch.Put(handles_[1], key, score_value);
        s = vdb_->Write(default_write_options_, &batch);
      } else {
        VWriteBatch batch;
        batch.Delete(handles_[0], key);
        batch.Delete(handles_[1], key);
        s = vdb_->Write(default_write_options_, &batch);
      }
    }
  } else {
    s = Status::NotFound();
  }
  return s;
}

Status RedisZSets::ZScan(const Slice& key, int64_t cursor, const std::string& pattern,
                         int64_t count, std::vector<ScoreMember>* score_members, int64_t* next_cursor) {
  *next_cursor = 0;
  score_members->clear();
  if (cursor < 0) {
    *next_cursor = 0;
    return Status::OK();
  }

  int64_t rest = count;
  int64_t step_length = count;
  shannon::ReadOptions read_options;
  const shannon::Snapshot* snapshot;

  std::string meta_value;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;
  Status s = db_->Get(read_options, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedZSetsMetaValue parsed_zsets_meta_value(&meta_value);
    if (parsed_zsets_meta_value.IsStale()
      || parsed_zsets_meta_value.count() == 0) {
      *next_cursor = 0;
      return Status::NotFound();
    } else {
      std::string sub_member;
      std::string start_point;
      s = GetScanStartPoint(key, pattern, cursor, &start_point);
      if (s.IsNotFound()) {
        cursor = 0;
        if (isTailWildcard(pattern)) {
          start_point = pattern.substr(0, pattern.size() - 1);
        }
      }
      if (isTailWildcard(pattern)) {
        sub_member = pattern.substr(0, pattern.size() - 1);
      }

      // ZSetsMemberKey zsets_member_prefix(key, version, sub_member);
      std::string prefix = sub_member;  // zsets_member_prefix.Encode().ToString();
      SkipList skiplist(&meta_value, ZSET_PREFIX_LENGTH, false);
      SkipList::Iterator* iter = skiplist.NewIterator();
      if (start_point.size() > 0) {
        iter->Seek(start_point);
      } else {
        iter->SeekToFirst();
      }
      for (;
           iter->Valid() && rest > 0 && iter->key().starts_with(prefix);
           iter->Next()) {
        ParsedZSetsMemberScore parsed_zsets_member_key(iter->key());
        std::string member = parsed_zsets_member_key.member().ToString();
        if (StringMatch(pattern.data(), pattern.size(), member.data(), member.size(), 0)) {
          double score = parsed_zsets_member_key.score();
          score_members->push_back({score, member});
        }
        rest--;
      }
      if (iter->Valid()
        && (iter->key().compare(prefix) <= 0 || iter->key().starts_with(prefix))) {
        *next_cursor = cursor + step_length;
        ParsedZSetsMemberScore parsed_zsets_member_score(iter->key());
        std::string next_member = parsed_zsets_member_score.member().ToString();
        StoreScanNextPoint(key, pattern, *next_cursor, next_member);
      } else {
        *next_cursor = 0;
      }
      delete iter;
    }
  } else {
    *next_cursor = 0;
    return s;
  }
  return Status::OK();
}

Status RedisZSets::PKScanRange(const Slice& key_start, const Slice& key_end,
                               const Slice& pattern, int32_t limit,
                               std::vector<std::string>* keys, std::string* next_key) {
  next_key->clear();

  std::string key;
  int32_t remain = limit;
  shannon::ReadOptions iterator_options;
  const shannon::Snapshot* snapshot;
  ScopeSnapshot ss(db_, &snapshot);
  iterator_options.snapshot = snapshot;
  iterator_options.fill_cache = false;

  bool start_no_limit = !key_start.compare("");
  bool end_no_limit = !key_end.compare("");

  if (!start_no_limit
    && !end_no_limit
    && (key_start.compare(key_end) > 0)) {
    return Status::InvalidArgument("error in given range");
  }

  shannon::Iterator* it = db_->NewIterator(iterator_options, handles_[0]);
  if (start_no_limit) {
    it->SeekToFirst();
  } else {
    it->Seek(key_start);
  }

  while (it->Valid() && remain > 0
    && (end_no_limit || it->key().compare(key_end) <= 0)) {
    ParsedZSetsMetaValue parsed_zsets_meta_value(it->value());
    if (parsed_zsets_meta_value.IsStale()
      || parsed_zsets_meta_value.count() == 0) {
      it->Next();
    } else {
      key = it->key().ToString();
      if (StringMatch(pattern.data(), pattern.size(),
                         key.data(), key.size(), 0)) {
        keys->push_back(key);
      }
      remain--;
      it->Next();
    }
  }

  while (it->Valid()
    && (end_no_limit || it->key().compare(key_end) <= 0)) {
    ParsedZSetsMetaValue parsed_zsets_meta_value(it->value());
    if (parsed_zsets_meta_value.IsStale()
      || parsed_zsets_meta_value.count() == 0) {
      it->Next();
    } else {
      *next_key = it->key().ToString();
      break;
    }
  }
  delete it;
  return Status::OK();
}

Status RedisZSets::PKRScanRange(const Slice& key_start, const Slice& key_end,
                                const Slice& pattern, int32_t limit,
                                std::vector<std::string>* keys, std::string* next_key) {
  next_key->clear();

  std::string key;
  int32_t remain = limit;
  shannon::ReadOptions iterator_options;
  const shannon::Snapshot* snapshot;
  ScopeSnapshot ss(db_, &snapshot);
  iterator_options.snapshot = snapshot;
  iterator_options.fill_cache = false;

  bool start_no_limit = !key_start.compare("");
  bool end_no_limit = !key_end.compare("");

  if (!start_no_limit
    && !end_no_limit
    && (key_start.compare(key_end) < 0)) {
    return Status::InvalidArgument("error in given range");
  }

  shannon::Iterator* it = db_->NewIterator(iterator_options, handles_[0]);
  if (start_no_limit) {
    it->SeekToLast();
  } else {
    it->SeekForPrev(key_start);
  }

  while (it->Valid() && remain > 0
       && (end_no_limit || it->key().compare(key_end) >= 0)) {
    ParsedZSetsMetaValue parsed_zsets_meta_value(it->value());
    if (parsed_zsets_meta_value.IsStale()
      || parsed_zsets_meta_value.count() == 0) {
      it->Prev();
    } else {
      key = it->key().ToString();
      if (StringMatch(pattern.data(), pattern.size(),
                         key.data(), key.size(), 0)) {
        keys->push_back(key);
      }
      remain--;
      it->Prev();
    }
  }

  while (it->Valid()
    && (end_no_limit || it->key().compare(key_end) >= 0)) {
    ParsedZSetsMetaValue parsed_zsets_meta_value(it->value());
    if (parsed_zsets_meta_value.IsStale()
      || parsed_zsets_meta_value.count() == 0) {
      it->Prev();
    } else {
      *next_key = it->key().ToString();
      break;
    }
  }
  delete it;
  return Status::OK();
}

Status RedisZSets::Persist(const Slice& key) {
  std::string meta_value;
  ScopeRecordLock l(lock_mgr_, key);
  Status s;
  s = db_->Get(default_read_options_, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedZSetsMetaValue parsed_zsets_meta_value(&meta_value);
    if (parsed_zsets_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if (parsed_zsets_meta_value.count() == 0) {
      return Status::NotFound();
    } else {
      int32_t timestamp = parsed_zsets_meta_value.timestamp();
      if (timestamp == 0) {
        return Status::NotFound("Not have an associated timeout");
      } else {
        std::string score_value;
        s = db_->Get(default_read_options_, handles_[1], key, &score_value);
        assert(s.ok());
        parsed_zsets_meta_value.set_timestamp(0);
        memcpy(const_cast<char*>(score_value.data()), const_cast<char*>(meta_value.data()), ZSET_PREFIX_LENGTH);
        VWriteBatch batch;
        batch.Put(handles_[0], key, meta_value);
        batch.Put(handles_[1], key, score_value);
        s = vdb_->Write(default_write_options_, &batch);
      }
    }
  } else {
      s = Status::NotFound();
  }
  return s;
}

Status RedisZSets::TTL(const Slice& key, int64_t* timestamp) {
  Status s;
  std::string meta_value;
  s = db_->Get(default_read_options_, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedZSetsMetaValue parsed_zsets_meta_value(&meta_value);
    if (parsed_zsets_meta_value.IsStale()) {
      *timestamp = -2;
      return Status::NotFound("Stale");
    } else if (parsed_zsets_meta_value.count() == 0) {
      *timestamp = -2;
      return Status::NotFound();
    } else {
      *timestamp = parsed_zsets_meta_value.timestamp();
      if (*timestamp == 0) {
        *timestamp = -1;
      } else {
        int64_t curtime;
        shannon::Env::Default()->GetCurrentTime(&curtime);
        *timestamp = *timestamp - curtime > 0 ? *timestamp - curtime : -2;
      }
    }
  } else {
    *timestamp = -2;
    s = Status::NotFound();
  }
  return s;
}

std::vector<shannon::ColumnFamilyHandle*> RedisZSets::GetColumnFamilyHandles() {
  return handles_;
}

void RedisZSets::ScanDatabase() {
  shannon::ReadOptions iterator_options;
  const shannon::Snapshot* snapshot;
  ScopeSnapshot ss(db_, &snapshot);
  iterator_options.snapshot = snapshot;
  iterator_options.fill_cache = false;
  int32_t current_time = time(NULL);

  printf("\n***************ZSets Meta Data***************\n");
  auto meta_iter = db_->NewIterator(iterator_options, handles_[0]);
  auto score_iter = db_->NewIterator(iterator_options, handles_[1]);
  for (meta_iter->SeekToFirst(), score_iter->SeekToFirst();
       meta_iter->Valid() && score_iter->Valid();
       meta_iter->Next(), score_iter->Next()) {
    ParsedZSetsMetaValue parsed_zsets_meta_value(meta_iter->value());
    int32_t survival_time = 0;
    if (parsed_zsets_meta_value.timestamp() != 0) {
      survival_time = parsed_zsets_meta_value.timestamp() - current_time > 0 ?
        parsed_zsets_meta_value.timestamp() - current_time : -1;
    }

    printf("[key : %-30s] [count : %-10d] [timestamp : %-10d] [version : %d] [survival_time : %d]\n",
           meta_iter->key().ToString().c_str(),
           parsed_zsets_meta_value.count(),
           parsed_zsets_meta_value.timestamp(),
           parsed_zsets_meta_value.version(),
           survival_time);
    if (parsed_zsets_meta_value.count() == 0) {
     continue;
    }

    std::string meta_value = meta_iter->value().ToString();
    std::string score_value = score_iter->value().ToString();
    SkipList skiplist_member_score(&meta_value, ZSET_PREFIX_LENGTH, false);
    SkipList skiplist_score_member(&score_value, ZSET_PREFIX_LENGTH, false);
    assert(skiplist_score_member.count() == skiplist_member_score.count());
    SkipList::Iterator *iter_member_score = skiplist_member_score.NewIterator();
    SkipList::Iterator *iter_score_member = skiplist_score_member.NewIterator();
    for (iter_member_score->SeekToFirst(), iter_score_member->SeekToFirst();
         iter_member_score->Valid() && iter_score_member->Valid();
         iter_member_score->Next(), iter_score_member->Next()) {
      ParsedZSetsMemberScore parsed_zsets_member_score(iter_member_score->key());
      ParsedZSetsScoreMember parsed_zsets_score_member(iter_score_member->key());
      printf("[member : %-20s] [score : %-20lf]\n",
             parsed_zsets_score_member.member().ToString().c_str(), parsed_zsets_score_member.score());
    }
  }
  delete score_iter;
  delete meta_iter;
}

Status RedisZSets::DelTimeout(BlackWidow * bw,std::string * key) {
  Status s = Status::OK();
  shannon::Iterator *iter = db_->NewIterator(shannon::ReadOptions(), handles_[2]);
  if (nullptr == iter) {
    *key = "";
    return s;
  }
  iter->SeekToFirst();
  if (!iter->Valid()) {
    *key = "";
    delete iter;
    return s;
  }
  Slice slice_key = iter->key().data();
  int64_t cur_meta_timestamp_ = DecodeFixed32(slice_key.data());
  int64_t unix_time;
  shannon::Env::Default()->GetCurrentTime(&unix_time);
  if (cur_meta_timestamp_ > 0 && cur_meta_timestamp_ < static_cast<int32_t>(unix_time))
  {
   key->resize(iter->key().size()-sizeof(int32_t));
   memcpy(const_cast<char *>(key->data()),slice_key.data()+sizeof(int32_t),iter->key().size()-sizeof(int32_t));
    s = RealDelTimeout(bw,key);
  }
  else  *key = "";
  delete iter;
  return s;
}

Status RedisZSets::RealDelTimeout(BlackWidow *bw, std::string *key) {
  Status s = Status::OK();
  ScopeRecordLock l(lock_mgr_, *key);
  std::string meta_value;
  s = db_->Get(default_read_options_, handles_[0], *key, &meta_value);
  if (s.ok()) {
    ParsedZSetsMetaValue parsed_zsets_meta_value(&meta_value);
    int64_t unix_time;
    shannon::Env::Default()->GetCurrentTime(&unix_time);
    if (parsed_zsets_meta_value.IsStale()) {
      VWriteBatch batch;
      batch.Delete(handles_[0], *key);
      batch.Delete(handles_[1], *key);
      s = vdb_->Write(default_write_options_, &batch);
    }
  }
  return s;
}

Status RedisZSets::LogAdd(const Slice& key, const Slice& value,
                          const std::string& cf_name) {
  Status s;
  bool flag = false;
  for (auto cfh : handles_) {
    if (cfh->GetName() == cf_name) {
      s = vdb_->Put(default_write_options_, cfh, key, value);
      if (!s.ok()) {
        return s;
      }
      flag = true;
      break;
    }
  }
  if (!flag) {
    return Status::NotFound();
  }
  return s;
}

Status RedisZSets::LogDelete(const Slice& key, const std::string& cf_name) {
  Status s;
  bool flag = false;
  for (auto cfh : handles_) {
    if (cfh->GetName() == cf_name) {
      s = vdb_->Delete(default_write_options_, cfh, key);
      flag = true;
      break;
    }
  }
  if (!flag) {
    return Status::NotFound();
  }
  return s;
}

} // blackwidow
