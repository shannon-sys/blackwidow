//  Copyright (c) 2017-present The blackwidow Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "src/redis_sets.h"

#include <map>
#include <memory>
#include <random>
#include <algorithm>
#include <unordered_map>
#include <time.h>
#include <sys/time.h>
#include <unistd.h>

#include "blackwidow/util.h"
#include "src/base_filter.h"
#include "src/scope_record_lock.h"
#include "src/scope_snapshot.h"
#include "src/unordered_map_cache_lock.h"
#include "skip_list.h"

namespace blackwidow {
unordered_map_cache_lock meta_infos_set_;
RedisSets::RedisSets(BlackWidow* const bw, const DataType& type)
    : Redis(bw, type) {
  spop_counts_store_.max_size_ = 1000;
}

RedisSets::~RedisSets() {
  std::vector<shannon::ColumnFamilyHandle*> tmp_handles = handles_;
  handles_.clear();
  for (auto handle : tmp_handles) {
    delete handle;
  }
  for (std::unordered_map<std::string, std::string*>::iterator iter =
          meta_infos_set_.begin();iter != meta_infos_set_.end();
          ++ iter) {
      delete iter->second;
  }
  meta_infos_set_.clear();
}

Status RedisSets::Open(const BlackwidowOptions& bw_options,
                       const std::string& db_path) {
  statistics_store_.max_size_ = bw_options.statistics_max_size;
  small_compaction_threshold_ = bw_options.small_compaction_threshold;

  shannon::Options ops(bw_options.options);
  Status s = shannon::DB::Open(ops, db_path, default_device_name_, &db_);
  if (s.ok()) {
    // create column family
    shannon::ColumnFamilyHandle* cf ,*tcf;
    shannon::ColumnFamilyOptions cfo;
    s = db_->CreateColumnFamily(cfo, "member_cf", &cf);
    if (!s.ok()) {
      return s;
    }

    s = db_->CreateColumnFamily(shannon::ColumnFamilyOptions(), "timeout_cf", &tcf);
    if (!s.ok()) {
      return s;
    }
    // close DB
    delete cf;
    delete tcf;
    delete db_;
  }

  // Open
  shannon::DBOptions db_ops(bw_options.options);
  shannon::ColumnFamilyOptions meta_cf_ops(bw_options.options);
  shannon::ColumnFamilyOptions member_cf_ops(bw_options.options);
  shannon::ColumnFamilyOptions timeout_cf_ops(bw_options.options);
  meta_cf_ops.compaction_filter_factory =
      std::make_shared<SetsMetaFilterFactory>();
  member_cf_ops.compaction_filter_factory =
      std::make_shared<SetsMemberFilterFactory>(&db_, &handles_);

  //use the bloom filter policy to reduce disk reads
  std::vector<shannon::ColumnFamilyDescriptor> column_families;
  // Meta CF
  column_families.push_back(shannon::ColumnFamilyDescriptor(
      shannon::kDefaultColumnFamilyName, meta_cf_ops));
  // Member CF
  column_families.push_back(shannon::ColumnFamilyDescriptor(
      "member_cf", member_cf_ops));
  column_families.push_back(shannon::ColumnFamilyDescriptor(
      "timeout_cf", shannon::ColumnFamilyOptions()));
  s = shannon::DB::Open(db_ops, db_path, default_device_name_, column_families, &handles_, &db_);
  if (s.ok()) {
    vdb_ = new VDB(db_);
    meta_infos_set_.SetDb(db_);
    meta_infos_set_.SetColumnFamilyHandle(handles_[0]);
  }
  return s;
}

Status RedisSets::CompactRange(const shannon::Slice* begin,
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
Status RedisSets::AddDelKey(BlackWidow * bw,const string  & str){
  return bw->AddDelKey(db_,str,handles_[0]);
};
Status RedisSets::GetProperty(const std::string& property, uint64_t* out) {
  std::string value;
  db_->GetProperty(handles_[0], property, &value);
  *out = std::strtoull(value.c_str(), NULL, 10);
  db_->GetProperty(handles_[1], property, &value);
  *out += std::strtoull(value.c_str(), NULL, 10);
  return Status::OK();
}

Status RedisSets::ScanKeyNum(KeyInfo* key_info) {
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
    ParsedSetsMetaValue parsed_sets_meta_value(iter->value());
    if (parsed_sets_meta_value.IsStale()
      || parsed_sets_meta_value.count() == 0) {
      invaild_keys++;
    } else {
      keys++;
      if (!parsed_sets_meta_value.IsPermanentSurvival()) {
        expires++;
        ttl_sum += parsed_sets_meta_value.timestamp() - curtime;
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

Status RedisSets::ScanKeys(const std::string& pattern,
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
    ParsedSetsMetaValue parsed_sets_meta_value(iter->value());
    if (!parsed_sets_meta_value.IsStale()
      && parsed_sets_meta_value.count() != 0) {
      key = iter->key().ToString();
      if (StringMatch(pattern.data(), pattern.size(), key.data(), key.size(), 0)) {
        keys->push_back(key);
      }
    }
  }
  delete iter;
  return Status::OK();
}

Status RedisSets::SAdd(const Slice& key,
                       const std::vector<std::string>& members, int32_t* ret) {
  std::unordered_set<std::string> unique;
  std::vector<std::string> filtered_members;
  int32_t old_count ;
  // Deduplication
  for (const auto& member : members) {
    if (unique.find(member) == unique.end()) {
      unique.insert(member);
      filtered_members.push_back(member);
    }
  }
  ScopeRecordLock l(lock_mgr_, key);
  Status s;
  std::string meta_value;
  meta_value.reserve(SET_RESERVE_LENGTH);
  s =  db_->Get(ReadOptions(), handles_[0], key, &meta_value);
  bool need_reset;
  if(s.ok()) {
    ParsedSetsMetaValue parsed_sets_meta_value(&meta_value);
    if (parsed_sets_meta_value.count() == 0 ) {
      need_reset = false;
    } else if ( parsed_sets_meta_value.IsStale() ) {
      parsed_sets_meta_value.InitialMetaValue();
      need_reset = true;
    } else {
      need_reset = false;
    }
    SkipList skiplist = SkipList(&meta_value, SET_PREFIX_LENGTH, need_reset);
    old_count = skiplist.count();
    for (const auto  &member : filtered_members) {
      skiplist.insert(member);
    }
    *ret = skiplist.count() - old_count;
    parsed_sets_meta_value.set_count(skiplist.count());
    s = vdb_->Put(WriteOptions(), handles_[0], key, meta_value);
  } else {
    SkipList skiplist = SkipList(&meta_value, SET_PREFIX_LENGTH, true);
    for (const auto& member : filtered_members) {
      skiplist.insert(member);
    }
    ParsedSetsMetaValue parsed_sets_meta_value(&meta_value);
    parsed_sets_meta_value.InitialMetaValue();
    parsed_sets_meta_value.set_count(skiplist.count());
    *ret = filtered_members.size();
    s = vdb_->Put(WriteOptions(), handles_[0], key, meta_value);
  }
  return s;
}

Status RedisSets::SCard(const Slice& key, int32_t* ret) {
  *ret = 0;
  std::string meta_value;
  Status s;
  meta_value.reserve(SET_RESERVE_LENGTH);
  s =  db_->Get(ReadOptions(), handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedSetsMetaValue parsed_sets_meta_value(&meta_value);
    if (parsed_sets_meta_value.count() == 0 || parsed_sets_meta_value.IsStale()) {
      return Status::NotFound("Deleted");
    } else {
      *ret = parsed_sets_meta_value.count();
      if (*ret == 0) {
        return Status::NotFound("Deleted");
      }
    }
  } else {
      s = Status::NotFound();
  }
  return Status::OK();
}

Status RedisSets::SDiff(const std::vector<std::string>& keys,
                        std::vector<std::string>* members) {
  if (keys.size() <= 0) {
    return Status::Corruption("SDiff invalid parameter, no keys");
  } 
  shannon::ReadOptions read_options;
  const shannon::Snapshot* snapshot;

  std::string meta_value;
  meta_value.reserve(SET_RESERVE_LENGTH);
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;
  std::vector<SkipList* > meta_lists;
  std::vector<std::string* > str_ptrs;
  Status s;
  s = db_->Get(read_options, handles_[0], keys[0], &meta_value);
  if (s.ok()) {
    ParsedSetsMetaValue parsed_sets_meta_value(&meta_value);
    if (parsed_sets_meta_value.count() == 0 || parsed_sets_meta_value.IsStale()) {
      return Status::NotFound("Deleted");
    }
    bool fg = true ;
    for (auto key : keys){
      if (fg) {
        fg = false;
        continue;
      }
      std::string * meta_info = new std::string("")  ;
      meta_info->reserve(SET_RESERVE_LENGTH);
      s = db_->Get(read_options, handles_[0], key, meta_info);
      if (s.ok()) {
        ParsedSetsMetaValue parsed_sets_meta_value(meta_info);
        if (parsed_sets_meta_value.count() == 0 || parsed_sets_meta_value.IsStale()) {
          delete meta_info;
          continue;
        }
        meta_lists.push_back(new SkipList(meta_info, SET_PREFIX_LENGTH, false));
        str_ptrs.push_back(meta_info);
      } else {
        delete meta_info;
      }
    }
    SkipList  skiplist =  SkipList(&meta_value, SET_PREFIX_LENGTH, false);
    auto iter = skiplist.NewIterator();
    for (iter->SeekToFirst();iter->Valid() ;iter->Next()) {
      shannon::Slice key_com =iter->key();
      bool isfound = false;
      for (auto meta_skip_list : meta_lists) {
          if (  meta_skip_list->exists(key_com) ) {
            isfound = true;
            break;
          }
      }
      if (isfound == false) {
        members->push_back(iter->key().ToString());
      }
    }
    delete iter;
    for (auto meta_skip_list : meta_lists) 
      delete meta_skip_list;
    for (auto ptr : str_ptrs) 
      delete ptr;
  } else {
      return Status::OK();
  }
  return Status::OK();
}

Status RedisSets::SDiffstore(const Slice& destination,
                             const std::vector<std::string>& keys,
                             int32_t* ret) {
  if (keys.size() <= 0) {
    return Status::Corruption("SDiff invalid parameter, no keys");
  } 
  shannon::ReadOptions read_options;
  const shannon::Snapshot* snapshot;
  ScopeRecordLock l(lock_mgr_, destination);
  std::vector<std::string> members;
  std::string meta_value;
  meta_value.reserve(SET_RESERVE_LENGTH);
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;
  std::vector<SkipList* > meta_lists;
  std::vector<std::string* > str_ptrs;
  Status s;
  s = db_->Get(read_options, handles_[0], keys[0], &meta_value);
  if (s.ok()) {
    ParsedSetsMetaValue parsed_sets_meta_value(&meta_value);
    if (parsed_sets_meta_value.count() == 0 || parsed_sets_meta_value.IsStale()) {
      return Status::NotFound("Deleted");
    }
    bool fg = true ;
    for (auto key : keys){
      if (fg) {
        fg = false;
        continue;
      }
      std::string * meta_info = new std::string("")  ;
      meta_info->reserve(SET_RESERVE_LENGTH);
      s = db_->Get(read_options, handles_[0], key, meta_info);
      if (s.ok()){
        ParsedSetsMetaValue parsed_sets_meta_value(meta_info);
        if (parsed_sets_meta_value.count() == 0 || parsed_sets_meta_value.IsStale()) {
          delete meta_info;
          continue;
        }
        meta_lists.push_back(new SkipList(meta_info, SET_PREFIX_LENGTH, false));
        str_ptrs.push_back(meta_info);
      } else {
        delete meta_info;
      }
    }
    SkipList skiplist = SkipList(&meta_value, SET_PREFIX_LENGTH, false);
    auto iter = skiplist.NewIterator();
    for (iter->SeekToFirst();iter->Valid() ;iter->Next()) {
      shannon::Slice key_com =iter->key();
      bool isfound = false;
      for (auto meta_skip_list : meta_lists) {
          if (  meta_skip_list->exists(key_com) ) {
            isfound = true;
            break;
          }
      }
      if (isfound == false) {
        members.push_back(iter->key().ToString());
      }
    }
    delete iter;
    for (auto meta_skip_list : meta_lists) 
      delete meta_skip_list;
    for (auto ptr : str_ptrs) 
      delete ptr;
  } else {
      return Status::NotFound();
  }
  string destination_value;
  destination_value.reserve(SET_RESERVE_LENGTH);
  s = db_->Get(read_options, handles_[0] , destination , &destination_value);
  SkipList destination_skip_lis = SkipList(&destination_value, SET_PREFIX_LENGTH, true);
  for (auto member : members ) {
    destination_skip_lis.insert(member);
  }
  ParsedSetsMetaValue parsed_sets_meta_value(&destination_value);
  parsed_sets_meta_value.InitialMetaValue();
  parsed_sets_meta_value.set_count(destination_skip_lis.count());
  *ret = destination_skip_lis.count();
  s = vdb_->Put(WriteOptions(), handles_[0], destination, destination_value);
  return s;
}

Status RedisSets::SInter(const std::vector<std::string>& keys,
                         std::vector<std::string>* members) {
  if (keys.size() <= 0) {
    return Status::Corruption("SDiff invalid parameter, no keys");
  } 
  shannon::ReadOptions read_options;
  const shannon::Snapshot* snapshot;

  std::string meta_value;
  meta_value.reserve(SET_RESERVE_LENGTH);
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;
  std::vector<SkipList* > meta_lists;
  std::vector<std::string* > str_ptrs;
  Status s;
  s = db_->Get(read_options, handles_[0], keys[0], &meta_value);
  if (s.ok()) {
    ParsedSetsMetaValue parsed_sets_meta_value(&meta_value);
    if (parsed_sets_meta_value.count() == 0 || parsed_sets_meta_value.IsStale()) {
      return Status::NotFound("Deleted");
    }
    bool fg = true ;
    for (auto key : keys){
      if (fg) {
        fg = false;
        continue;
      }
      std::string * meta_info = new std::string("")  ;
      meta_info->reserve(SET_RESERVE_LENGTH);
      s = db_->Get(read_options, handles_[0], key, meta_info);
      if (s.ok()){
        ParsedSetsMetaValue parsed_sets_meta_value(meta_info);
        if (parsed_sets_meta_value.count() == 0 || parsed_sets_meta_value.IsStale()) {
            delete meta_info;
            continue;
        }
        meta_lists.push_back(new SkipList(meta_info, SET_PREFIX_LENGTH, false));
        str_ptrs.push_back(meta_info);
      } else {
        delete meta_info;
      }
    }
    SkipList  skiplist = SkipList(&meta_value, SET_PREFIX_LENGTH, false);
    auto iter = skiplist.NewIterator();
    for (iter->SeekToFirst();iter->Valid() ;iter->Next()) {
      shannon::Slice key_com =iter->key();
      bool isfound = true;
      for (auto meta_skip_list : meta_lists) {
          if (  ! meta_skip_list->exists(key_com) ) {
            isfound = false;
            break;
          }
      }
      if (isfound ) {
        members->push_back(iter->key().ToString());
      }
    }
    delete iter ;
    for (auto meta_skip_list : meta_lists) 
      delete meta_skip_list;
    for (auto ptr : str_ptrs) 
      delete ptr;
  } else {
      return Status::OK();
  }
  return Status::OK();
}

Status RedisSets::SInterstore(const Slice& destination,
                              const std::vector<std::string>& keys,
                              int32_t* ret) {
  if (keys.size() <= 0) {
    return Status::Corruption("SDiff invalid parameter, no keys");
  } 
  shannon::ReadOptions read_options;
  const shannon::Snapshot* snapshot;
  ScopeRecordLock l(lock_mgr_, destination);
  std::vector<std::string> members;
  std::string meta_value;
  meta_value.reserve(SET_RESERVE_LENGTH);
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;
  std::vector<SkipList* > meta_lists;
  std::vector<std::string* > str_ptrs;
  Status s;
  s = db_->Get(read_options, handles_[0], keys[0], &meta_value);
  if (s.ok()) {
    ParsedSetsMetaValue parsed_sets_meta_value(&meta_value);
    if (parsed_sets_meta_value.count() == 0 || parsed_sets_meta_value.IsStale()) {
      return Status::NotFound("Deleted");
    }
    bool fg = true ;
    for (auto key : keys){
      if (fg) {
        fg = false;
        continue;
      }
      std::string * meta_info = new std::string("")  ;
      meta_info->reserve(SET_RESERVE_LENGTH);
      s = db_->Get(read_options, handles_[0], key, meta_info);
      if (s.ok()) {
      ParsedSetsMetaValue parsed_meta_info(meta_info);
      if (parsed_meta_info.count() == 0 || parsed_meta_info.IsStale()) {
         delete meta_info;
         continue;
      }
        meta_lists.push_back(new SkipList(meta_info, SET_PREFIX_LENGTH, false));
        str_ptrs.push_back(meta_info);
      } else {
        delete meta_info;
      }
    }
    SkipList  skiplist = SkipList(&meta_value, SET_PREFIX_LENGTH, false);
    auto iter = skiplist.NewIterator();
    for (iter->SeekToFirst();iter->Valid() ;iter->Next()) {
      bool isfound = true;
      for (auto meta_skip_list : meta_lists) {
          if (  ! meta_skip_list->exists( iter->key()) ) {
            isfound = false;
            break;
          }
      }
      if (isfound ) {
        members.push_back(iter->key().ToString());
      }
    }
    delete iter ;
    for (auto meta_skip_list : meta_lists) 
      delete meta_skip_list;
    for (auto ptr : str_ptrs) 
      delete ptr;
  } else {
      return Status::NotFound();
  }
  string destination_value;
  destination_value.reserve(SET_RESERVE_LENGTH);
  SkipList  destination_skip_lis = SkipList(&destination_value, SET_PREFIX_LENGTH, true);
  for (auto member : members ) {
    destination_skip_lis.insert(member);
  }
  ParsedSetsMetaValue parsed_destination_value(&destination_value);
  parsed_destination_value.InitialMetaValue();
  parsed_destination_value.set_count(destination_skip_lis.count());
  *ret = destination_skip_lis.count();
  s = vdb_->Put(WriteOptions(), handles_[0], destination, destination_value);
  return s;
}

Status RedisSets::SIsmember(const Slice& key, const Slice& member,
                            int32_t* ret) {
  *ret = 0;
  std::string meta_value;
  meta_value.reserve(SET_RESERVE_LENGTH);
  Status s;
  s = db_->Get(ReadOptions(), handles_[0], key, &meta_value);
  if(s.ok()) {
    ParsedSetsMetaValue parsed_sets_meta_value(&meta_value);
    if (parsed_sets_meta_value.count() == 0 || parsed_sets_meta_value.IsStale()) {
      *ret = 0;
      return Status::OK();
    }
    SkipList  skiplist = SkipList(&meta_value, SET_PREFIX_LENGTH, false);
    if (skiplist.exists(member)){
      *ret =1;
    } else {
      *ret = 0;
    }
  } else {
    *ret = 0;
  }
  return Status::OK();
}

Status RedisSets::SMembers(const Slice& key,
                           std::vector<std::string>* members) {
  std::string meta_value;
  meta_value.reserve(SET_RESERVE_LENGTH);
  Status s;
  s = db_->Get(ReadOptions(), handles_[0], key, &meta_value);
  if(s.ok()) {
    ParsedSetsMetaValue parsed_sets_meta_value(&meta_value);
    if (parsed_sets_meta_value.count() == 0 || parsed_sets_meta_value.IsStale()) {
      return Status::NotFound();
    }
    SkipList skiplist =  SkipList(&meta_value, SET_PREFIX_LENGTH, false);
    auto iter = skiplist.NewIterator();
    for (iter->SeekToFirst();iter->Valid() ;iter->Next()) {
      members->push_back(iter->key().ToString());
    }
    delete iter;
  } else {
    return Status::NotFound();
  }
  return s;
}

Status RedisSets::SMove(const Slice& source, const Slice& destination,
                        const Slice& member, int32_t* ret) {
  std::vector<std::string> keys {source.ToString(), destination.ToString()};
  MultiScopeRecordLock ml(lock_mgr_, keys);
  Status s;
  *ret = 0;
  std::string sou_value;
  string destination_value;
  sou_value.reserve(SET_RESERVE_LENGTH);
  s = db_->Get(ReadOptions(), handles_[0], source,&sou_value );
  if (s.ok()) {
      ParsedSetsMetaValue parsed_sets_meta_value1(&sou_value);
      if (parsed_sets_meta_value1.count() == 0 || parsed_sets_meta_value1.IsStale()) {
        return Status::NotFound("Deleted");
      }
      std::string des_value;
      des_value.reserve(SET_RESERVE_LENGTH);
      s = db_->Get(ReadOptions(), handles_[0], destination, &des_value );
      if (s.ok()) {
        ParsedSetsMetaValue parsed_sets_meta_value2(&des_value);
        if (parsed_sets_meta_value2.IsStale()) {
          return Status::OK();
        }
        VWriteBatch batch;
        SkipList sou_skiplist = SkipList(&sou_value , SET_PREFIX_LENGTH , false);
        SkipList des_skiplist = SkipList(&des_value , SET_PREFIX_LENGTH , false);
        if (sou_skiplist.exists(member)) {
          sou_skiplist.del(member);
        parsed_sets_meta_value1.set_count(sou_skiplist.count());
        batch.Put(handles_[0], source, sou_value );
        *ret = 1;
        if( ! des_skiplist.exists(member)) {
          des_skiplist.insert(member);
          parsed_sets_meta_value2.set_count(des_skiplist.count());
          batch.Put(handles_[0], destination,des_value );
        }
        s = vdb_->Write(default_write_options_, &batch);
        return s;
      } else {
        return Status::OK();
      }
    }
    return Status::NotFound();
  }
  return Status::NotFound("Stale");
}

Status RedisSets::SPop(const Slice& key, std::string* member, bool* need_compact) {
  std::default_random_engine engine;
  Status s;
  *need_compact = false;
  std::string meta_value;
  meta_value.reserve(SET_RESERVE_LENGTH);
  ScopeRecordLock l(lock_mgr_, key);
  s = db_->Get(ReadOptions(), handles_[0], key,&meta_value );
  if (s.ok()) {
    ParsedSetsMetaValue parsed_sets_meta_value(&meta_value);
    if (parsed_sets_meta_value.count() == 0 || parsed_sets_meta_value.IsStale()) {
      return Status::NotFound("Deleted");
    }
    SkipList skiplist = SkipList(&meta_value,SET_PREFIX_LENGTH);
    if (skiplist.count() != 0) {
      engine.seed(time(NULL));
      int32_t cur_index = 0;
      int32_t size = skiplist.count();
      int32_t target_index = engine() % (size < 50 ? size : 50);
      auto iter = skiplist.NewIterator();
      for (iter->SeekToFirst();
        iter->Valid() && cur_index < size;
        iter->Next(), cur_index++) {
        if (cur_index == target_index) {
          *member = iter->key().ToString();
          std::string mem = iter->key().ToString();
          skiplist.del(mem);
          parsed_sets_meta_value.set_count(skiplist.count());
          s = vdb_->Put(WriteOptions(), handles_[0], key, meta_value);
          delete iter;
          return s;
        }
      }
      delete iter;
    }
    return Status::NotFound();
  }
  return Status::NotFound();
}

Status RedisSets::ResetSpopCount(const std::string& key) {
  slash::MutexLock l(&spop_counts_mutex_);
  if (spop_counts_store_.map_.find(key) == spop_counts_store_.map_.end()) {
    return Status::NotFound();
  }
  spop_counts_store_.map_.erase(key);
  spop_counts_store_.list_.remove(key);
  return Status::OK();
}

Status RedisSets::AddAndGetSpopCount(const std::string& key, uint64_t* count) {
  slash::MutexLock l(&spop_counts_mutex_);
  if (spop_counts_store_.map_.find(key) == spop_counts_store_.map_.end()) {
    *count = ++spop_counts_store_.map_[key];
    spop_counts_store_.list_.push_front(key);
  } else {
    *count = ++spop_counts_store_.map_[key];
    spop_counts_store_.list_.remove(key);
    spop_counts_store_.list_.push_front(key);
  }

  if (spop_counts_store_.list_.size() > spop_counts_store_.max_size_) {
    std::string tail = spop_counts_store_.list_.back();
    spop_counts_store_.map_.erase(tail);
    spop_counts_store_.list_.pop_back();
  }
  return Status::OK();
}

Status RedisSets::SRandmember(const Slice& key, int32_t count,
                              std::vector<std::string>* members) {
  if (count == 0) {
    return Status::OK();
  }
  members->clear();
  int32_t last_seed = time(NULL);
  std::default_random_engine engine;

  VWriteBatch batch;
  ScopeRecordLock l(lock_mgr_, key);
  std::vector<int32_t> targets;
  std::unordered_set<int32_t> unique;
  Status s;
  std :: string meta_info;
  db_->Get(ReadOptions(), handles_[0], key, &meta_info );
  if (s.ok()) {
    ParsedSetsMetaValue parsed_sets_meta_value(meta_info);
    if (parsed_sets_meta_value.count() == 0 || parsed_sets_meta_value.IsStale()) {
      return Status::NotFound("Deleted");
    }
    SkipList skiplist = SkipList(&meta_info, SET_PREFIX_LENGTH , false );
     if (skiplist.count() == 0) {
      return Status::NotFound();
    } else {
      int32_t size = skiplist.count();
      if (count > 0) {
        count = count <= size ? count : size;
        while (targets.size() < static_cast<size_t>(count)) {
          engine.seed(last_seed);
          last_seed = engine();
          uint32_t pos = last_seed % size;
          if (unique.find(pos) == unique.end()) {
            unique.insert(pos);
            targets.push_back(pos);
          }
        }
      } else {
        count = -count;
        while (targets.size() < static_cast<size_t>(count)) {
          engine.seed(last_seed);
          last_seed = engine();
          targets.push_back(last_seed % size);
        }
      }
      std::sort(targets.begin(), targets.end());

      int32_t cur_index = 0, idx = 0;
      auto iter = skiplist.NewIterator();
      for (iter->SeekToFirst();
           iter->Valid() && cur_index < size;
           iter->Next(), cur_index++) {
        if (static_cast<size_t>(idx) >= targets.size()) {
          break;
        }
        ParsedSetsMemberKey parsed_sets_member_key(iter->key());
        while (static_cast<size_t>(idx) < targets.size()
          && cur_index == targets[idx]) {
          idx++;
          members->push_back(iter->key().ToString());
        }
      }
      random_shuffle(members->begin(), members->end());
      delete iter;
    }
  } else {
      return Status::NotFound();
  }
  return s;
}

Status RedisSets::SRem(const Slice& key,
                       const std::vector<std::string>& members,
                       int32_t* ret) {
  std::unordered_set<std::string> unique;
  std::vector<std::string> filtered_members;
  // Deduplication
  for (const auto& member : members) {
    if (unique.find(member) == unique.end()) {
      unique.insert(member);
      filtered_members.push_back(member);
    }
  }
                         
  *ret = 0;
  Status s;
  std::string meta_value;
  meta_value.reserve(SET_RESERVE_LENGTH);
  ScopeRecordLock l(lock_mgr_, key);
  s = db_->Get(ReadOptions(), handles_[0], key, &meta_value );
  if (s.ok()) {
    ParsedSetsMetaValue parsed_sets_meta_value(&meta_value);
    if (parsed_sets_meta_value.count() == 0 || parsed_sets_meta_value.IsStale()) {
      return Status::NotFound("Deleted");
    }
    SkipList skiplist = SkipList(&meta_value , SET_PREFIX_LENGTH ,false);
    int32_t size_before = skiplist.count();
    for (auto member : filtered_members) {
      skiplist.del(member);
    }
    *ret =  size_before - skiplist.count();
    parsed_sets_meta_value.set_count( skiplist.count());
    s = vdb_->Put(WriteOptions(), handles_[0], key, meta_value);
  } else {
    return Status::NotFound("");
  }
  return s;
}

Status RedisSets::SUnion(const std::vector<std::string>& keys,
                         std::vector<std::string>* members) {

  if (keys.size() <= 0) {
    return Status::Corruption("SDiff invalid parameter, no keys");
  }
  shannon::ReadOptions read_options;
  const shannon::Snapshot* snapshot;

  std::string meta_value;
  meta_value.reserve(SET_RESERVE_LENGTH);
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;
  std::vector<SkipList* > meta_lists;
  std::vector<std::string* > str_ptrs;
  Status s;
  std::map<std::string, bool> result_flag;
  for (auto key : keys) {
      s = db_->Get(read_options, handles_[0], key, &meta_value);
      if (s.ok()) {
        ParsedSetsMetaValue parsed_sets_meta_value(&meta_value);
        if (parsed_sets_meta_value.count() == 0 || parsed_sets_meta_value.IsStale()) {
          continue;
        }
        SkipList  skiplist =  SkipList(&meta_value, SET_PREFIX_LENGTH, false);
        auto iter = skiplist.NewIterator();
        for (iter->SeekToFirst();iter->Valid() ;iter->Next()) {
          if (result_flag.find(iter->key().ToString()) == result_flag.end()) {
            members->push_back(iter->key().ToString());
            result_flag[iter->key().ToString()] = true;
          }
        }
        delete iter;
      }
    }
  return Status::OK();
}

Status RedisSets::SUnionstore(const Slice& destination,
                              const std::vector<std::string>& keys,
                              int32_t* ret) {
  if (keys.size() <= 0) {
    return Status::Corruption("SDiff invalid parameter, no keys");
  }
  shannon::ReadOptions read_options;
  const shannon::Snapshot* snapshot;

  std::vector<std::string>members;
  std::string meta_value;
  meta_value.reserve(SET_RESERVE_LENGTH);
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;
  std::vector<SkipList* > meta_lists;
  std::vector<std::string* > str_ptrs;
  Status s;
  std::map<std::string, bool> result_flag;
  for (auto key : keys) {
      s = db_->Get(read_options, handles_[0], key, &meta_value);
    if (s.ok()) {
      ParsedSetsMetaValue parsed_sets_meta_value(&meta_value);
      if (parsed_sets_meta_value.count() == 0 || parsed_sets_meta_value.IsStale()) {
        continue;
      }
    SkipList  skiplist =  SkipList(&meta_value, SET_PREFIX_LENGTH, false);
    auto iter = skiplist.NewIterator();
    for (iter->SeekToFirst();iter->Valid() ;iter->Next()) {
      if (result_flag.find(iter->key().ToString()) == result_flag.end()) {
        members.push_back(iter->key().ToString());
        result_flag[iter->key().ToString()] = true;
      }
    }
    delete iter;
    }
  }
  string destination_value;
  destination_value.reserve(SET_RESERVE_LENGTH);
  s = db_->Get(read_options, handles_[0] , destination , &destination_value);
  SkipList destination_skip_lis = SkipList(&destination_value, SET_PREFIX_LENGTH, true);
  for (auto member : members ) {
    destination_skip_lis.insert(member);
  }
  ParsedSetsMetaValue parsed_destination_value(&meta_value);
  parsed_destination_value.InitialMetaValue();
  parsed_destination_value.set_count(destination_skip_lis.count());
  *ret = destination_skip_lis.count();
  s = vdb_->Put(WriteOptions(), handles_[0], destination, destination_value);
  return s;
}

Status RedisSets::SScan(const Slice& key, int64_t cursor, const std::string& pattern,
                        int64_t count, std::vector<std::string>* members, int64_t* next_cursor) {
  *next_cursor = 0;
  members->clear();
  if (cursor < 0) {
    *next_cursor = 0;
    return Status::OK();
  }

  int64_t rest = count;
  int64_t step_length = count;

  std::string meta_value;

  Status s = db_->Get(default_read_options_, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedSetsMetaValue parsed_sets_meta_value(&meta_value);
    if (parsed_sets_meta_value.IsStale()
      || parsed_sets_meta_value.count() == 0) {
      *next_cursor = 0;
      return Status::NotFound();
    } else {
      std::string sub_member;
      std::string start_point;
      int32_t version = parsed_sets_meta_value.version();
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

      std::string prefix = sub_member;
      SkipList skiplist = SkipList(&meta_value, SET_PREFIX_LENGTH, false);
      auto iter = skiplist.NewIterator();
      if (start_point.length() != 0) {
        iter->Seek(start_point);
      } else {
        iter->SeekToFirst();
      }
      while ( iter->Valid() && rest > 0 && iter->key().starts_with(prefix) ) {
        std::string member = iter->key().ToString();
        if (StringMatch(pattern.data(), pattern.size(), member.data(), member.size(), 0)) {
          members->push_back(member);
        }
        rest--;
        iter->Next();
      }

      if (iter->Valid()
        && (iter->key().compare(prefix) <= 0 || iter->key().starts_with(prefix))) {
        *next_cursor = cursor + step_length;
        std::string next_member = iter->key().ToString();
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

Status RedisSets::PKScanRange(const Slice& key_start, const Slice& key_end,
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
    ParsedSetsMetaValue parsed_meta_value(it->value());
    if (parsed_meta_value.IsStale()
      || parsed_meta_value.count() == 0) {
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
    ParsedSetsMetaValue parsed_sets_meta_value(it->value());
    if (parsed_sets_meta_value.IsStale()
      || parsed_sets_meta_value.count() == 0) {
      it->Next();
    } else {
      *next_key = it->key().ToString();
      break;
    }
  }
  delete it;
  return Status::OK();
}

Status RedisSets::PKRScanRange(const Slice& key_start, const Slice& key_end,
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
    ParsedSetsMetaValue parsed_sets_meta_value(it->value());
    if (parsed_sets_meta_value.IsStale()
      || parsed_sets_meta_value.count() == 0) {
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
    ParsedSetsMetaValue parsed_sets_meta_value(it->value());
    if (parsed_sets_meta_value.IsStale()
      || parsed_sets_meta_value.count() == 0) {
      it->Prev();
    } else {
      *next_key = it->key().ToString();
      break;
    }
  }
  delete it;
  return Status::OK();
}


Status RedisSets::Expire(const Slice& key, int32_t ttl) {
  std::string meta_value;
  meta_value.reserve(SET_RESERVE_LENGTH);
  Status s;
  ScopeRecordLock l(lock_mgr_, key);
  s = db_->Get(ReadOptions(), handles_[0], key,  &meta_value);
  if (s.ok()) {
    ParsedSetsMetaValue parsed_sets_meta_value(&meta_value);
    if (parsed_sets_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if (parsed_sets_meta_value.count() == 0) {
      return Status::NotFound();
    }
    if (ttl > 0) {
      parsed_sets_meta_value.SetRelativeTimestamp(ttl);
      s = vdb_->Put(WriteOptions(), handles_[0], key, meta_value);
      if (s.ok() && parsed_sets_meta_value.timestamp() > 0 ) {
        char str[sizeof(int32_t)+key.size() +1];
        str[sizeof(int32_t)+key.size() ] = '\0';
        EncodeFixed32(str,parsed_sets_meta_value.timestamp());
        memcpy(str + sizeof(int32_t) , key.data(),key.size());
        vdb_->Put(WriteOptions(), handles_[1], {str,sizeof(int32_t)+key.size()}, "1" );
      }
    } else {
      SkipList skiplist = SkipList(&meta_value , SET_PREFIX_LENGTH , true);
      parsed_sets_meta_value.InitialMetaValue();
      s = vdb_->Put(WriteOptions(), handles_[0], key, meta_value);
    }
  } else {
      s = Status::NotFound();
  }
  return s;
}

Status RedisSets::Del(const Slice& key) {
  std::string meta_value;
  Status s;
  ScopeRecordLock l(lock_mgr_, key);
  s = db_->Get(ReadOptions(),handles_[0],key,&meta_value);
  if (s.ok()) {
    ParsedSetsMetaValue parsed_sets_meta_value(&meta_value);
    if (parsed_sets_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if (parsed_sets_meta_value.count() == 0) {
      return Status::NotFound();
    } else {
    SkipList skiplist = SkipList(&meta_value, SET_PREFIX_LENGTH, true);
    parsed_sets_meta_value.InitialMetaValue();
    s = vdb_->Put(WriteOptions(), handles_[0], key, meta_value);
    }
  } else {
      s = Status::NotFound();
  }
  return s;
}

bool RedisSets::Scan(const std::string& start_key,
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
    ParsedSetsMetaValue parsed_meta_value(it->value());
    if (parsed_meta_value.IsStale()
      || parsed_meta_value.count() == 0) {
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

Status RedisSets::Expireat(const Slice& key, int32_t timestamp) {
  std::string meta_value;
  ScopeRecordLock l(lock_mgr_, key);
  meta_value.reserve(SET_RESERVE_LENGTH);
  Status s;
  s = db_->Get(ReadOptions(), handles_[0] , key ,&meta_value);
  if (s.ok()) {
    ParsedSetsMetaValue parsed_sets_meta_value (&meta_value);
    if (parsed_sets_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if (parsed_sets_meta_value.count() == 0) {
      return Status::NotFound();
    }
    parsed_sets_meta_value.set_timestamp(timestamp);
    if (parsed_sets_meta_value.timestamp() != 0 ) {
      char str[sizeof(int32_t)+key.size() +1];
      str[sizeof(int32_t)+key.size() ] = '\0';
      EncodeFixed32(str,parsed_sets_meta_value.timestamp());
      memcpy(str + sizeof(int32_t) , key.data(),key.size());
      vdb_->Put(default_write_options_,handles_[1], {str,sizeof(int32_t)+key.size()}, "1" );
    } else {
      SkipList skiplist = SkipList(&meta_value, SET_PREFIX_LENGTH, true);
      parsed_sets_meta_value.InitialMetaValue();
    }
    s = vdb_->Put(default_write_options_, handles_[0], key, meta_value);
  } else {
      s = Status::NotFound();
  }
  return s;
}

Status RedisSets::Persist(const Slice& key) {
  std::string meta_value;
  ScopeRecordLock l(lock_mgr_, key);
  meta_value.reserve(SET_RESERVE_LENGTH);
  Status s;
  s = db_->Get(ReadOptions(), handles_[0] , key ,&meta_value);
  if (s.ok()) {
    ParsedSetsMetaValue parsed_sets_meta_value (&meta_value);
    if (parsed_sets_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if (parsed_sets_meta_value.count() == 0) {
      return Status::NotFound();
    } else {
      int32_t timestamp = parsed_sets_meta_value.timestamp();
      if (timestamp == 0) {
        return Status::NotFound("Not have an associated timeout");
      } else {
        parsed_sets_meta_value.set_timestamp(0);
        s = vdb_->Put(default_write_options_, handles_[0], key, meta_value);
      }
    }
  } else {
      s = Status::NotFound();
  }
  return s;
}

Status RedisSets::TTL(const Slice& key, int64_t* timestamp) {
  Status s;
  std::string meta_value;
  meta_value.reserve(SET_RESERVE_LENGTH);
  s = db_->Get(ReadOptions(), handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedSetsMetaValue parsed_setes_meta_value(meta_value);
    if (parsed_setes_meta_value.IsStale()) {
      *timestamp = -2;
      return Status::NotFound("Stale");
    } else if (parsed_setes_meta_value.count() == 0) {
      *timestamp = -2;
      return Status::NotFound();
    } else {
      *timestamp = parsed_setes_meta_value.timestamp();
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

std::vector<shannon::ColumnFamilyHandle*> RedisSets::GetColumnFamilyHandles() {
  return handles_;
}

void RedisSets::ScanDatabase() {

  shannon::ReadOptions iterator_options;
  const shannon::Snapshot* snapshot;
  ScopeSnapshot ss(db_, &snapshot);
  iterator_options.snapshot = snapshot;
  iterator_options.fill_cache = false;
  int32_t current_time = time(NULL);

  printf("\n***************Sets Meta Data***************\n");
  auto meta_iter = db_->NewIterator(iterator_options, handles_[0]);
  for (meta_iter->SeekToFirst();
       meta_iter->Valid();
       meta_iter->Next()) {
    ParsedSetsMetaValue parsed_sets_meta_value(meta_iter->value());
    int32_t survival_time = 0;
    if (parsed_sets_meta_value.timestamp() != 0) {
      survival_time = parsed_sets_meta_value.timestamp() - current_time > 0 ?
        parsed_sets_meta_value.timestamp() - current_time : -1;
    }

    printf("[key : %-30s] [count : %-10d] [timestamp : %-10d] [version : %d] [survival_time : %d]\n",
           meta_iter->key().ToString().c_str(),
           parsed_sets_meta_value.count(),
           parsed_sets_meta_value.timestamp(),
           parsed_sets_meta_value.version(),
           survival_time);
  }
  delete meta_iter;

  printf("\n***************Sets Member Data***************\n");
  iterator_options.only_read_key = true;
  auto member_iter = db_->NewIterator(iterator_options, handles_[1]);
  for (member_iter->SeekToFirst();
       member_iter->Valid();
       member_iter->Next()) {
    ParsedSetsMemberKey parsed_sets_member_key(member_iter->key());
    printf("[key : %-30s] [member : %-20s] [version : %d]\n",
           parsed_sets_member_key.key().ToString().c_str(),
           parsed_sets_member_key.member().ToString().c_str(),
           parsed_sets_member_key.version());
  }
  delete member_iter;
}

Status RedisSets::DelTimeout(BlackWidow * bw,std::string * key) {
  Status s = Status::OK();
  shannon::Iterator *iter = db_->NewIterator(shannon::ReadOptions(), handles_[1]);
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
  memcpy(const_cast<char *>(key->data()),slice_key.data() + sizeof(int32_t),iter->key().size()-sizeof(int32_t));
    s = RealDelTimeout(bw,key);
    if (s.ok()) {
      s = vdb_->Delete(shannon::WriteOptions(), handles_[1], iter->key());
    }
  }
  else  *key = "";
  delete iter;
  return s;
}

Status RedisSets::RealDelTimeout(BlackWidow * bw,std::string * key) {
    Status s = Status::OK();
    ScopeRecordLock l(lock_mgr_, *key);
    std::string meta_value;
    meta_value.reserve(SET_RESERVE_LENGTH);
    s = db_->Get(ReadOptions() , handles_[0], *key, &meta_value );
    if (s.ok()) {
      ParsedSetsMetaValue parsed_set_meta_value(&meta_value);
      int64_t unix_time;
      shannon::Env::Default()->GetCurrentTime(&unix_time);
      if (parsed_set_meta_value.IsStale()) {
        // AddDelKey(bw, *key);
        s = vdb_->Delete(shannon::WriteOptions(), handles_[0], *key);
      }
    } else {
        s = Status::NotFound();
    }
    return s;
}

Status RedisSets::LogAdd(const Slice& key, const Slice& value,
                          const std::string& cf_name) {
  Status s;
  bool flag = false;
  for (auto cfh : handles_) {
    if (cfh->GetName() == cf_name) {
      s = vdb_->Put(default_write_options_, cfh, key, value);
      if (!s.ok()) {
        return s;
      }
      if (cf_name == "default") {
        unordered_map<std::string, std::string*>::iterator iter =
            meta_infos_set_.find(key.ToString());
        if (iter != meta_infos_set_.end()) {
          delete iter->second;
          meta_infos_set_.erase(key.ToString());
        }
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

Status RedisSets::LogDelete(const Slice& key, const std::string& cf_name) {
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
} //  namespace blackwidow
