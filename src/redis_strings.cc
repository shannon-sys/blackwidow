//  Copyright (c) 2017-present The blackwidow Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "src/redis_strings.h"

#include <memory>
#include <climits>
#include <algorithm>
#include <limits>
#include <iostream>

#include "blackwidow/util.h"
#include "src/strings_filter.h"
#include "src/scope_record_lock.h"
#include "src/scope_snapshot.h"

#include <exception>

namespace blackwidow {
RedisStrings::RedisStrings(BlackWidow* const bw, const DataType& type)
    : Redis(bw, type) {
}

Status RedisStrings::Open(const BlackwidowOptions& bw_options,
    const std::string& db_path) {
  shannon::Options ops(bw_options.options);
  Status s = shannon::DB::Open(ops, db_path, default_device_name_, &db_);
  if (s.ok()) {
    // create column family
    shannon::ColumnFamilyHandle *tcf;
    s = db_->CreateColumnFamily(shannon::ColumnFamilyOptions(), "timeout_cf", &tcf);
    if (!s.ok()) {
      return s;
    }
    // close DB
    delete tcf;
    delete db_;
  }
  shannon::DBOptions db_ops(bw_options.options);
  shannon::ColumnFamilyOptions timeout_cf_ops(bw_options.options);
  ops.compaction_filter_factory = std::make_shared<StringsFilterFactory>();
  //use the bloom filter policy to reduce disk reads
  std::vector<shannon::ColumnFamilyDescriptor> column_families;
  column_families.push_back(shannon::ColumnFamilyDescriptor(
    shannon::kDefaultColumnFamilyName, ops));
  column_families.push_back(shannon::ColumnFamilyDescriptor(
      "timeout_cf", shannon::ColumnFamilyOptions()));
  s = shannon::DB::Open(db_ops, db_path, default_device_name_, column_families, &handles_, &db_);
  if (s.ok()) {
    vdb_ = new VDB(db_);
  }
  return  s;
}

Status RedisStrings::CompactRange(const shannon::Slice* begin,
                                  const shannon::Slice* end,
                                  const ColumnFamilyType& type) {
  return db_->CompactRange(default_compact_range_options_, begin, end);
}

Status RedisStrings::GetProperty(const std::string& property, uint64_t* out) {
  std::string value;
  db_->GetProperty(property, &value);
  *out = std::strtoull(value.c_str(), NULL, 10);
  return Status::OK();
}
Status RedisStrings::AddDelKey(BlackWidow * bw,const string & str) {
  return Status::NotSupported("not supposed yet");
};

Status RedisStrings::ScanKeyNum(KeyInfo* key_info) {
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

  // Note: This is a string type and does not need to pass the column family as
  // a parameter, use the default column family
  shannon::Iterator* iter = db_->NewIterator(iterator_options);
  for (iter->SeekToFirst();
       iter->Valid();
       iter->Next()) {
    ParsedStringsValue parsed_strings_value(iter->value());
    if (parsed_strings_value.IsStale()) {
      invaild_keys++;
    } else {
      keys++;
      if (!parsed_strings_value.IsPermanentSurvival()) {
        expires++;
        ttl_sum += parsed_strings_value.timestamp() - curtime;
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

Status RedisStrings::ScanKeys(const std::string& pattern,
                              std::vector<std::string>* keys) {
  std::string key;
  shannon::ReadOptions iterator_options;
  const shannon::Snapshot* snapshot;
  ScopeSnapshot ss(db_, &snapshot);
  iterator_options.snapshot = snapshot;
  iterator_options.fill_cache = false;

  // Note: This is a string type and does not need to pass the column family as
  // a parameter, use the default column family
  shannon::Iterator* iter = db_->NewIterator(iterator_options);
  for (iter->SeekToFirst();
       iter->Valid();
       iter->Next()) {
    ParsedStringsValue parsed_strings_value(iter->value());
    if (!parsed_strings_value.IsStale()) {
      key = iter->key().ToString();
      if (StringMatch(pattern.data(), pattern.size(), key.data(), key.size(), 0)) {
        keys->push_back(key);
      }
    }
  }
  delete iter;
  return Status::OK();
}

Status RedisStrings::Append(const Slice& key, const Slice& value,
    int32_t* ret) {
  std::string old_value;
  *ret = 0;
  ScopeRecordLock l(lock_mgr_, key);
  Status s = db_->Get(default_read_options_, key, &old_value);
  if (s.ok()) {
    ParsedStringsValue parsed_strings_value(&old_value);
    if (parsed_strings_value.IsStale()) {
      *ret = value.size();
      StringsValue strings_value(value);
      return vdb_->Put(default_write_options_, key, strings_value.Encode());
    } else {
      int32_t timestamp = parsed_strings_value.timestamp();
      std::string old_user_value = parsed_strings_value.value().ToString();
      std::string new_value = old_user_value + value.ToString();
      StringsValue strings_value(new_value);
      strings_value.set_timestamp(timestamp);
      *ret = new_value.size();
      return vdb_->Put(default_write_options_, key, strings_value.Encode());
    }
  } else if (s.IsNotFound()) {
    *ret = value.size();
    StringsValue strings_value(value);
    return vdb_->Put(default_write_options_, key, strings_value.Encode());
  }
  return s;
}

int GetBitCount(const unsigned char* value, int64_t bytes) {
  int bit_num = 0;
  static const unsigned char bitsinbyte[256] =
    {0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4,
     1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
     1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
     2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
     1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
     2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
     2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
     3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
     1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
     2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
     2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
     3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
     2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
     3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
     3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
     4, 5, 5, 6, 5, 6, 6, 7, 5, 6, 6, 7, 6, 7, 7, 8};
  for (int i = 0; i < bytes; i++) {
    bit_num += bitsinbyte[static_cast<unsigned int>(value[i])];
  }
  return bit_num;
}

Status RedisStrings::BitCount(const Slice& key,
                              int64_t start_offset, int64_t end_offset,
                              int32_t* ret, bool have_range) {
  *ret = 0;
  std::string value;
  Status s = db_->Get(default_read_options_, key, &value);
  if (s.ok()) {
    ParsedStringsValue parsed_strings_value(&value);
    if (parsed_strings_value.IsStale()) {
      return Status::NotFound("Stale");
    } else {
      parsed_strings_value.StripSuffix();
      const unsigned char* bit_value =
        reinterpret_cast<const unsigned char*>(value.data());
      int64_t value_length = value.length();
      if (have_range) {
        if (start_offset < 0) {
          start_offset = start_offset + value_length;
        }
        if (end_offset < 0) {
          end_offset = end_offset + value_length;
        }
        if (start_offset < 0) {
          start_offset = 0;
        }
        if (end_offset < 0) {
          end_offset = 0;
        }

        if (end_offset >= value_length) {
          end_offset = value_length -1;
        }
        if (start_offset > end_offset) {
          return Status::OK();
        }
      } else {
        start_offset = 0;
        end_offset = std::max(value_length - 1, static_cast<int64_t>(0));
      }
      *ret = GetBitCount(bit_value + start_offset,
                         end_offset - start_offset + 1);
    }
  } else {
    return s;
  }
  return Status::OK();
}

std::string BitOpOperate(BitOpType op,
                         const std::vector<std::string> &src_values,
                         int64_t max_len) {
  char* dest_value = new char[max_len];

  char byte, output;
  for (int64_t j = 0; j < max_len; j++) {
    if (j < static_cast<int64_t>(src_values[0].size())) {
      output = src_values[0][j];
    } else {
      output = 0;
    }
    if (op == kBitOpNot) {
      output = ~(output);
    }
    for (size_t i = 1; i < src_values.size(); i++) {
      if (static_cast<int64_t>(src_values[i].size()) - 1 >= j) {
        byte = src_values[i][j];
      } else {
        byte = 0;
      }
      switch (op) {
        case kBitOpNot:
          break;
        case kBitOpAnd:
          output &= byte;
          break;
        case kBitOpOr:
          output |= byte;
          break;
        case kBitOpXor:
          output ^= byte;
          break;
        case kBitOpDefault:
          break;
      }
    }
    dest_value[j] = output;
  }
  std::string dest_str(dest_value, max_len);
  delete[] dest_value;
  return dest_str;
}

Status RedisStrings::BitOp(BitOpType op,
                           const std::string& dest_key,
                           const std::vector<std::string>& src_keys,
                           int64_t* ret) {
  Status s;
  if (op == kBitOpNot && src_keys.size() != 1) {
    return Status::InvalidArgument("the number of source keys is not right");
  } else if (src_keys.size() < 1) {
    return Status::InvalidArgument("the number of source keys is not right");
  }

  int64_t max_len = 0, value_len = 0;
  std::vector<std::string> src_values;
  for (size_t i = 0; i < src_keys.size(); i++) {
    std::string value;
    s = db_->Get(default_read_options_, src_keys[i], &value);
    if (s.ok()) {
      ParsedStringsValue parsed_strings_value(&value);
      if (parsed_strings_value.IsStale()) {
        src_values.push_back(std::string(""));
        value_len = 0;
      } else {
        parsed_strings_value.StripSuffix();
        src_values.push_back(value);
        value_len = value.size();
      }
    } else if (s.IsNotFound()) {
      src_values.push_back(std::string(""));
      value_len = 0;
    } else {
      return s;
    }
    max_len = std::max(max_len, value_len);
  }

  std::string dest_value = BitOpOperate(op, src_values, max_len);
  *ret = dest_value.size();

  StringsValue strings_value(Slice(dest_value.c_str(),
                                   static_cast<size_t>(max_len)));
  ScopeRecordLock l(lock_mgr_, dest_key);
  return vdb_->Put(default_write_options_, dest_key, strings_value.Encode());
}

Status RedisStrings::Decrby(const Slice& key, int64_t value, int64_t* ret) {
  std::string old_value;
  std::string new_value;
  ScopeRecordLock l(lock_mgr_, key);
  Status s = db_->Get(default_read_options_, key, &old_value);
  if (s.ok()) {
    ParsedStringsValue parsed_strings_value(&old_value);
    if (parsed_strings_value.IsStale()) {
      *ret = -value;
      new_value = std::to_string(*ret);
      StringsValue strings_value(new_value);
      return vdb_->Put(default_write_options_, key, strings_value.Encode());
    } else {
      int32_t timestamp = parsed_strings_value.timestamp();
      std::string old_user_value = parsed_strings_value.value().ToString();
      char* end = nullptr;
      int64_t ival = strtoll(old_user_value.c_str(), &end, 10);
      if (*end != 0) {
        return Status::Corruption("Value is not a integer");
      }
      if ((value >= 0 && LLONG_MIN + value > ival) ||
          (value < 0 && LLONG_MAX + value < ival)) {
        return Status::InvalidArgument("Overflow");
      }
      *ret = ival - value;
      new_value = std::to_string(*ret);
      StringsValue strings_value(new_value);
      strings_value.set_timestamp(timestamp);
      return vdb_->Put(default_write_options_, key, strings_value.Encode());
    }
  } else if (s.IsNotFound()) {
    *ret = -value;
    new_value = std::to_string(*ret);
    StringsValue strings_value(new_value);
    return vdb_->Put(default_write_options_, key, strings_value.Encode());
  } else {
    return s;
  }
}

Status RedisStrings::Get(const Slice& key, std::string* value) {
  value->clear();
  Status s = db_->Get(default_read_options_, key, value);
  if (s.ok()) {
    ParsedStringsValue parsed_strings_value(value);
    if (parsed_strings_value.IsStale()) {
      value->clear();
      return Status::NotFound("Stale");
    } else {
      parsed_strings_value.StripSuffix();
    }
  }
  return s;
}

Status RedisStrings::GetBit(const Slice& key, int64_t offset, int32_t* ret) {
  std::string meta_value;
  Status s = db_->Get(default_read_options_, key, &meta_value);
  if (s.ok() || s.IsNotFound()) {
    std::string data_value;
    if (s.ok()) {
      ParsedStringsValue parsed_strings_value(&meta_value);
      if (parsed_strings_value.IsStale()) {
        *ret = 0;
        return Status::OK();
      } else {
        data_value = parsed_strings_value.value().ToString();
      }
    }
    size_t byte = offset >> 3;
    size_t bit = 7 - (offset & 0x7);
    if (byte + 1 > data_value.length()) {
      *ret = 0;
    } else {
      *ret = (data_value[byte] & 1<<bit) >> bit;
    }
  } else {
    return s;
  }
  return Status::OK();
}

Status RedisStrings::Getrange(const Slice& key,
                              int64_t start_offset, int64_t end_offset,
                              std::string* ret) {
  *ret = "";
  std::string value;
  Status s = db_->Get(default_read_options_, key, &value);
  if (s.ok()) {
    ParsedStringsValue parsed_strings_value(&value);
    if (parsed_strings_value.IsStale()) {
      return Status::NotFound("Stale");
    } else {
      parsed_strings_value.StripSuffix();
      int64_t size = value.size();
      int64_t start_t = start_offset >= 0 ? start_offset : size + start_offset;
      int64_t end_t = end_offset >= 0 ? end_offset : size + end_offset;
      if (start_t > size - 1 ||
          (start_t != 0 && start_t > end_t) ||
          (start_t != 0 && end_t < 0)
          ) {
        return Status::OK();
      }
      if (start_t < 0) {
        start_t  = 0;
      }
      if (end_t >= size) {
        end_t = size - 1;
      }
      if (start_t == 0 && end_t < 0) {
        end_t = 0;
      }
      *ret = value.substr(start_t, end_t-start_t+1);
      return Status::OK();
    }
  } else {
    return s;
  }
}

Status RedisStrings::GetSet(const Slice& key, const Slice& value,
                            std::string* old_value) {
  ScopeRecordLock l(lock_mgr_, key);
  Status s = db_->Get(default_read_options_, key, old_value);
  if (s.ok()) {
    ParsedStringsValue parsed_strings_value(old_value);
    if (parsed_strings_value.IsStale()) {
      *old_value = "";
    } else {
      parsed_strings_value.StripSuffix();
    }
  } else if (!s.IsNotFound()) {
    return s;
  }
  StringsValue strings_value(value);
  return vdb_->Put(default_write_options_, key, strings_value.Encode());
}

Status RedisStrings::Incrby(const Slice& key, int64_t value, int64_t* ret) {
  std::string old_value;
  std::string new_value;
  ScopeRecordLock l(lock_mgr_, key);
  Status s = db_->Get(default_read_options_, key, &old_value);
  if (s.ok()) {
    ParsedStringsValue parsed_strings_value(&old_value);
    if (parsed_strings_value.IsStale()) {
      *ret = value;
      char buf[32];
      Int64ToStr(buf, 32, value);
      StringsValue strings_value(buf);
      return vdb_->Put(default_write_options_, key, strings_value.Encode());
    } else {
      int32_t timestamp = parsed_strings_value.timestamp();
      std::string old_user_value = parsed_strings_value.value().ToString();
      char* end = nullptr;
      int64_t ival = strtoll(old_user_value.c_str(), &end, 10);
      if (*end != 0) {
        return Status::Corruption("Value is not a integer");
      }
      if ((value >= 0 && LLONG_MAX - value < ival) ||
          (value < 0 && LLONG_MIN - value > ival)) {
        return Status::InvalidArgument("Overflow");
      }
      *ret = ival + value;
      new_value = std::to_string(*ret);
      StringsValue strings_value(new_value);
      strings_value.set_timestamp(timestamp);
      return vdb_->Put(default_write_options_, key, strings_value.Encode());
    }
  } else if (s.IsNotFound()) {
    *ret = value;
    char buf[32];
    Int64ToStr(buf, 32, value);
    StringsValue strings_value(buf);
    return vdb_->Put(default_write_options_, key, strings_value.Encode());
  } else {
    return s;
  }
}

Status RedisStrings::Incrbyfloat(const Slice& key, const Slice& value,
                                 std::string* ret) {
  std::string old_value, new_value;
  long double long_double_by;
  if (StrToLongDouble(value.data(), value.size(), &long_double_by) == -1) {
    return Status::Corruption("Value is not a vaild float");
  }
  ScopeRecordLock l(lock_mgr_, key);
  Status s = db_->Get(default_read_options_, key, &old_value);
  if (s.ok()) {
    ParsedStringsValue parsed_strings_value(&old_value);
    if (parsed_strings_value.IsStale()) {
      LongDoubleToStr(long_double_by, &new_value);
      *ret = new_value;
      StringsValue strings_value(new_value);
      return vdb_->Put(default_write_options_, key, strings_value.Encode());
    } else {
      int32_t timestamp = parsed_strings_value.timestamp();
      std::string old_user_value = parsed_strings_value.value().ToString();
      long double total, old_number;
      if (StrToLongDouble(old_user_value.data(),
                          old_user_value.size(), &old_number) == -1) {
        return Status::Corruption("Value is not a vaild float");
      }
      total = old_number + long_double_by;
      if (LongDoubleToStr(total, &new_value) == -1) {
        return Status::InvalidArgument("Overflow");
      }
      *ret = new_value;
      StringsValue strings_value(new_value);
      strings_value.set_timestamp(timestamp);
      return vdb_->Put(default_write_options_, key, strings_value.Encode());
    }
  } else if (s.IsNotFound()) {
    LongDoubleToStr(long_double_by, &new_value);
    *ret = new_value;
    StringsValue strings_value(new_value);
    return vdb_->Put(default_write_options_, key, strings_value.Encode());
  } else {
    return s;
  }
}

Status RedisStrings::MGet(const std::vector<std::string>& keys,
                          std::vector<ValueStatus>* vss) {
  vss->clear();

  Status s;
  shannon::ReadOptions read_options;
  const shannon::Snapshot* snapshot;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;
  shannon::ReadBatch read_batch;
  std::vector<std::string> values;
  int read_count = 0;
  for (const auto& key : keys) {
    s = read_batch.Get(key);
    read_count ++;
    if (s.IsBatchFull()) {
      s = db_->Read(read_options, &read_batch, &values);
      if (!s.ok()) {
        return s;
      }
      for (auto v : values) {
        if (v.size() == 0) {
	      vss->push_back({std::string(), Status::NotFound("Stale")});
	    } else {
	      ParsedStringsValue parsed_strings_value(&v);
          if (parsed_strings_value.IsStale()) {
            vss->push_back({std::string(), Status::NotFound("Stale")});
          } else {
            vss->push_back({parsed_strings_value.user_value().ToString(), Status::OK()});
          }
	  }
    }
      read_batch.Clear();
      read_count = 0;
    } // end batch full
  }
  if (read_count > 0) {
    s = db_->Read(read_options, &read_batch, &values);
    if (!s.ok()) {
      return s;
    }
    for (auto v : values) {
      if (v.size() == 0) {
        vss->push_back({std::string(), Status::NotFound("Stale")});
      } else {
	    ParsedStringsValue parsed_strings_value(&v);
        if (parsed_strings_value.IsStale()) {
	      vss->push_back({std::string(), Status::NotFound("Stale")});
        } else {
          std::string vv = parsed_strings_value.user_value().ToString();
          vss->push_back({parsed_strings_value.user_value().ToString(), Status::OK()});
        }
      }
    }
    read_batch.Clear();
    read_count = 0;
  } // end read_batch read
  return Status::OK();
}

Status RedisStrings::MSet(const std::vector<KeyValue>& kvs) {
  std::vector<std::string> keys;
  for (const auto& kv :  kvs) {
    keys.push_back(kv.key);
  }

  MultiScopeRecordLock ml(lock_mgr_, keys);
  VWriteBatch batch;
  for (const auto& kv : kvs) {
    StringsValue strings_value(kv.value);
    batch.Put(kv.key, strings_value.Encode());
  }
  return vdb_->Write(default_write_options_, &batch);
}

Status RedisStrings::MSetnx(const std::vector<KeyValue>& kvs,
                            int32_t* ret) {
  Status s;
  bool exists = false;
  *ret = 0;
  std::string value;
  for (size_t i = 0; i < kvs.size(); i++) {
    s = db_->Get(default_read_options_, kvs[i].key, &value);
    if (s.ok()) {
      ParsedStringsValue parsed_strings_value(&value);
      if (!parsed_strings_value.IsStale()) {
        exists = true;
        break;
      }
    }
  }
  if (!exists) {
    s = MSet(kvs);
    if (s.ok()) {
      *ret = 1;
    }
  }
  return s;
}

Status RedisStrings::Set(const Slice& key, const Slice& value, const int32_t ttl) {
  StringsValue strings_value(value);
  ScopeRecordLock l(lock_mgr_, key);
  if (ttl > 0) {
    strings_value.SetRelativeTimestamp(ttl);
    char str[sizeof(int32_t)+key.size() +1];
    str[sizeof(int32_t)+key.size() ] = '\0';
    EncodeFixed32(str,strings_value.timestamp());
    memcpy(str + sizeof(int32_t) , key.data(),key.size());
    vdb_->Put(default_write_options_,handles_[1],{str,sizeof(int32_t)+key.size()}, "1" );
  }
  return vdb_->Put(default_write_options_, key, strings_value.Encode());
}

Status RedisStrings::Setxx(const Slice& key, const Slice& value, int32_t* ret, const int32_t ttl) {
  bool not_found = true;
  std::string old_value;
  StringsValue strings_value(value);
  ScopeRecordLock l(lock_mgr_, key);
  Status s = db_->Get(default_read_options_, key, &old_value);
  if (s.ok()) {
    ParsedStringsValue parsed_strings_value(old_value);
    if (!parsed_strings_value.IsStale()) {
      not_found = false;
    }
  } else if (!s.IsNotFound()) {
    return s;
  }

  if (not_found) {
    *ret = 0;
    return s;
  } else {
    *ret = 1;
    if (ttl > 0) {
      strings_value.SetRelativeTimestamp(ttl);
    }
    return vdb_->Put(default_write_options_, key, strings_value.Encode());
  }
}

Status RedisStrings::SetBit(const Slice& key, int64_t offset,
                            int32_t on, int32_t* ret) {
  std::string meta_value;
  if (offset < 0) {
    return Status::InvalidArgument("offset < 0");
  }

  ScopeRecordLock l(lock_mgr_, key);
  Status s = db_->Get(default_read_options_, key, &meta_value);
  if (s.ok() || s.IsNotFound()) {
    std::string data_value;
    if (s.ok()) {
      ParsedStringsValue parsed_strings_value(&meta_value);
      if (!parsed_strings_value.IsStale()) {
        data_value = parsed_strings_value.value().ToString();
      }
    }
    size_t byte = offset >> 3;
    size_t bit = 7 - (offset & 0x7);
    char byte_val;
    size_t value_lenth = data_value.length();
    if (byte + 1 > value_lenth) {
      *ret = 0;
      byte_val = 0;
    } else {
      *ret = (data_value[byte] & 1<<bit) >> bit;
      byte_val = data_value[byte];
    }
    if (*ret == on) {
      return Status::OK();
    }
    byte_val &= (char) ~(1 << bit);
    byte_val |= (char) ((on & 0x1) << bit);
    if (byte + 1 <= value_lenth) {
      data_value.replace(byte, 1, &byte_val, 1);
    } else {
      data_value.append(byte + 1 - value_lenth - 1, 0);
      data_value.append(1, byte_val);
    }
    StringsValue strings_value(data_value);
    return  vdb_->Put(shannon::WriteOptions(), key, strings_value.Encode());
  } else {
    return s;
  }
}

Status RedisStrings::Setex(const Slice& key, const Slice& value, int32_t ttl) {
  if (ttl <= 0) {
    return Status::InvalidArgument("invalid expire time");
  }
  StringsValue strings_value(value);
  strings_value.SetRelativeTimestamp(ttl);
  ScopeRecordLock l(lock_mgr_, key);
  return vdb_->Put(default_write_options_, key, strings_value.Encode());
}

Status RedisStrings::Setnx(const Slice& key, const Slice& value, int32_t* ret, const int32_t ttl) {
  *ret = 0;
  std::string old_value;
  ScopeRecordLock l(lock_mgr_, key);
  Status s = db_->Get(default_read_options_, key, &old_value);
  if (s.ok()) {
    ParsedStringsValue parsed_strings_value(&old_value);
    if (parsed_strings_value.IsStale()) {
      StringsValue strings_value(value);
      if (ttl > 0) {
        strings_value.SetRelativeTimestamp(ttl);
      }
      s = vdb_->Put(default_write_options_, key, strings_value.Encode());
      if (s.ok()) {
        *ret = 1;
      }
    }
  } else if (s.IsNotFound()) {
    StringsValue strings_value(value);
    if (ttl > 0) {
      strings_value.SetRelativeTimestamp(ttl);
    }
    s = vdb_->Put(default_write_options_, key, strings_value.Encode());
    if (s.ok()) {
      *ret = 1;
    }
  }
  return s;
}

Status RedisStrings::Setvx(const Slice& key, const Slice& value,
                           const Slice& new_value, int32_t* ret, const int32_t ttl) {
  *ret = 0;
  std::string old_value;
  ScopeRecordLock l(lock_mgr_, key);
  Status s = db_->Get(default_read_options_, key, &old_value);
  if (s.ok()) {
    ParsedStringsValue parsed_strings_value(&old_value);
    if (parsed_strings_value.IsStale()) {
      *ret = 0;
    } else {
      if (!value.compare(parsed_strings_value.value())) {
        StringsValue strings_value(new_value);
        if (ttl > 0) {
          strings_value.SetRelativeTimestamp(ttl);
        }
        s = vdb_->Put(default_write_options_, key, strings_value.Encode());
        if (!s.ok()) {
          return s;
        }
        *ret = 1;
      } else {
        *ret = -1;
      }
    }
  } else if (s.IsNotFound()) {
    *ret = 0;
  } else {
    return s;
  }
  return Status::OK();
}

Status RedisStrings::Delvx(const Slice& key, const Slice& value, int32_t* ret) {
  *ret = 0;
  std::string old_value;
  ScopeRecordLock l(lock_mgr_, key);
  Status s = db_->Get(default_read_options_, key, &old_value);
  if (s.ok()) {
    ParsedStringsValue parsed_strings_value(&old_value);
    if (parsed_strings_value.IsStale()) {
      *ret = 0;
      return Status::NotFound("Stale");
    } else {
      if (!value.compare(parsed_strings_value.value())) {
        *ret = 1;
        return vdb_->Delete(default_write_options_, key);
      } else {
        *ret = -1;
      }
    }
  } else if (s.IsNotFound()) {
    *ret = 0;
  }
  return s;
}

Status RedisStrings::Setrange(const Slice& key, int64_t start_offset,
                              const Slice& value, int32_t* ret) {
  std::string old_value;
  std::string new_value;
  if (start_offset < 0) {
    return Status::InvalidArgument("offset < 0");
  }

  ScopeRecordLock l(lock_mgr_, key);
  Status s = db_->Get(default_read_options_, key, &old_value);
  if (s.ok()) {
    ParsedStringsValue parsed_strings_value(&old_value);
    parsed_strings_value.StripSuffix();
    if (parsed_strings_value.IsStale()) {
      std::string tmp(start_offset, '\0');
      new_value = tmp.append(value.data());
      *ret = new_value.length();
    } else {
      if (static_cast<size_t>(start_offset) > old_value.length()) {
        old_value.resize(start_offset);
        new_value = old_value.append(value.data());
      } else {
        std::string head = old_value.substr(0, start_offset);
        std::string tail;
        if (start_offset + value.size() - 1 < old_value.length() - 1) {
          tail = old_value.substr(start_offset + value.size());
        }
        new_value = head + value.data() + tail;
      }
    }
    *ret = new_value.length();
    StringsValue strings_value(new_value);
    return vdb_->Put(default_write_options_, key, strings_value.Encode());
  } else if (s.IsNotFound()) {
    std::string tmp(start_offset, '\0');
    new_value = tmp.append(value.data());
    *ret = new_value.length();
    StringsValue strings_value(new_value);
    return vdb_->Put(default_write_options_, key, strings_value.Encode());
  }
  return s;
}

Status RedisStrings::Strlen(const Slice& key, int32_t *len) {
  std::string value;
  Status s = Get(key, &value);
  if (s.ok()) {
    *len = value.size();
  } else {
    *len = 0;
  }
  return s;
}

int32_t GetBitPos(const unsigned char* s, unsigned int bytes, int bit) {
  uint64_t word = 0;
  uint64_t skip_val = 0;
  unsigned char* value = const_cast<unsigned char *>(s);
  uint64_t* l = reinterpret_cast<uint64_t *>(value);
  int pos = 0;
  if (bit == 0) {
    skip_val = std::numeric_limits<uint64_t>::max();
  } else {
    skip_val = 0;
  }
  // skip 8 bytes at one time, find the first int64 that should not be skipped
  while (bytes >= sizeof(*l)) {
    if (*l != skip_val) {
      break;
    }
    l++;
    bytes = bytes - sizeof(*l);
    pos = pos + 8 * sizeof(*l);
  }
  unsigned char * c = reinterpret_cast<unsigned char *>(l);
  for (size_t j = 0; j < sizeof(*l); j++) {
    word = word << 8;
    if (bytes) {
      word = word | *c;
      c++;
      bytes--;
    }
  }
  if (bit == 1 && word == 0) {
    return -1;
  }
  // set each bit of mask to 0 except msb
  uint64_t mask = std::numeric_limits<uint64_t>::max();
  mask = mask >> 1;
  mask = ~(mask);
  while (mask) {
    if (((word & mask) != 0) == bit) {
      return pos;
    }
    pos++;
    mask = mask >> 1;
  }
  return pos;
}

Status RedisStrings::BitPos(const Slice& key, int32_t bit,
                            int64_t* ret) {
  Status s;
  std::string value;
  s = db_->Get(default_read_options_, key, &value);
  if (s.ok()) {
    ParsedStringsValue parsed_strings_value(&value);
    if (parsed_strings_value.IsStale()) {
      if (bit == 1) {
        *ret = -1;
      } else if (bit == 0) {
        *ret = 0;
      }
      return Status::NotFound("Stale");
    } else {
      parsed_strings_value.StripSuffix();
      const unsigned char* bit_value =
        reinterpret_cast<const unsigned char* >(value.data());
      int64_t value_length = value.length();
      int64_t start_offset = 0;
      int64_t end_offset = std::max(value_length - 1, static_cast<int64_t>(0));
      int64_t bytes = end_offset - start_offset + 1;
      int64_t pos = GetBitPos(bit_value + start_offset, bytes, bit);
      if (pos == (8 * bytes) && bit == 0) {
        pos = -1;
      }
      if (pos != -1) {
        pos = pos + 8 * start_offset;
      }
      *ret = pos;
    }
  } else {
    return s;
  }
  return Status::OK();
}

Status RedisStrings::PKScanRange(const Slice& key_start, const Slice& key_end,
                                 const Slice& pattern, int32_t limit,
                                 std::vector<KeyValue>* kvs, std::string* next_key) {
  next_key->clear();

  std::string key, value;
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

  // Note: This is a string type and does not need to pass the column family as
  // a parameter, use the default column family
  shannon::Iterator* it = db_->NewIterator(iterator_options);
  if (start_no_limit) {
    it->SeekToFirst();
  } else {
    it->Seek(key_start);
  }

  while (it->Valid() && remain > 0
    && (end_no_limit || it->key().compare(key_end) <= 0)) {
    ParsedStringsValue parsed_strings_value(it->value());
    if (parsed_strings_value.IsStale()) {
      it->Next();
    } else {
      key = it->key().ToString();
      value = parsed_strings_value.value().ToString();
      if (StringMatch(pattern.data(), pattern.size(),
                         key.data(), key.size(), 0)) {
        kvs->push_back({key, value});
      }
      remain--;
      it->Next();
    }
  }

  while (it->Valid()
    && (end_no_limit || it->key().compare(key_end) <= 0)) {
    ParsedStringsValue parsed_strings_value(it->value());
    if (parsed_strings_value.IsStale()) {
      it->Next();
    } else {
      *next_key = it->key().ToString();
      break;
    }
  }
  delete it;
  return Status::OK();
}

Status RedisStrings::PKRScanRange(const Slice& key_start, const Slice& key_end,
                                  const Slice& pattern, int32_t limit,
                                  std::vector<KeyValue>* kvs, std::string* next_key) {
  std::string key, value;
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

  // Note: This is a string type and does not need to pass the column family as
  // a parameter, use the default column family
  shannon::Iterator* it = db_->NewIterator(iterator_options);
  if (start_no_limit) {
    it->SeekToLast();
  } else {
    it->SeekForPrev(key_start);
  }

  while (it->Valid() && remain > 0
    && (end_no_limit || it->key().compare(key_end) >= 0)) {
    ParsedStringsValue parsed_strings_value(it->value());
    if (parsed_strings_value.IsStale()) {
      it->Prev();
    } else {
      key = it->key().ToString();
      value = parsed_strings_value.value().ToString();
      if (StringMatch(pattern.data(), pattern.size(),
                         key.data(), key.size(), 0)) {
        kvs->push_back({key, value});
      }
      remain--;
      it->Prev();
    }
  }

  while (it->Valid()
    && (end_no_limit || it->key().compare(key_end) >= 0)) {
    ParsedStringsValue parsed_strings_value(it->value());
    if (parsed_strings_value.IsStale()) {
      it->Prev();
    } else {
      *next_key = it->key().ToString();
      break;
    }
  }
  delete it;
  return Status::OK();
}

Status RedisStrings::BitPos(const Slice& key, int32_t bit,
                            int64_t start_offset, int64_t* ret) {
  Status s;
  std::string value;
  s = db_->Get(default_read_options_, key, &value);
  if (s.ok()) {
    ParsedStringsValue parsed_strings_value(&value);
    if (parsed_strings_value.IsStale()) {
      if (bit == 1) {
        *ret = -1;
      } else if (bit == 0) {
        *ret = 0;
      }
      return Status::NotFound("Stale");
    } else {
      parsed_strings_value.StripSuffix();
      const unsigned char* bit_value =
        reinterpret_cast<const unsigned char* >(value.data());
      int64_t value_length = value.length();
      int64_t end_offset = std::max(value_length - 1, static_cast<int64_t>(0));
      if (start_offset < 0) {
        start_offset = start_offset + value_length;
      }
      if (start_offset < 0) {
        start_offset = 0;
      }
      if (start_offset > end_offset) {
        *ret = -1;
        return Status::OK();
      }
      if (start_offset > value_length - 1) {
        *ret = -1;
        return Status::OK();
      }
      int64_t bytes = end_offset - start_offset + 1;
      int64_t pos = GetBitPos(bit_value + start_offset, bytes, bit);
      if (pos == (8 * bytes) && bit == 0) {
        pos = -1;
      }
      if (pos != -1) {
        pos = pos + 8 * start_offset;
      }
      *ret = pos;
    }
  } else {
    return s;
  }
  return Status::OK();
}

Status RedisStrings::BitPos(const Slice& key, int32_t bit,
                            int64_t start_offset, int64_t end_offset,
                            int64_t* ret) {
  Status s;
  std::string value;
  s = db_->Get(default_read_options_, key, &value);
  if (s.ok()) {
    ParsedStringsValue parsed_strings_value(&value);
    if (parsed_strings_value.IsStale()) {
      if (bit == 1) {
        *ret = -1;
      } else if (bit == 0) {
        *ret = 0;
      }
      return Status::NotFound("Stale");
    } else {
      parsed_strings_value.StripSuffix();
      const unsigned char* bit_value =
        reinterpret_cast<const unsigned char* >(value.data());
      int64_t value_length = value.length();
      if (start_offset < 0) {
        start_offset = start_offset + value_length;
      }
      if (start_offset < 0) {
        start_offset = 0;
      }
      if (end_offset < 0) {
        end_offset = end_offset + value_length;
      }
      // converting to int64_t just avoid warning
      if (end_offset > static_cast<int64_t>(value.length()) - 1) {
        end_offset = value_length - 1;
      }
      if (end_offset < 0) {
        end_offset = 0;
      }
      if (start_offset > end_offset) {
        *ret = -1;
        return Status::OK();
      }
      if (start_offset > value_length - 1) {
        *ret = -1;
        return Status::OK();
      }
      int64_t bytes = end_offset - start_offset + 1;
      int64_t pos = GetBitPos(bit_value + start_offset, bytes, bit);
      if (pos == (8 * bytes) && bit == 0) {
        pos = -1;
      }
      if (pos != -1) {
        pos = pos + 8 * start_offset;
      }
      *ret = pos;
    }
  } else {
    return s;
  }
  return Status::OK();
}

Status RedisStrings::Expire(const Slice& key, int32_t ttl) {
  std::string value;
  ScopeRecordLock l(lock_mgr_, key);
  Status s = db_->Get(default_read_options_, key, &value);
  if (s.ok()) {
    ParsedStringsValue parsed_strings_value(&value);
    if (parsed_strings_value.IsStale()) {
      return Status::NotFound("Stale");
    }
    if (ttl > 0) {
      parsed_strings_value.SetRelativeTimestamp(ttl);
      char str[sizeof(int32_t)+key.size() +1];
      str[sizeof(int32_t)+key.size() ] = '\0';
      EncodeFixed32(str,parsed_strings_value.timestamp());
      memcpy(str + sizeof(int32_t) , key.data(),key.size());
      vdb_->Put(default_write_options_,handles_[1],{str,sizeof(int32_t)+key.size()}, "1" );
      return vdb_->Put(default_write_options_,key, value);
    } else {
      return vdb_->Delete(default_write_options_, key);
    }
  }
  return s;
}

Status RedisStrings::Del(const Slice& key) {
  std::string value;
  ScopeRecordLock l(lock_mgr_, key);
  Status s = db_->Get(default_read_options_, key, &value);
  if (s.ok()) {
    ParsedStringsValue parsed_strings_value(&value);
    if (parsed_strings_value.IsStale()) {
      return Status::NotFound("Stale");
    }
    return vdb_->Delete(default_write_options_, key);
  }
  return s;
}

bool RedisStrings::Scan(const std::string& start_key,
                        const std::string& pattern,
                        std::vector<std::string>* keys,
                        int64_t* count,
                        std::string* next_key) {
  std::string key;
  bool is_finish = true;
  shannon::ReadOptions iterator_options;
  const shannon::Snapshot* snapshot;
  ScopeSnapshot ss(db_, &snapshot);
  iterator_options.snapshot = snapshot;
  iterator_options.fill_cache = false;

  // Note: This is a string type and does not need to pass the column family as
  // a parameter, use the default column family
  shannon::Iterator* it = db_->NewIterator(iterator_options);
  it->Seek(start_key);
  while (it->Valid() && (*count) > 0) {
    ParsedStringsValue parsed_strings_value(it->value());
    if (parsed_strings_value.IsStale()) {
      it->Next();
      continue;
    } else {
      key = it->key().ToString();
      if (StringMatch(pattern.data(), pattern.size(),
                         key.data(), key.size(), 0)) {
        keys->push_back(key);
      }
      (*count)--;
      it->Next();
    }
  }
  std::string prefix = isTailWildcard(pattern) ?
    pattern.substr(0, pattern.size() - 1) : "";
  if (it->Valid()
    && (it->key().compare(prefix) <= 0 || it->key().starts_with(prefix))) {
    is_finish = false;
    *next_key = it->key().ToString();
  } else {
    *next_key = "";
  }
  delete it;
  return is_finish;
}

Status RedisStrings::Expireat(const Slice& key, int32_t timestamp) {
  std::string value;
  ScopeRecordLock l(lock_mgr_, key);
  Status s = db_->Get(default_read_options_, key, &value);
  if (s.ok()) {
    ParsedStringsValue parsed_strings_value(&value);
    if (parsed_strings_value.IsStale()) {
      return Status::NotFound("Stale");
    } else {
      if (timestamp > 0) {
        parsed_strings_value.set_timestamp(timestamp);
        char str[sizeof(int32_t)+key.size() +1];
        str[sizeof(int32_t)+key.size() ] = '\0';
        EncodeFixed32(str,parsed_strings_value.timestamp());
        memcpy(str + sizeof(int32_t) , key.data(),key.size());
        vdb_->Put(default_write_options_,handles_[1],{str,sizeof(int32_t)+key.size()}, "1" );
        return vdb_->Put(default_write_options_, key, value);
      } else {
        return vdb_->Delete(default_write_options_, key);
      }
    }
  }
  return s;
}

Status RedisStrings::Persist(const Slice& key) {
  std::string value;
  ScopeRecordLock l(lock_mgr_, key);
  Status s = db_->Get(default_read_options_, key, &value);
  if (s.ok()) {
    ParsedStringsValue parsed_strings_value(&value);
    if (parsed_strings_value.IsStale()) {
      return Status::NotFound("Stale");
    } else {
      int32_t timestamp = parsed_strings_value.timestamp();
      if (timestamp == 0) {
        return Status::NotFound("Not have an associated timeout");
      } else {
        parsed_strings_value.set_timestamp(0);
        return vdb_->Put(default_write_options_, key, value);
      }
    }
  }
  return s;
}

Status RedisStrings::TTL(const Slice& key, int64_t* timestamp) {
  std::string value;
  ScopeRecordLock l(lock_mgr_, key);
  Status s = db_->Get(default_read_options_, key, &value);
  if (s.ok()) {
    ParsedStringsValue parsed_strings_value(&value);
    if (parsed_strings_value.IsStale()) {
      *timestamp = -2;
      return Status::NotFound("Stale");
    } else {
      *timestamp = parsed_strings_value.timestamp();
      if (*timestamp == 0) {
        *timestamp = -1;
      } else {
        int64_t curtime;
        shannon::Env::Default()->GetCurrentTime(&curtime);
        *timestamp = *timestamp - curtime > 0 ? *timestamp - curtime : -2;
      }
    }
  } else if (s.IsNotFound()) {
    *timestamp = -2;
  }
  return s;
}

std::vector<shannon::ColumnFamilyHandle*> RedisStrings::GetColumnFamilyHandles() {
  return handles_;
}
void RedisStrings::ScanDatabase() {

  shannon::ReadOptions iterator_options;
  const shannon::Snapshot* snapshot;
  ScopeSnapshot ss(db_, &snapshot);
  iterator_options.snapshot = snapshot;
  iterator_options.fill_cache = false;
  int32_t current_time = time(NULL);

  printf("\n***************String Data***************\n");
  auto iter = db_->NewIterator(iterator_options);
  for (iter->SeekToFirst();
       iter->Valid();
       iter->Next()) {
    ParsedStringsValue parsed_strings_value(iter->value());
    int32_t survival_time = 0;
    if (parsed_strings_value.timestamp() != 0) {
      survival_time = parsed_strings_value.timestamp() - current_time > 0 ?
        parsed_strings_value.timestamp() - current_time : -1;
    }

    printf("[key : %-30s] [value : %-30s] [timestamp : %-10d] [version : %d] [survival_time : %d]\n",
           iter->key().ToString().c_str(),
           parsed_strings_value.value().ToString().c_str(),
           parsed_strings_value.timestamp(),
           parsed_strings_value.version(),
           survival_time);
  }
  delete iter;
}
Status RedisStrings::DelTimeout(BlackWidow * bw,std::string * key) {
  Status s = Status::OK();
  shannon::ReadOptions read_options;
  read_options.only_read_key = true;
  shannon::Iterator *iter = db_->NewIterator(read_options, handles_[1]);
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
  Slice  slice_key = iter->key();
  int64_t cur_meta_timestamp_ = DecodeFixed32(slice_key.data());
  int64_t unix_time;
  cout <<"curtime"<< cur_meta_timestamp_ << "  "<< iter->key().data() <<endl;
  shannon::Env::Default()->GetCurrentTime(&unix_time);
  if (cur_meta_timestamp_ > 0 && cur_meta_timestamp_ < static_cast<int32_t>(unix_time))
  {
    cout << " substr error  : "<<endl ;
    key->resize(iter->key().size()-sizeof(int32_t));
    memcpy(const_cast<char *>(key->data()),slice_key.data()+sizeof(int32_t),iter->key().size()-sizeof(int32_t));
    ScopeRecordLock l(lock_mgr_, *key);
    std::string value;
      s = db_->Get(default_read_options_, *key, &value);
        if (s.ok()) {
          ParsedStringsValue parsed_strings_value(&value);
          if (parsed_strings_value.IsStale()) {
            vdb_->Delete(default_write_options_,*key);
          }
        }
      s = vdb_->Delete(shannon::WriteOptions(), handles_[1], iter->key());
  }
  else  *key = "";
  delete iter;
  return s;
}

Status RedisStrings::LogAdd(const Slice& key, const Slice& value,
                          const std::string& cf_name) {
  return vdb_->Put(default_write_options_, key, value);
}

Status RedisStrings::LogDelete(const Slice& key, const std::string& cf_name) {
  return vdb_->Delete(default_write_options_, key);
}

}  //  namespace blackwidow
