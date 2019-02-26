//  Copyright (c) 2017-present The blackwidow Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_REDIS_LISTS_H_
#define SRC_REDIS_LISTS_H_

#include <string>
#include <vector>
#include <unordered_set>

#include "src/redis.h"
#include "blackwidow/blackwidow.h"
#include "src/custom_comparator.h"
#include "slash/include/slash_mutex.h"

namespace blackwidow {

class RedisLists : public Redis {
  public:
    RedisLists(BlackWidow* const bw, const DataType& type);
    ~RedisLists();

    // Common commands
    virtual Status Open(const BlackwidowOptions& bw_options,
                        const std::string& db_path) override;
    virtual Status CompactRange(const shannon::Slice* begin,
                      		const shannon::Slice* end,
                      		const ColumnFamilyType& type = kMetaAndData) override;
    virtual Status GetProperty(const std::string& property, uint64_t* out) override;
    Status ScanKeyNum(KeyInfo* key_info) override;
    virtual Status ScanKeys(const std::string& pattern,
                            std::vector<std::string>* keys) override;


    // Lists commands;
    Status LIndex(const Slice& key, int64_t index, std::string* element);
    Status LInsert(const Slice& key, const BeforeOrAfter& before_or_after,
                   const std::string& pivot, const std::string& value, int64_t* ret);
    Status LLen(const Slice& key, uint64_t* len);
    Status LPop(const Slice& key, std::string* element);
    Status LPush(const Slice& key, const std::vector<std::string>& values,
                 uint64_t* ret);
    Status LPushx(const Slice& key, const Slice& value, uint64_t* len);
    Status LRange(const Slice& key, int64_t start, int64_t stop,
                  std::vector<std::string>* ret);
    Status LRem(const Slice& key, int64_t count, const Slice& value, uint64_t* ret);
    Status LSet(const Slice& key, int64_t index, const Slice& value);
    Status LTrim(const Slice& key, int64_t start, int64_t stop);
    Status RPop(const Slice& key, std::string* element);
    Status RPoplpush(const Slice& source, const Slice& destination, std::string* element);
    Status RPush(const Slice& key, const std::vector<std::string>& values,
                 uint64_t* ret);
    Status RPushx(const Slice& key, const Slice& value, uint64_t* len);
    Status PKScanRange(const Slice& key_start, const Slice& key_end,
                       const Slice& pattern, int32_t limit,
                       std::vector<std::string>* keys, std::string* next_key);
    Status PKRScanRange(const Slice& key_start, const Slice& key_end,
                        const Slice& pattern, int32_t limit,
                        std::vector<std::string>* keys, std::string* next_key);
    Status DelTimeout(BlackWidow * bw,std::string * key) ;
    Status RealDelTimeout(BlackWidow * bw,std::string * key) ;
    Status RecoveryMetaValue(Slice key, int32_t version);
    void AddRecoveryMetaValueTask(Slice key, int32_t version);

    // Keys Commands
    virtual Status Expire(const Slice& key, int32_t ttl) override;
    virtual Status Del(const Slice& key) override;
    virtual bool Scan(const std::string& start_key, const std::string& pattern,
                      std::vector<std::string>* keys,
                      int64_t* count, std::string* next_key) override;
    virtual Status Expireat(const Slice& key, int32_t timestamp) override;
    virtual Status Persist(const Slice& key) override;
    virtual Status TTL(const Slice& key, int64_t* timestamp) override;
    virtual std::vector<shannon::ColumnFamilyHandle*> GetColumnFamilyHandles() override;
    virtual Status AddDelKey(BlackWidow *bw,const string & str) override;
    virtual Status LogAdd(const Slice& key, const Slice& value,
                          const std::string& cf_name) override;
    virtual Status LogDelete(const Slice& key, const std::string &cf_name) override;
    // Iterate all data
    void ScanDatabase();

  private:
    std::vector<shannon::ColumnFamilyHandle*> handles_;
    shannon::DB* db_meta_value_log_;
    uint32_t lists_log_count_ = 0;
};

}  //  namespace blackwidow
#endif  //  SRC_REDIS_LISTS_H_
