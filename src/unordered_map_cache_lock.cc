#include "src/unordered_map_cache_lock.h"
#include "swift/shannon_db.h"

namespace blackwidow {
  std::unordered_map<std::string, std::string*>::iterator
      unordered_map_cache_lock::find(std::string key, bool not_found_insert) {
    mutex_.lock();
    std::unordered_map<std::string, std::string*>::iterator iter = map_.find(key);
    mutex_.unlock();
    if (not_found_insert && iter == map_.end()) {
      std::string *value = new std::string();
      shannon::Status s = db_->Get(shannon::ReadOptions(), cfh_, key, value);
      if (s.ok()) {
        this->insert(make_pair(key, value));
        mutex_.lock();
        iter = map_.find(key);
        mutex_.unlock();
      }
    }
    return iter;
  }

  std::unordered_map<std::string, std::string*>::iterator unordered_map_cache_lock::begin() {
    mutex_.lock();
    std::unordered_map<std::string, std::string*>::iterator iter = map_.begin();
    mutex_.unlock();
    return iter;
  }

  std::unordered_map<std::string, std::string*>::iterator unordered_map_cache_lock::end() {
    mutex_.lock();
    std::unordered_map<std::string, std::string*>::iterator iter = map_.end();
    mutex_.unlock();
    return iter;
  }

  void unordered_map_cache_lock::insert(std::pair<std::string, std::string*> data) {
    mutex_.lock();
    map_.insert(data);
    mutex_.unlock();
  }

  void unordered_map_cache_lock::erase(std::string key) {
    mutex_.lock();
    map_.erase(key);
    mutex_.unlock();
  }

  void unordered_map_cache_lock::clear() {
    mutex_.lock();
    map_.clear();
    mutex_.unlock();
  }

  void unordered_map_cache_lock::SetDb(shannon::DB* db) {
    db_ = db;
  }

  void unordered_map_cache_lock::SetColumnFamilyHandle(
          shannon::ColumnFamilyHandle* cfh) {
    cfh_ = cfh;
  }

};
