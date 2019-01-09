#include "src/unordered_map_cache_lock.h"

namespace blackwidow {
  std::unordered_map<std::string, std::string*>::iterator unordered_map_cache_lock::find(std::string key) {
    mutex_.lock();
    std::unordered_map<std::string, std::string*>::iterator iter = map_.find(key);
    mutex_.unlock();
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
};
