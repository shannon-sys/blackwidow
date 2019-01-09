#ifndef _UNORDERED_MAP_LOCK_H_
#define _UNORDERED_MAP_LOCK_H_
#include <utility>
#include <string>
#include <unordered_map>
#include <mutex>

namespace blackwidow {
class unordered_map_cache_lock {
  public:
    std::unordered_map<std::string, std::string*>::iterator find(std::string);
    std::unordered_map<std::string, std::string*>::iterator begin();
    std::unordered_map<std::string, std::string*>::iterator end();
    void insert(std::pair<std::string, std::string*> data);
    void erase(std::string key);
    void clear();
  private:
    std::unordered_map<std::string, std::string*> map_;
    std::mutex mutex_;
};
}
#endif
