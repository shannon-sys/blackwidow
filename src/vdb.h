#ifndef VDB_H_
#define VDB_H_

#include "swift/shannon_db.h"
#include <atomic>

using namespace shannon;

namespace blackwidow {

class VWriteBatch {
  public:
    VWriteBatch() {
      size_ = 0;
    }
    Status Put(ColumnFamilyHandle* column_family, const Slice& key,
            const Slice& value) {
      size_ += key.size() + value.size();
      return write_batch_.Put(column_family, key, value);
    }

    Status Put(const Slice& key, const Slice& value) {
      size_ += key.size() + value.size();
      return write_batch_.Put(key, value);
    }

    Status Delete(ColumnFamilyHandle* column_family, const Slice& key) {
      size_ += key.size();
      return write_batch_.Delete(column_family, key);
    }

    Status Delete(const Slice& key) {
      size_ += key.size();
      return write_batch_.Delete(key);
    }

    void Clear() {
      size_ = 0;
      return write_batch_.Clear();
    }

    int64_t size() {
      return size_;
    }

    shannon::WriteBatch* write_batch() {
      return &write_batch_;
    }
 private:
   int64_t size_;
   shannon::WriteBatch write_batch_;
};

class VDB {
 public:
   VDB(DB** db) :
       db_(db) {
     write_size_ = 0;
   }
   Status Put(const shannon::WriteOptions&, const shannon::Slice& key,
                      const shannon::Slice& value);
   Status Delete(const shannon::WriteOptions&, const shannon::Slice& key);
   Status Write(const shannon::WriteOptions&, VWriteBatch* updates);
   Status Put(const shannon::WriteOptions&, shannon::ColumnFamilyHandle*,
                      const shannon::Slice&, const shannon::Slice&);
   Status Delete(const shannon::WriteOptions&, shannon::ColumnFamilyHandle*,
                         const shannon::Slice&);
   int64_t GetWriteSize();
   void ResetWriteSize();
 private:
   std::atomic<int64_t> write_size_;
   DB** db_;
};

}  // namespace blackwidow
#endif  // VDB_H_
