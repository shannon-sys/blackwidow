#include "src/vdb.h"

namespace blackwidow {

Status VDB::Put(const shannon::WriteOptions& write_options, const shannon::Slice& key,
        const Slice& value) {
  Status s = db_->Put(write_options, key, value);
  if (s.ok()) {
    write_size_ += key.size() + value.size();
  }
  return s;
}

Status VDB::Delete(const shannon::WriteOptions& write_options, const shannon::Slice& key) {
  Status s = db_->Delete(write_options, key);
  if (s.ok()) {
    write_size_ += key.size();
  }
  return s;
}

Status VDB::Write(const shannon::WriteOptions& write_options, VWriteBatch* updates) {
  Status s = db_->Write(write_options, updates->write_batch());
  if (s.ok()) {
    write_size_ += updates->size();
  }
  return s;
}

Status VDB::Put(const shannon::WriteOptions& write_options, shannon::ColumnFamilyHandle* handle,
        const shannon::Slice& key, const shannon::Slice& value) {
  Status s = db_->Put(write_options, handle, key, value);
  if (s.ok()) {
    write_size_ += key.size() + value.size();
  }
  return s;
}

Status VDB::Delete(const shannon::WriteOptions& write_options, shannon::ColumnFamilyHandle* handle,
        const shannon::Slice& key) {
  Status s = db_->Delete(write_options, handle, key);
  if (s.ok()) {
    write_size_ += key.size();
  }
  return s;
}

int64_t VDB::GetWriteSize() {
  return write_size_;
}

void VDB::ResetWriteSize() {
  write_size_ = 0;
}

}  // namespace blackwidow
