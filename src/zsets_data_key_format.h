//  Copyright (c) 2017-present The blackwidow Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_ZSETS_DATA_KEY_FORMAT_H_
#define SRC_ZSETS_DATA_KEY_FORMAT_H_

namespace blackwidow {

/*
 * |  <Score>  |      <Member>      |
 *       8 Bytes    member size Bytes
 */
class ZSetsKey {
 public:
  ZSetsKey(double score, const Slice& member) :
    start_(nullptr), score_(score), member_(member) {
  }

  ZSetsKey(const Slice& member, double score) :
    start_(nullptr), score_(score), member_(member) {

    }

  ~ZSetsKey() {
    if (start_ != space_) {
      delete[] start_;
    }
  }

  const Slice EncodeMemberScore() {
    size_t needed = member_.size() + sizeof(uint64_t);
    char* dst = nullptr;
    if (needed <= sizeof(space_)) {
      dst = space_;
    } else {
      dst = new char[needed];

      // Need to allocate space, delete previous space
      if (start_ != space_) {
        delete[] start_;
      }
    }
    start_ = dst;
    // encode member
    memcpy(dst, member_.data(), member_.size());
    dst += member_.size();
    // encode score
    char tmp_score[8];
    EncodeDouble64Sort(tmp_score, score_);
    EncodeFixed64(dst, DecodeFixed64(tmp_score));
    dst += sizeof(uint64_t);
    return Slice(start_, needed);
  }

  const Slice EncodeScoreMember() {
    size_t needed = member_.size() + sizeof(uint64_t);
    char* dst = nullptr;
    if (needed <= sizeof(space_)) {
      dst = space_;
    } else {
      dst = new char[needed];
      // Need to allocate space, delete previous space
      if (start_ != space_) {
        delete[] start_;
      }
    }
    start_ = dst;
    // encode score
    char tmp_score[8];
    EncodeDouble64Sort(tmp_score, score_);
    EncodeFixed64(dst, DecodeFixed64(tmp_score));
    dst += sizeof(uint64_t);
    // encode member
    memcpy(dst, member_.data(), member_.size());
    dst += member_.size();
    return Slice(start_, needed);
  }

 private:
  char space_[200];
  char* start_;
  Slice key_;
  int32_t version_;
  double score_;
  Slice member_;
};


class ParsedZSetsMemberScore {
 public:
  explicit ParsedZSetsMemberScore(const std::string* key) {
    const char* ptr = key->data();
    member_ = Slice(ptr, key->size() - sizeof(uint64_t));
    ptr = ptr + key->size() - sizeof(uint64_t);
    uint64_t tmp = DecodeFixed64(ptr);
    const void* ptr_tmp = reinterpret_cast<const void*>(&tmp);
    DecodeDouble64Sort(reinterpret_cast<const char *>(ptr_tmp), &score_);
  }

  explicit ParsedZSetsMemberScore(const Slice& key) {
    const char* ptr = key.data();
    member_ = Slice(ptr, key.size() - sizeof(uint64_t));
    ptr = ptr + key.size() - sizeof(uint64_t);
    uint64_t tmp = DecodeFixed64(ptr);
    const void* ptr_tmp = reinterpret_cast<const void*>(&tmp);
    DecodeDouble64Sort(reinterpret_cast<const char *>(ptr_tmp), &score_);
  }

  double score() const {
    return score_;
  }
  Slice member() {
    return member_;
  }

 private:
  double score_;
  Slice member_;
};

class ParsedZSetsScoreMember {
 public:
  explicit ParsedZSetsScoreMember(const std::string* key) {
    const char* ptr = key->data();
    uint64_t tmp = DecodeFixed64(ptr);
    const void* ptr_tmp = reinterpret_cast<const void*>(&tmp);
    DecodeDouble64Sort(reinterpret_cast<const char *>(ptr_tmp), &score_);
    ptr += sizeof(uint64_t);
    member_ = Slice(ptr, key->size() - sizeof(uint64_t));
  }

  explicit ParsedZSetsScoreMember(const Slice& key) {
    const char* ptr = key.data();
    uint64_t tmp = DecodeFixed64(ptr);
    const void* ptr_tmp = reinterpret_cast<const void*>(&tmp);
    DecodeDouble64Sort(reinterpret_cast<const char *>(ptr_tmp), &score_);
    ptr += sizeof(uint64_t);
    member_ = Slice(ptr, key.size() - sizeof(uint64_t));
  }

  double score() const {
    return score_;
  }
  Slice member() {
    return member_;
  }

 private:
  double score_;
  Slice member_;
};

} // blackwidow
#endif // SRC_ZSETS_DATA_KEY_FORMAT_H_
