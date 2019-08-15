// Copyright (c) 2019-present The blackwidow Authors. All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory
#ifndef SRC_SKIP_LIST_H_
#define SRC_SKIP_LIST_H_
#include "blackwidow/blackwidow.h"
#include <random>
namespace blackwidow {
  #define MAX_SKIPLIST_LEVEL 12
  #define MAX_SKIPLIST_BUF 4096
  class SkipList {
    private:
      struct SkipListNode {
        int next[MAX_SKIPLIST_LEVEL];
        int prev;
        int key_len;
        char key[0];
      };
      struct SkipListManager {
        int cur_level;
        int count;
        int cur_buf_index;
        int dummy;
        SkipListNode head[1];
      };
    public:
      friend class Iterator;
      class Iterator{
        private:
          SkipList::SkipListManager* skip_list_manager_;
          SkipList::SkipListNode* node_;
          SkipList* skip_list_;
          int cur_index_;
        public:
          Iterator(SkipList::SkipListManager* skip_list_manager, SkipList* skip_list) {
            this->skip_list_manager_ = skip_list_manager;
            this->skip_list_ = skip_list;
            node_ = NULL;
            cur_index_ = 0;
          }
          void SeekToFirst();
          void SeekToLast();
          void Seek(const shannon::Slice& key);
          void Next();
          void Prev();
          bool Valid();
          const shannon::Slice key() {
            return shannon::Slice(this->node_->key, this->node_->key_len);
          }
      };

      /*
      SkipList() {
        this->buffer_ = new char[MAX_SKIPLIST_BUF];
        this->buffer_len_ = MAX_SKIPLIST_BUF;
        init();
      }*/
      SkipList(std::string* buffer_str, int prefix = 0, bool is_init = false) {
        if (buffer_str != NULL) {
          buffer_str_ = buffer_str;
          prefix_ = prefix;
          if (is_init)
            init();
          skip_list_manager_ = reinterpret_cast<SkipListManager*>(const_cast<char*>(this->buffer_str_->data() + prefix_));
        }
      }
      Iterator* NewIterator() {
        this->skip_list_manager_ = reinterpret_cast<SkipListManager*>(const_cast<char*>(this->buffer_str_->data() + prefix_));
        return new Iterator(this->skip_list_manager_, this);
      }
      bool insert(const shannon::Slice& key);
      bool del(const shannon::Slice& key);
      bool exists(const shannon::Slice& key);
      int count();
      int iterator_count();
      // auxiliary function
      void show();
      void circle();
      void show_struct();
      int offset_to_ranking(int offset);
      int offset_to_pranking(int offset);
    private:
    SkipListNode* find(const shannon::Slice& key, SkipList::SkipListNode* prev[]);
    void init() {
      if (this->buffer_str_ == NULL) {
        return;
      }
      this->buffer_str_->resize(prefix_ + sizeof(SkipListManager));
      skip_list_manager_ = reinterpret_cast<SkipListManager*>(const_cast<char*>(this->buffer_str_->data() + prefix_));
      //
      skip_list_manager_->count = 0;
      skip_list_manager_->cur_level = 0;
      skip_list_manager_->cur_buf_index = sizeof(SkipListManager);
      memset(skip_list_manager_->head[0].next, 0, sizeof(int)*MAX_SKIPLIST_LEVEL);
      skip_list_manager_->head[0].key_len = 0;
      skip_list_manager_->head[0].prev = sizeof(SkipListManager) - sizeof(SkipListNode);
      max_level_ = MAX_SKIPLIST_LEVEL;
      kbranching_ = 4;
      random_engine_.seed(get_timestamp());
    }
    int random_height() {
      int height = 1;
      while (height < max_level_ && random() % kbranching_ == 0) {
          height ++;
      }
      return height;
    }
    int random() {
      return random_engine_();
    }
    int get_timestamp() {
      return time(NULL);
    }
    int kbranching_;
    int max_level_;
    SkipListManager *skip_list_manager_;
    // char *buffer_;
    // unsigned int buffer_len_;
    std::string* buffer_str_;
    int prefix_;
    std::default_random_engine random_engine_;
  };
}
#endif
