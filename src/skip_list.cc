//  Copyright (c) 2019-present The blackwidow Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
#include "src/skip_list.h"
#include <cstring>

namespace blackwidow {
  void SkipList::show() {
    char *buffer = const_cast<char*>(this->buffer_str_->data() + prefix_);
    SkipListNode* head = this->skip_list_manager_->head;
    std::cout<<"key list:";
    while (head->next[0] > 0) {
      head = reinterpret_cast<SkipListNode*>(buffer + head->next[0]);
      shannon::Slice key(head->key, head->key_len);
      std::cout<<key.ToString()<<" ";
    }
    std::cout<<std::endl;
  }

  int SkipList::offset_to_ranking(int offset) {
    int ranking = 1;
    char *buffer = const_cast<char*>(this->buffer_str_->data() + prefix_);
    int cur_index = this->skip_list_manager_->head[0].next[0];
    while (cur_index != 0 && cur_index != offset) {
      SkipListNode *cur_node = reinterpret_cast<SkipListNode*>(const_cast<char*>(buffer + cur_index));
      cur_index = cur_node->next[0];
      ranking ++;
    }
    if (offset == 0)
      return 0;
    if (cur_index == offset)
      return ranking;
    return  0;
  }

  int SkipList::offset_to_pranking(int offset) {
    int ranking = 1;
    char *buffer = const_cast<char*>(this->buffer_str_->data() + prefix_);
    int cur_index = sizeof(SkipListManager);//((unsigned long)((char*)&this->skip_list_manager_->head[0]) - (int)0) + sizeof(SkipListNode);
    while (cur_index < skip_list_manager_->cur_buf_index) {
      if (cur_index == offset) {
        return ranking;
      }
      SkipListNode* node = reinterpret_cast<SkipListNode*>(buffer + cur_index);
      cur_index += sizeof(SkipListNode) + node->key_len;
      ranking ++;
    }
    return 0;
  }

  void SkipList::show_struct() {
    char *buffer = const_cast<char*>(this->buffer_str_->data() + prefix_);
    skip_list_manager_ = reinterpret_cast<SkipListManager*>(buffer);
    SkipListNode* head = this->skip_list_manager_->head;
    // print title
    printf("%-10d ", 0);
    int index = 1;
    int cur_index = this->skip_list_manager_->head->next[0];
    while (cur_index != 0) {
      printf("%-10d ", index);
      SkipListNode* node = reinterpret_cast<SkipListNode*>(buffer + cur_index);
      cur_index = node->next[0];
      index ++;
    }
    printf("\n");

    for (int i = MAX_SKIPLIST_LEVEL - 1; i >= 0; i --) {
      SkipListNode* head = this->skip_list_manager_->head;
      int cur_index = head->next[i];
      int next_index = head->next[0];
      printf ("%-10d ", this->offset_to_ranking(cur_index));
      while (next_index != 0) {
        SkipListNode* tnode = reinterpret_cast<SkipListNode*>(const_cast<char*>(buffer + next_index));
        printf("%-10d ", this->offset_to_ranking(tnode->next[i]));
        next_index = tnode->next[0];
      }
      printf("\n");
    }
    printf("%-10s ", "head");
    cur_index = head->next[0];
    while (cur_index != 0) {
      SkipListNode* cur_node = reinterpret_cast<SkipListNode*>(const_cast<char*>(buffer + cur_index));
      printf("%-10s ", shannon::Slice(cur_node->key, cur_node->key_len).ToString().data());
      cur_index = cur_node->next[0];
    }
    printf("\n");
    printf("%-10d ", 0);
    cur_index = head->next[0];
    while (cur_index != 0) {
      SkipListNode* cur_node = reinterpret_cast<SkipListNode*>(const_cast<char*>(buffer + cur_index));
      printf("%-10d ", this->offset_to_pranking(cur_index));
      cur_index = cur_node->next[0];
    }
    printf("\n");
  }

  void SkipList::circle() {
    char *buffer = const_cast<char*>(buffer_str_->data() + prefix_);
    skip_list_manager_ = reinterpret_cast<SkipListManager*>(buffer);
    if (skip_list_manager_->count > 0) {
      std::cout<<"count:"<<skip_list_manager_->count<<std::endl;
      std::cout<<"first node next list:"<<std::endl;
      // SkipListNode* node = reinterpret_cast<SkipListNode*>(buffer + skip_list_manager_->head[0].next[0]);
      SkipListNode* node = skip_list_manager_->head;
      for (int i = 0; i < MAX_SKIPLIST_LEVEL; i ++) {
        std::cout<<"next["<<i<<"]="<<node->next[i]<<std::endl;
      }
    }
  }

  SkipList::SkipListNode* SkipList::find(const shannon::Slice& key, SkipList::SkipListNode* prev[]) {
    assert(buffer_str_);
    assert(buffer_str_->size() >= sizeof(SkipListManager));
    char *buffer = const_cast<char*>(buffer_str_->data() + prefix_);
    skip_list_manager_ = reinterpret_cast<SkipListManager*>(buffer);
    if (skip_list_manager_->count == 0) {
      return NULL;
    }
    int level = this->skip_list_manager_->cur_level;
    SkipListNode* p = reinterpret_cast<SkipListNode*>(skip_list_manager_->head);
    for (int i = level - 1; i >= 0; i --) {
      // to right search
      while (p->next[i] != 0) {
        SkipListNode* node = reinterpret_cast<SkipListNode*>(buffer + p->next[i]);
        // std::cout<<"key:"<<shannon::Slice(node->key, node->key_len).ToString()<<std::endl;
        shannon::Slice node_key(node->key, node->key_len);
        int flag = key.compare(node_key);
        if (flag > 0) {
          p = node;
        } else if (flag < 0) {
          // to next level
          break;
        } else {
          // find the node
          for (; i >= 0; i --) {
            prev[i] = node;
          }
          return node;
        }
      }
      prev[i] = p;
    }
    return NULL;
  }

  bool SkipList::insert(const shannon::Slice& key) {
    SkipListNode *prev[MAX_SKIPLIST_LEVEL];
    this->buffer_str_->append(sizeof(SkipListNode) + key.size(), ' ');
    SkipListNode* node = this->find(key, prev);
    if (node != NULL) {
      this->buffer_str_->resize(this->buffer_str_->size() - sizeof(SkipListNode) - key.size());
      return false;
    }
    // append new node size's buffer
    // this->buffer_str_->append(sizeof(SkipListNode) + key.size(), ' ');
    char* buffer = const_cast<char*>(this->buffer_str_->data() + prefix_);
    skip_list_manager_ = reinterpret_cast<SkipListManager*>(buffer);
    SkipListNode* new_node = reinterpret_cast<SkipListNode*>(buffer + skip_list_manager_->cur_buf_index);
    new_node->key_len = key.size();
    memcpy(new_node->key, key.data(), key.size());
    memset(new_node->next, 0, sizeof(int) * MAX_SKIPLIST_LEVEL);
    new_node->prev = 0;
    int cur_level_height = this->random_height();
    for (int i = skip_list_manager_->cur_level; i < cur_level_height; i ++) {
      prev[i] = skip_list_manager_->head;
    }
    // if skiplist is empty
    if (skip_list_manager_->cur_level == 0) {
      for (int i = 0; i < cur_level_height; i ++) {
        new_node->next[i] = skip_list_manager_->head[0].next[i];
        skip_list_manager_->head[0].next[i] = skip_list_manager_->cur_buf_index;
      }
      new_node->prev = (char*)skip_list_manager_->head - buffer;
    } else {
      for (int i = 0; i < cur_level_height; i ++) {
        new_node->next[i] = prev[i]->next[i];
        prev[i]->next[i] = skip_list_manager_->cur_buf_index;
      }

      new_node->prev = ((char*)prev[0]) - buffer;
      // modify next node's prev pointer = cur node(if exists)
      if (new_node->next[0] != 0) {
        SkipListNode* next_node = reinterpret_cast<SkipListNode*>(buffer + new_node->next[0]);
        next_node->prev = ((char*)new_node) - buffer;
      }
    }
    // if new node is end node, save new node offset
    if (new_node->next[0] == 0)
    {
      // skip_list_manager_->end_node_position = (char*)new_node - buffer;
      skip_list_manager_->head[0].prev = (char*)new_node - buffer;
    }
    // move cur_index
    skip_list_manager_->cur_buf_index += sizeof(SkipListNode) + key.size();
    skip_list_manager_->cur_level = (cur_level_height > skip_list_manager_->cur_level) ? (cur_level_height) : (skip_list_manager_->cur_level);
    skip_list_manager_->count ++;
    return true;
  }
  bool SkipList::del(const shannon::Slice& key) {
    SkipListNode* prev[MAX_SKIPLIST_LEVEL];
    SkipListNode* node = this->find(key, prev);
    if (node == NULL) {
      return false;
    }
    char *buffer = const_cast<char*>(this->buffer_str_->data() + prefix_);
    // change next node's prev pointer
    if (node->next[0] != 0) {
      SkipListNode* next_node = reinterpret_cast<SkipListNode*>(buffer + node->next[0]);
      assert(next_node->prev == ((char*)node - buffer));
      next_node->prev = node->prev;
    }
    // chagnge head prev pointer
    if (skip_list_manager_->head[0].prev == (char*)node - buffer) {
      skip_list_manager_->head[0].prev = node->prev;
    }
    // change all node's next pointer and prev pointer in after cur node
    int cur_node_offset = ((char*)node) - buffer;
    SkipListNode* head_node;
    int head_index = (char*)skip_list_manager_->head - buffer;
    while (head_index != 0) {
      head_node = reinterpret_cast<SkipListNode*>(buffer + head_index);
      int next_index = head_node->next[0];
      if (head_index == cur_node_offset) {
        // skip cur node
        head_index = next_index;
        continue;
      }
      if (head_node->prev > cur_node_offset) {
        head_node->prev = head_node->prev - node->key_len - sizeof(SkipListNode);
      }

      for (int i = 0; i < MAX_SKIPLIST_LEVEL; i ++) {
        // if current->next = delete_node
        // then current->next = delete_node->next
        if (head_node->next[i] == cur_node_offset) {
          head_node->next[i] = node->next[i];
        }
        // if current->next > cur_node_offset
        // then change next offset substract current node size
        if (head_node->next[i] > cur_node_offset) {
          head_node->next[i] = head_node->next[i] - node->key_len - sizeof(SkipListNode);
        }
        // reset max level
      }
      head_index = next_index;
    }
    // if end_node_position > cur_node_offset
    // then end_node_position -= cur node size
    /*if (skip_list_manager_->count == 1) {
      skip_list_manager_->end_node_position = 0;
    } else {
      if (skip_list_manager_->end_node_position > cur_node_offset) {
        skip_list_manager_->end_node_position = skip_list_manager_->end_node_position -(node->key_len + sizeof(SkipListNode));
      } else (skip_list_manager_->end_node_position == cur_node_offset) {

      }
    }*/
    int node_key_len = node->key_len;
    std::memmove((char*)node, (char*)node + node->key_len + sizeof(SkipListNode),
    skip_list_manager_->cur_buf_index - ((char*)node - buffer + node->key_len + sizeof(SkipListNode)));
    // update member
    // skip_list_manager_->cur_buf_index = skip_list_manager_->cur_buf_index - (node->key_len + sizeof(SkipListNode));
    skip_list_manager_->count --;
    // recalc skiplist height
    for (int i = MAX_SKIPLIST_LEVEL - 1; i >= 0; i --) {
      if (skip_list_manager_->head[0].next[i] != 0) {
        skip_list_manager_->cur_level = i + 1;
        break;
      }
    }
    if (skip_list_manager_->count == 0) {
      skip_list_manager_->cur_level = 0;
    }
    this->buffer_str_->resize(this->buffer_str_->size() - (node_key_len + sizeof(SkipListNode)));
    skip_list_manager_->cur_buf_index = skip_list_manager_->cur_buf_index - (node_key_len + sizeof(SkipListNode));
    assert(skip_list_manager_->cur_buf_index == this->buffer_str_->size());
    return true;
  }

  bool SkipList::exists(const Slice& key) {
    SkipListNode *prev[MAX_SKIPLIST_LEVEL];
    SkipListNode *node = this->find(key, prev);
    if (node == NULL) {
      return false;
    }
    return true;
  }

  int SkipList::count() {
    return skip_list_manager_->count;
  }

  int SkipList::iterator_count() {
    int count = 0;
    Iterator *iter = this->NewIterator();
    for (iter->SeekToFirst();
         iter->Valid();
         iter->Next()) {
      count ++;
    }
    return count;
  }
  void SkipList::Iterator::SeekToFirst() {
    if (skip_list_manager_->head[0].next[0] == 0) {
      node_ = NULL;
      return;
    }
    node_ = reinterpret_cast<SkipList::SkipListNode*>((char*)(skip_list_manager_) + skip_list_manager_->head[0].next[0]);
  }

  void SkipList::Iterator::SeekToLast() {
    if (skip_list_manager_->head[0].prev == ((char*)skip_list_manager_->head - (char*)skip_list_manager_)) {
      node_ = NULL;
      return;
    }
    node_ = reinterpret_cast<SkipList::SkipListNode*>(((char*)skip_list_manager_) + skip_list_manager_->head[0].prev);
  }

  void SkipList::Iterator::Seek(const shannon::Slice& key) {
    SkipListNode* prev[MAX_SKIPLIST_LEVEL];
    node_ = skip_list_->find(key, prev);
    if (node_ == NULL) {
      char *buffer = const_cast<char*>(skip_list_->buffer_str_->data() + skip_list_->prefix_);
      skip_list_manager_ = reinterpret_cast<SkipListManager*>(buffer);
      if (skip_list_manager_->count > 0) {
        SkipListNode* cur_node = prev[0];
        if (cur_node == NULL || cur_node->next[0] == 0)
          return;
        cur_node = reinterpret_cast<SkipListNode*>(const_cast<char*>(buffer + cur_node->next[0]));
        if (shannon::Slice(cur_node->key, cur_node->key_len).starts_with(key)) {
          node_ = cur_node;
        }
      }
    }
  }

  void SkipList::Iterator::Next() {
    if (node_ == NULL ||  node_->next[0] == 0) {
      node_ = NULL;
      return;
    }
    SkipList::SkipListNode* next_node = reinterpret_cast<SkipList::SkipListNode*>((char*)(skip_list_manager_) + node_->next[0]);
    node_ = next_node;
  }
  void SkipList::Iterator::Prev() {
    if (node_ == NULL || node_->prev == 0) {
      node_ = NULL;
      return;
    }
    SkipList::SkipListNode* prev_node = reinterpret_cast<SkipList::SkipListNode*>((char*)(skip_list_manager_) + node_->prev);
    node_ = prev_node;
    // arrive head
    if (node_ == skip_list_manager_->head)
      node_ = NULL;
  }

  bool SkipList::Iterator::Valid() {
    return node_ != NULL;
  }
}

