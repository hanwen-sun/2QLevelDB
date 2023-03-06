// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_SKIPLIST_H_
#define STORAGE_LEVELDB_DB_SKIPLIST_H_

// Thread safety
// -------------
//
// Writes require external synchronization, most likely a mutex.
// Reads require a guarantee that the SkipList will not be destroyed
// while the read is in progress.  Apart from that, reads progress
// without any internal locking or synchronization.
//
// Invariants:
//
// (1) Allocated nodes are never deleted until the SkipList is
// destroyed.  This is trivially guaranteed by the code since we
// never delete any skip list nodes.
//
// (2) The contents of a Node except for the next/prev pointers are
// immutable after the Node has been linked into the SkipList.
// Only Insert() modifies the list, and it is careful to initialize
// a node and use release-stores to publish the nodes in one or
// more lists.
//
// ... prev vs. next pointer ordering ...

#include <atomic>
#include <cassert>
#include <cstdlib>
#include <cstdio>

#include "util/arena.h"
#include "util/random.h"

namespace leveldb {

template <typename Key, class Comparator>
class SkipList {
 private:
  struct Node;

 public:
  // Create a new SkipList object that will use "cmp" for comparing keys,
  // and will allocate memory using "*arena".  Objects allocated in the arena
  // must remain allocated for the lifetime of the skiplist object.
  explicit SkipList(Comparator cmp, Arena* arena);

  SkipList(const SkipList&) = delete;
  SkipList& operator=(const SkipList&) = delete;

  // Insert key into the list.
  // REQUIRES: nothing that compares equal to key is currently in the list.
  void Insert(const Key& key, size_t Size);

  // Returns true iff an entry that compares equal to key is in the list.
  bool Contains(const Key& key) const;

  class FIFO;

  // Iteration over the contents of a skip list
  class Iterator {
   public:
    // Initialize an iterator over the specified list.
    // The returned iterator is not valid.
    explicit Iterator(const SkipList* list);

    // Returns true iff the iterator is positioned at a valid node.
    bool Valid() const;

    // Returns the key at the current position.
    // REQUIRES: Valid()
    const Key& key() const;

    // Advances to the next position.
    // REQUIRES: Valid()
    void Next();

    // Advances to the previous position.
    // REQUIRES: Valid()
    void Prev();

    // Advance to the first entry with a key >= target
    void Seek(const Key& target);

    // Position at the first entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    void SeekToFirst();

    // Position at the last entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    void SeekToLast();

   private:
    const SkipList* list_;
    Node* node_;
    // Intentionally copyable
  };

 private:
  enum { kMaxHeight = 12 };

  inline int GetMaxHeight() const {
    return max_height_.load(std::memory_order_relaxed); 
  }

  Node* NewNode(const Key& key, int height, size_t Size);
  int RandomHeight();
  bool Equal(const Key& a, const Key& b) const { return (compare_(a, b) == 0); }

  // Return true if key is greater than the data stored in "n"
  bool KeyIsAfterNode(const Key& key, Node* n) const;

  // Return the earliest node that comes at or after key.
  // Return nullptr if there is no such node.
  //
  // If prev is non-null, fills prev[level] with pointer to previous
  // node at "level" for every level in [0..max_height_-1].
  Node* FindGreaterOrEqual(const Key& key, Node** prev) const;

  // Return the latest node with a key < key.
  // Return head_ if there is no such node.
  Node* FindLessThan(const Key& key) const;

  // Return the last node in the list.
  // Return head_ if list is empty.
  Node* FindLast() const;

  // Immutable after construction
  Comparator const compare_;
  Arena* const arena_;  // Arena used for allocations of nodes

  Node* const head_;

  // Modified only by Insert().  Read racily by readers, but stale
  // values are ok.
  std::atomic<int> max_height_;  // Height of the entire list

  FIFO* FIFO_;
  // Read/written only by Insert().
  Random rnd_;
};

// Implementation details follow
template <typename Key, class Comparator>
struct SkipList<Key, Comparator>::Node {
  explicit Node(const Key& k, const size_t s) : key(k), node_size(s) {
    FIFO_next = nullptr;
    FIFO_prev = nullptr;
    // node_size = 0;
  }

  Key const key;

  // Accessors/mutators for links.  Wrapped in methods so we can
  // add the appropriate barriers as necessary.
  Node* Next(int n) {
    assert(n >= 0);
    // Use an 'acquire load' so that we observe a fully initialized
    // version of the returned Node.
    return next_[n].load(std::memory_order_acquire);
  }
  void SetNext(int n, Node* x) {
    assert(n >= 0);
    // Use a 'release store' so that anybody who reads through this
    // pointer observes a fully initialized version of the inserted node.
    next_[n].store(x, std::memory_order_release);
  }

  // No-barrier variants that can be safely used in a few locations.
  Node* NoBarrier_Next(int n) {
    assert(n >= 0);
    return next_[n].load(std::memory_order_relaxed);
  }
  void NoBarrier_SetNext(int n, Node* x) {
    assert(n >= 0);
    next_[n].store(x, std::memory_order_relaxed);
  }

  Node* FIFO_Next() {
    return FIFO_next.load(std::memory_order_acquire);
  }

  void Set_FIFO_Next(Node* x) {
    FIFO_next.store(x, std::memory_order_acquire);
  }

  Node* NoBarrier_FIFO_Next() {
    return FIFO_next.load(std::memory_order_relaxed);
  }

  void NoBarrier_Set_FIFO_Next(Node* x) {
    FIFO_next.store(x, std::memory_order_relaxed);
  }

  Node* FIFO_Prev() {
    return FIFO_prev.load(std::memory_order_acquire);
  }

  void Set_FIFO_Prev(Node* x) {
    FIFO_prev.store(x, std::memory_order_acquire);
  }

  Node* NoBarrier_FIFO_Prev() {
    return FIFO_prev.load(std::memory_order_relaxed);
  }

  void NoBarrier_Set_FIFO_Prev(Node* x) {
    FIFO_prev.store(x, std::memory_order_relaxed);
  }

  void Set_Node_Size(size_t x) {
    node_size.store(x, std::memory_order_acquire);
  }

  size_t Show_Node_Size() {
    return node_size.load(std::memory_order_acquire);
  }

 private:
  // Array of length equal to the node height.  next_[0] is lowest level link.
  std::atomic<size_t> node_size;    // 这里要注意内存对齐;
  std::atomic<Node*> FIFO_next;  // 考虑到node的构建方式, next_[1]应该放在最后;
  std::atomic<Node*> FIFO_prev;  // 由于每个node的层数是不确定的, 所以这里初始化为next_[1], 在构造函数中实际指定层数;
  std::atomic<Node*> next_[1];
};

template <typename Key, class Comparator>
typename SkipList<Key, Comparator>::Node* SkipList<Key, Comparator>::NewNode(
    const Key& key, int height, size_t Size) {
  size_t Node_memory =  sizeof(Node) + sizeof(std::atomic<Node*>) * (height - 1);
  char* const node_memory = arena_->AllocateAligned(
      sizeof(Node) + sizeof(std::atomic<Node*>) * (height - 1));
  // node_size.store(Node_memory + Size, std::memory_order_acquire);
  Size += Node_memory;
  // fprintf(stderr, "height: %zu, Key Size: %u, Node Size: %u\n", height, Size, Node_memory);

  return new (node_memory) Node(key, Size);   // 在获得了一块可以容纳指定对象的内存后, 在该内存空间上直接构造; placement new;
}

// FIFO部分函数功能:
template <typename Key, class Comparator>
class SkipList<Key, Comparator>::FIFO {
 public:
   explicit FIFO();

   void Insert(Node* x);

   size_t Hot_MemoryUsage() const;

   size_t Cold_MemoryUsage() const;

   void FreezeNode(Node* x);

   // void FreezeNode(int r);

   class FIFO_Iterator {
      public:
         // Initialize an iterator over the specified list.
    // The returned iterator is not valid.
    explicit FIFO_Iterator(const SkipList* list);

    // Returns true iff the iterator is positioned at a valid node.
    bool Valid() const;

    // Returns the key at the current position.
    // REQUIRES: Valid()
    const Key& key() const;

    // Advances to the next position.
    // REQUIRES: Valid()
    void Next();

    // Advances to the previous position.
    // REQUIRES: Valid()
    void Prev();

    // Advance to the first entry with a key >= target
    void Seek(const Key& target);

    // Position at the first entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    void SeekToFirst();

    // Position at the last entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    void SeekToLast();
      
      private:
         // const FIFO* list_;
         const SkipList* list_;
         Node* node_;
   };

   private:
      // SkipList* list_;
      Node* normal_head_;           //  实际上是normal_head;  the oldest hot node;
      Node* cold_head_;      // 初始化为nullptr;  the oldest cold node, also the oldest of the whole node;
      Node* cur_node_;    // the newest node;

      std::atomic<size_t> hot_mem;
      std::atomic<size_t> cold_mem;
      // size_t threshold_;
};

template <typename Key, class Comparator>
SkipList<Key, Comparator>::FIFO::FIFO() : hot_mem(0), cold_mem(0) {
  normal_head_ = nullptr;
  cold_head_ = nullptr;
  cur_node_ = nullptr;
}

template <typename Key, class Comparator>
void SkipList<Key, Comparator>::FIFO::Insert(SkipList::Node* x) {
  //fprintf(stderr, "%s\n", "FIFO Insert Begin");
  // FreezeNode();
  if(normal_head_ == nullptr) {
      normal_head_ = x;
      cur_node_ = x;
      return;
   }

  x->NoBarrier_Set_FIFO_Prev(cur_node_);  // 完成双向链表构建;
  cur_node_->Set_FIFO_Next(x);

  cur_node_ = cur_node_->FIFO_Next();
  //fprintf(stderr, "%zu\n", normal_head_->key);
  //fprintf(stderr, "%s\n", "FIFO Insert End!");
}

template <typename Key, class Comparator>
size_t SkipList<Key, Comparator>::FIFO::Hot_MemoryUsage() const {
  return hot_mem.load(std::memory_order_acquire);
}

template <typename Key, class Comparator>
size_t SkipList<Key, Comparator>::FIFO::Cold_MemoryUsage() const {
  return cold_mem.load(std::memory_order_acquire);
}

template <typename Key, class Comparator>
void SkipList<Key, Comparator>::FIFO::FreezeNode(Node* x) {
  /*size_t Node_Size_ = x->Show_Node_Size();
  size_t Move_Size_ = Hot_MemoryUsage() + Node_Size_ - (); 
  
  if(cold_head_ == nullptr) {
    cold_head_ = normal_head_;
  }

  size_t sum = 0;
  while(sum < Move_Size_) {

  } */
  
  
}


template <typename Key, class Comparator>
inline SkipList<Key, Comparator>::FIFO::FIFO_Iterator::FIFO_Iterator(const SkipList* list) {
  list_ = list;
  node_ = nullptr;
}

template <typename Key, class Comparator>
inline bool SkipList<Key, Comparator>::FIFO::FIFO_Iterator::Valid() const {
  return node_ != nullptr;
}

template <typename Key, class Comparator>
inline const Key& SkipList<Key, Comparator>::FIFO::FIFO_Iterator::key() const {
  assert(Valid());
  return node_->key;
}

template <typename Key, class Comparator>
inline void SkipList<Key, Comparator>::FIFO::FIFO_Iterator::Next() {
  assert(Valid());
  node_ = node_->FIFO_Next();
}

template <typename Key, class Comparator>
inline void SkipList<Key, Comparator>::FIFO::FIFO_Iterator::Prev() {
  assert(Valid());
  // to_do: ;
  node_ = node_->FIFO_Prev();
}

template <typename Key, class Comparator>
inline void SkipList<Key, Comparator>::FIFO::FIFO_Iterator::Seek(const Key& target) {
  node_ = list_->FindGreaterOrEqual(target, nullptr);
}

template <typename Key, class Comparator>
inline void SkipList<Key, Comparator>::FIFO::FIFO_Iterator::SeekToFirst() {
  if(list_->FIFO_->cold_head_ == nullptr)
      node_ = list_->FIFO_->normal_head_;
  else
      node_ = list_->FIFO_->cold_head_;
}

template <typename Key, class Comparator>
inline void SkipList<Key, Comparator>::FIFO::FIFO_Iterator::SeekToLast() {
  node_ = list_->FIFO_->cur_node_;
}




template <typename Key, class Comparator>
inline SkipList<Key, Comparator>::Iterator::Iterator(const SkipList* list) {
  list_ = list;
  node_ = nullptr;
}

template <typename Key, class Comparator>
inline bool SkipList<Key, Comparator>::Iterator::Valid() const {
  return node_ != nullptr;
}

template <typename Key, class Comparator>
inline const Key& SkipList<Key, Comparator>::Iterator::key() const {
  assert(Valid());
  return node_->key;
}

template <typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::Next() {
  assert(Valid());
  node_ = node_->Next(0);
}

template <typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::Prev() {
  // Instead of using explicit "prev" links, we just search for the
  // last node that falls before key.
  assert(Valid());
  node_ = list_->FindLessThan(node_->key);
  if (node_ == list_->head_) {
    node_ = nullptr;
  }
}

template <typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::Seek(const Key& target) {
  node_ = list_->FindGreaterOrEqual(target, nullptr);
}

template <typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::SeekToFirst() {
  node_ = list_->head_->Next(0);
}

template <typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::SeekToLast() {
  node_ = list_->FindLast();
  if (node_ == list_->head_) {
    node_ = nullptr;
  }
}

template <typename Key, class Comparator>
int SkipList<Key, Comparator>::RandomHeight() {
  // Increase height with probability 1 in kBranching
  static const unsigned int kBranching = 4;
  int height = 1;
  while (height < kMaxHeight && rnd_.OneIn(kBranching)) {
    height++;
  }
  assert(height > 0);
  assert(height <= kMaxHeight);
  return height;
}

template <typename Key, class Comparator>
bool SkipList<Key, Comparator>::KeyIsAfterNode(const Key& key, Node* n) const {
  // null n is considered infinite
  return (n != nullptr) && (compare_(n->key, key) < 0);
}

template <typename Key, class Comparator>
typename SkipList<Key, Comparator>::Node*
SkipList<Key, Comparator>::FindGreaterOrEqual(const Key& key,
                                              Node** prev) const {
  //fprintf(stderr, "%d\n", 1);
  Node* x = head_;
  int level = GetMaxHeight() - 1;
  while (true) {
    // //fprintf(stderr, "%d\n", x->key());
    Node* next = x->Next(level);
    if (KeyIsAfterNode(key, next)) {
      // Keep searching in this list
      x = next;
      // //fprintf(stderr, "%d\n", 4);
    } else {
      if (prev != nullptr) prev[level] = x;
      if (level == 0) {
        return next;
      } else {
        // Switch to next list
        level--;
      }
      // //fprintf(stderr, "%d\n", 5);
    }
  }
}

template <typename Key, class Comparator>
typename SkipList<Key, Comparator>::Node*
SkipList<Key, Comparator>::FindLessThan(const Key& key) const {
  Node* x = head_;
  int level = GetMaxHeight() - 1;
  while (true) {
    assert(x == head_ || compare_(x->key, key) < 0);
    Node* next = x->Next(level);
    if (next == nullptr || compare_(next->key, key) >= 0) {
      if (level == 0) {
        return x;
      } else {
        // Switch to next list
        level--;
      }
    } else {
      x = next;
    }
  }
}

template <typename Key, class Comparator>
typename SkipList<Key, Comparator>::Node* SkipList<Key, Comparator>::FindLast()
    const {
  Node* x = head_;
  int level = GetMaxHeight() - 1;
  while (true) {
    Node* next = x->Next(level);
    if (next == nullptr) {
      if (level == 0) {
        return x;
      } else {
        // Switch to next list
        level--;
      }
    } else {
      x = next;
    }
  }
}

template <typename Key, class Comparator>
SkipList<Key, Comparator>::SkipList(Comparator cmp, Arena* arena)
    : compare_(cmp),
      arena_(arena),
      head_(NewNode(0 /* any key will do */, kMaxHeight, 0)),
      max_height_(1),
      rnd_(0xdeadbeef) {
  FIFO_ = new FIFO();
  // uint16_t s = 0;
  // head_ = NewNode(0, kMaxHeight, s);
  for (int i = 0; i < kMaxHeight; i++) {
    head_->SetNext(i, nullptr);
  }
}

template <typename Key, class Comparator>
void SkipList<Key, Comparator>::Insert(const Key& key, size_t Size) {
  // TODO(opt): We can use a barrier-free variant of FindGreaterOrEqual()
  // here since Insert() is externally synchronized.
  // fprintf(stderr, "%s\n", "SkipList Insert Begin!");
  Node* prev[kMaxHeight];
  Node* x = FindGreaterOrEqual(key, prev);   // 找到对于x的每个level的要插入的前一个结点;
  //fprintf(stderr, "%d\n", 2);
  // Our data structure does not allow duplicate insertion
  assert(x == nullptr || !Equal(key, x->key));

  int height = RandomHeight();
  if (height > GetMaxHeight()) {
    for (int i = GetMaxHeight(); i < height; i++) {
      prev[i] = head_;
    }
    // It is ok to mutate max_height_ without any synchronization
    // with concurrent readers.  A concurrent reader that observes
    // the new value of max_height_ will see either the old value of
    // new level pointers from head_ (nullptr), or a new value set in
    // the loop below.  In the former case the reader will
    // immediately drop to the next level since nullptr sorts after all
    // keys.  In the latter case the reader will use the new node.
    max_height_.store(height, std::memory_order_relaxed);
  }

  x = NewNode(key, height, Size);
  
  // fprintf(stderr, "node size: %zu\n", x->Show_Node_Size());
  //fprintf(stderr, "Node Size: %zu\n", sizeof(*x));

  for (int i = 0; i < height; i++) {
    // NoBarrier_SetNext() suffices since we will add a barrier when
    // we publish a pointer to "x" in prev[i].
    x->NoBarrier_SetNext(i, prev[i]->NoBarrier_Next(i));
    prev[i]->SetNext(i, x);
  }

  FIFO_->Insert(x);
  //fprintf(stderr, "%s\n", "SkipList Insert Done!");
}

template <typename Key, class Comparator>
bool SkipList<Key, Comparator>::Contains(const Key& key) const {
  Node* x = FindGreaterOrEqual(key, nullptr);
  if (x != nullptr && Equal(key, x->key)) {
    return true;
  } else {
    return false;
  }
}

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_SKIPLIST_H_
