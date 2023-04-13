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
#include "leveldb/slice.h"
#include "util/coding.h"


namespace leveldb { 

static Slice GetLengthPrefixedSlice_SkipList(const char* data) {
  uint32_t len;
  const char* p = data;
  p = GetVarint32Ptr(p, p + 5, &len);  // +5: we assume "p" is not corrupted
  return Slice(p, len);
}

inline Slice ExtractUserKey_SkipList(const Slice& internal_key) {
  assert(internal_key.size() >= 8);
  return Slice(internal_key.data(), internal_key.size() - 8);
}

inline int CompareSequence_SkipList(const char* aptr,
                              const char* bptr) {
  Slice akey = GetLengthPrefixedSlice_SkipList(aptr);
  Slice bkey = GetLengthPrefixedSlice_SkipList(bptr);

  int r = 0;
  const uint64_t anum = DecodeFixed64(akey.data() + akey.size() - 8);
  const uint64_t bnum = DecodeFixed64(bkey.data() + bkey.size() - 8);
  // ////// //fprintf(stderr, "anum: %zu bnum: %zu\n", anum, bnum);
  if(anum > bnum) {
    r = -1;
  } else if (anum < bnum) {
    r = +1;
  } 
  return r;
} 

template <typename Key, class Comparator>
class SkipList {
 private:
  struct Node;

 public:
  // Create a new SkipList object that will use "cmp" for comparing keys,
  // and will allocate memory using "*arena".  Objects allocated in the arena
  // must remain allocated for the lifetime of the skiplist object.
  explicit SkipList(Comparator cmp, Arena* arena, size_t threshold);

  SkipList(const SkipList&) = delete;
  SkipList& operator=(const SkipList&) = delete;

  // Insert key into the list.
  // REQUIRES: nothing that compares equal to key is currently in the list.
  void Insert(const Key& key, size_t Size);

  // Returns true iff an entry that compares equal to key is in the list.
  bool Contains(const Key& key) const;

  size_t Hot_MemoryUsage_();

  size_t Cold_MemoryUsage_();

  void ThrawNode(const Key& key, int r);

  void Seperate(const Key& key);

  void SetHead(const Key& key);

  void FindNextKey(Node** x);

  void Test();

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

    // Seek to next key different from the current node;
    bool SeekToNextKey();

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
    //////// //fprintf(stderr, "%d\n", 1);
    FIFO_next.store(x, std::memory_order_acquire);
    // FIFO_next = x;
    //////// //fprintf(stderr, "%d\n", 2);
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
    // FIFO_prev = x;
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
  ////// //fprintf(stderr, "key: %s, Size: %zu\n", key, Size);

  return new (node_memory) Node(key, Size);   // 在获得了一块可以容纳指定对象的内存后, 在该内存空间上直接构造; placement new;
}

// FIFO部分函数功能:
template <typename Key, class Comparator>
class SkipList<Key, Comparator>::FIFO {
 public:
   explicit FIFO(size_t threshold);

   void Insert(Node* x);

   size_t Hot_MemoryUsage() const;

   size_t Cold_MemoryUsage() const;
   
   size_t Get_Threshold() const;

   void Set_Hot_Memory(size_t hot_mem_);

   void Set_Cold_Memory(size_t cold_mem_);

   void FreezeNode(Node* x);

   void DeleteNode(Node* x, int r);
   // void FreezeNode(int r);
   void ObsoleteNode(Node* x);

   void Move_head();

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

    void SeekToNormal();
      
      private:
         // const FIFO* list_;
         const SkipList* list_;
         Node* node_;
   };

   private:
      // SkipList* list_;
      Node* head_;             // 还是需要一个head_;
      Node* normal_head_;           //  实际上是normal_head;  the oldest hot node;
      Node* cold_head_;      // 初始化为nullptr;  the oldest cold node, also the oldest of the whole node;
      Node* cur_node_;    // the newest node;
      Node* obsolete_;    // 废弃区数据;

      std::atomic<size_t> hot_mem;
      std::atomic<size_t> cold_mem;
      size_t threshold_;
};

template <typename Key, class Comparator>
SkipList<Key, Comparator>::FIFO::FIFO(size_t threshold) : threshold_(threshold), hot_mem(0), cold_mem(0) {
  head_ = nullptr;
  normal_head_ = nullptr;
  cold_head_ = nullptr;
  cur_node_ = nullptr;
  obsolete_ = nullptr;
}

template <typename Key, class Comparator>
void SkipList<Key, Comparator>::FIFO::FreezeNode(Node* x) {
  size_t Node_Size = x->Show_Node_Size();
  /*if(Node_Size > 100)
    //// //fprintf(stderr, "key: %s freezenode: %zu Node: %zu threshold: %zu\n", x->key, Hot_MemoryUsage() + Node_Size,
                             Node_Size, threshold_);*/
  if(Hot_MemoryUsage() + Node_Size < threshold_)
    return;

  size_t Move_Size = Hot_MemoryUsage() + Node_Size - threshold_;  // 超出threshold， 移入冷数据;
                                                      // 先判断要不要FreezeNode!!!
  //// //fprintf(stderr, "move_size: %zu\n", Move_Size);
  if(normal_head_ == nullptr) {   // 上一个key-value太大, 热数据全变为冷数据且未成功插入为热数据;
    return;
  }
  
  if(cold_head_ == nullptr) {
    cold_head_ = head_;
  } 

  Node* tmp = normal_head_;
  size_t sum = 0;
  while(sum < Move_Size) {
    if(tmp == nullptr)             // 新插入的key-value对过大, 则当前的normal_head移动到cur_node;
      break;
    sum += tmp->Show_Node_Size();
    //////// //fprintf(stderr, "tmp size: %zu\n", tmp->Show_Node_Size());
    tmp = tmp->FIFO_Next();
  } 
  if(tmp == nullptr) 
    normal_head_ = nullptr;   // 如果没有hot_data, normal_head就是nullptr!!!
  else
    normal_head_ = tmp;

  hot_mem.store(Hot_MemoryUsage() - sum, std::memory_order_acquire);
  cold_mem.store(Cold_MemoryUsage() + sum, std::memory_order_acquire); 

  // return normal_head_ == nullptr;
  ////// //fprintf(stderr, "%zu\n", Hot_MemoryUsage());
  // ////// //fprintf(stderr, "%s\n", "end freezenode!");
}

// 把x插入Obsolete队列中;  修改X的prev和next;
// node的FIFO_Prev和FIFO_Next继续充当废弃结点的链表;
// 直接头插法就可以;
template <typename Key, class Comparator>
void SkipList<Key, Comparator>::FIFO::ObsoleteNode(Node* x) {
  // ////// //fprintf(stderr, "%s\n", "begin obsolete!");
  if(obsolete_ == nullptr) {
    obsolete_ = x;
    x->Set_FIFO_Next(nullptr);
    x->Set_FIFO_Prev(nullptr);

    return;
  }

  // 单链表;
  x->NoBarrier_Set_FIFO_Next(obsolete_->FIFO_Next());
  obsolete_->Set_FIFO_Next(x);
  // ////// //fprintf(stderr, "%s\n", "end obsolete!");
}

template <typename Key, class Comparator>
void SkipList<Key, Comparator>::FIFO::DeleteNode(Node* x, int r) {
  // 注: 这里的x可能是head node!
  // ////// //fprintf(stderr, "%s\n", "begin delete node!");
  Node* Prev_ = x->FIFO_Prev();
  Node* Next_ = x->FIFO_Next();
  // 在这里先进行冷热数据容量的变换(normal_head_移动前)  注意这里可能有并发读的问题?
  if(r > 0) {  // hot memory;
    // ////// //fprintf(stderr, "%s\n", "shrink cold memory!");
    cold_mem.store(Cold_MemoryUsage() - x->Show_Node_Size(), std::memory_order_acquire);
    //////// //fprintf(stderr, "shrink hot memory!\n")
  }
  else {
    // ////// //fprintf(stderr, "%s\n", "shrink hot memory!");
    hot_mem.store(Hot_MemoryUsage() - x->Show_Node_Size(), std::memory_order_acquire);
  }

  // 先特判x是否head_node并处理：
  if(x == head_) { 
    // ////// //fprintf(stderr, "%s\n", "head node!");
    if(x == cold_head_) {
      // ////// //fprintf(stderr, "%s\n", "cold head!");
      
      cold_head_ = cold_head_->FIFO_Next();
      cold_head_->Set_FIFO_Prev(nullptr);
      // test->Set_FIFO_Prev(nullptr);  一加这句就segmentation fault, 不知道为什么;
      // ////// //fprintf(stderr, "%d\n", 1);
    }
    
    else if(x == normal_head_) {
      // ////// //fprintf(stderr, "%s\n", "normal head!");
      normal_head_ = normal_head_->FIFO_Next();
      normal_head_->Set_FIFO_Prev(nullptr);
    }

    head_ = head_->FIFO_Next();

    ObsoleteNode(x);
    return;
  }

  if(x == normal_head_) {   // 不是head_node但是是normal_head_;
    normal_head_ = normal_head_->FIFO_Next();   // 直接向后移动;
    Prev_->Set_FIFO_Next(Next_);
    Next_->Set_FIFO_Prev(Prev_);

    ObsoleteNode(x);
    return;
  }

  // 被删除的结点不可能是最后一个;
  // ////// //fprintf(stderr, "%s\n", "no head node!");
  Prev_->Set_FIFO_Next(Next_);
  // ////// //fprintf(stderr, "%s\n", "break down");
  Next_->Set_FIFO_Prev(Prev_);

  ObsoleteNode(x);
}


template <typename Key, class Comparator>
void SkipList<Key, Comparator>::FIFO::Insert(SkipList::Node* x) {
  // //fprintf(stderr, "%s\n", "FIFO Insert Begin");
  //// //fprintf(stderr, "Insert: %s\n", x->key);
  FreezeNode(x);
  //if(normal_head_ != nullptr)
    //// //fprintf(stderr, "normal head: %s\n", normal_head_->key);
  //// //fprintf(stderr, "freezenode done!\n");
  if(head_ == nullptr) {            // 第一次插入, 直接返回; 注意key-value过大则赋为cold_head_;
    if(x->Show_Node_Size() <= threshold_) {
      normal_head_ = head_ = cur_node_ = x;
      hot_mem.fetch_add(x->Show_Node_Size(), std::memory_order_relaxed);
    }
    else {
      cold_head_ = head_ = cur_node_ = x;
      cold_mem.fetch_add(x->Show_Node_Size(), std::memory_order_relaxed);
    }
    return;
  }

  if(normal_head_ == nullptr) {             // 这里是新插入的key过大, freezenode将所有的Hot_mem都变为了cold_mem;
      //// //fprintf(stderr, "go here!\n");
      if(x->Show_Node_Size() <= threshold_) {
          normal_head_ = x;
          hot_mem.fetch_add(x->Show_Node_Size(), std::memory_order_relaxed);  // 这里别忘了!
      } 
      else {
        if(cold_head_ == nullptr)
          cold_head_ = head_;

        cold_mem.fetch_add(x->Show_Node_Size(), std::memory_order_relaxed);
      }
   }
   else {
      hot_mem.fetch_add(x->Show_Node_Size(), std::memory_order_relaxed);
   }
  //// //fprintf(stderr, "0\n");
  x->NoBarrier_Set_FIFO_Prev(cur_node_);  // 完成双向链表构建;
  cur_node_->Set_FIFO_Next(x);
  // cur_node_->Set_FIFO_Next(x->FIFO_Prev());
  //// //fprintf(stderr, "1\n");
  cur_node_ = cur_node_->FIFO_Next();
  //////// //fprintf(stderr, "%zu\n", normal_head_->key);
  // //fprintf(stderr, "%s\n", "FIFO Insert End!");
  ////// //fprintf(stderr, "hot_mem now: %zu\n", Hot_MemoryUsage());
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
void SkipList<Key, Comparator>::FIFO::Move_head() {
  normal_head_->Set_FIFO_Next(cur_node_); 
}
/*template <typename Key, class Comparator>
size_t SkipList<Key, Comparator>::FIFO::Get_Threshold() {
  return threshold_;
}*/

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
  /*if(list_->FIFO_->cold_head_ == nullptr)
      node_ = list_->FIFO_->normal_head_;
  else
      node_ = list_->FIFO_->cold_head_;*/
  node_ = list_->FIFO_->head_;
}

template <typename Key, class Comparator>
inline void SkipList<Key, Comparator>::FIFO::FIFO_Iterator::SeekToLast() {
  node_ = list_->FIFO_->cur_node_;
}

template <typename Key, class Comparator>
inline void SkipList<Key, Comparator>::FIFO::FIFO_Iterator::SeekToNormal() {
  node_ = list_->FIFO_->normal_head_;
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
  //fprintf(stderr, "SkipList Iterator: SeekToFirst!\n");
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
inline bool SkipList<Key, Comparator>::Iterator::SeekToNextKey() {
  Slice a = GetLengthPrefixedSlice_SkipList(node_->key);
  Slice a_ptr = ExtractUserKey_SkipList(a);

  while(node_ != nullptr) {
    node_ = node_->Next(0);
    if(node_ == nullptr)    // 这里可能segmentation fault;
      return false;
    Slice b = GetLengthPrefixedSlice_SkipList(node_->key);   
    Slice b_ptr = ExtractUserKey_SkipList(b);

    if(a_ptr.compare(b_ptr) != 0)
       return true;
  }

  return false;
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
  return (n != nullptr) && (compare_(n->key, key) < 0);   // 注意这里是反过来的;
}

template <typename Key, class Comparator>
typename SkipList<Key, Comparator>::Node*
SkipList<Key, Comparator>::FindGreaterOrEqual(const Key& key,
                                              Node** prev) const {
  //////// //fprintf(stderr, "%d\n", 1);
  Node* x = head_;
  int level = GetMaxHeight() - 1;
  while (true) {
    // //////// //fprintf(stderr, "%d\n", x->key());
    Node* next = x->Next(level);
    if (KeyIsAfterNode(key, next)) {
      // Keep searching in this list
      x = next;
      // //////// //fprintf(stderr, "%d\n", 4);
    } else {
      if (prev != nullptr) prev[level] = x;
      if (level == 0) {
        return next;
      } else {
        // Switch to next list
        level--;
      }
      // //////// //fprintf(stderr, "%d\n", 5);
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
SkipList<Key, Comparator>::SkipList(Comparator cmp, Arena* arena, size_t threshold)
    : compare_(cmp),
      arena_(arena),
      head_(NewNode(0 /* any key will do */, kMaxHeight, 0)),
      max_height_(1),
      rnd_(0xdeadbeef) {
  FIFO_ = new FIFO(threshold);
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
  // //fprintf(stderr, "%s\n", "SkipList Insert Begin!");
  Node* prev[kMaxHeight];
  Node* x = FindGreaterOrEqual(key, prev);   // 找到对于x的每个level的要插入的前一个结点;
  //////// //fprintf(stderr, "%d\n", 2);
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
  ////// //fprintf(stderr, "begin new node!\n");
  x = NewNode(key, height, Size);
  
  // ////// //fprintf(stderr, "node size: %zu\n", x->Show_Node_Size());
  //////// //fprintf(stderr, "Node Size: %zu\n", sizeof(*x));

  for (int i = 0; i < height; i++) {
    // NoBarrier_SetNext() suffices since we will add a barrier when
    // we publish a pointer to "x" in prev[i].
    x->NoBarrier_SetNext(i, prev[i]->NoBarrier_Next(i));
    prev[i]->SetNext(i, x);
  }
  //// //fprintf(stderr, "begin fifo insert!\n");
  FIFO_->Insert(x);
  // //fprintf(stderr, "%s\n", "SkipList Insert Done!");
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

template <typename Key, class Comparator>
size_t SkipList<Key, Comparator>::Hot_MemoryUsage_() {
  return FIFO_->Hot_MemoryUsage();
}

template <typename Key, class Comparator>
size_t SkipList<Key, Comparator>::Cold_MemoryUsage_() {
  return FIFO_->Cold_MemoryUsage();
}

template <typename Key, class Comparator>
void SkipList<Key, Comparator>::ThrawNode(const Key& key, int r) {
  //////// //fprintf(stderr, "%s\n", "begin thraw node!");
  Node* x = FindGreaterOrEqual(key, nullptr);   // 找到该结点;  
  
  FIFO_->DeleteNode(x, r);
  //////// //fprintf(stderr, "%s\n", "end thraw node!");
}

template <typename Key, class Comparator>
void SkipList<Key, Comparator>::FindNextKey(Node** x) {
  Slice a = GetLengthPrefixedSlice_SkipList((*x)->key);
  Slice a_ptr = ExtractUserKey_SkipList(a);

  // Node* tmp = x;
  while((*x) != nullptr) {
    (*x) = (*x)->Next(0);
    if((*x) == nullptr)
      return;
    Slice b = GetLengthPrefixedSlice_SkipList((*x)->key);
    Slice b_ptr = ExtractUserKey_SkipList(b);

    if(a_ptr.compare(b_ptr) != 0)
       return;
  }

  return;
}


template <typename Key, class Comparator>
void SkipList<Key, Comparator>::Seperate(const Key& normal_key) {
   // 连接所有cold_node; findnextkey + 判断是否是cold;
  ////// //fprintf(stderr, "normal key: %s\n", normal_key);
  Node* x = head_->Next(0);
  Node* prev_node = head_;

  while(x != nullptr) {
    Key cur_key = x->key;
    // ////// //fprintf(stderr, "before: %s\n", cur_key);
    if(normal_key == nullptr || CompareSequence_SkipList(cur_key, normal_key) > 0) {  // 大于0, 是cold_node; 
      //////// //fprintf(stderr, "move: %s\n", cur_key);
      prev_node->SetNext(0, x);
      prev_node = x;
    }

    FindNextKey(&x);
    //if(x != nullptr)
      //////// //fprintf(stderr, "after: %s\n", x->key);
  }
  prev_node->SetNext(0, nullptr);  // 最后一定要设置!
  
}

template <typename Key, class Comparator>
void SkipList<Key, Comparator>::SetHead(const Key& key) {
  //////// //fprintf(stderr, "set head: %s\n", key);
  Node* x = FindGreaterOrEqual(key, nullptr);
  //////// //fprintf(stderr, "x->key: %s\n", x->key);
  head_->SetNext(0, x);   // 这里set最底层吗?
  //////// //fprintf(stderr, "head_ now: %s\n", head_->Next(0)->key);
  // 移动head即可, 剩余的结点统一析构;
}

template <typename Key, class Comparator>
void SkipList<Key, Comparator>::Test() {
  FIFO_->Move_head();
}


}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_SKIPLIST_H_
