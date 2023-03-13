// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/memtable.h"
#include "db/dbformat.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "util/coding.h"

namespace leveldb {

static Slice GetLengthPrefixedSlice(const char* data) {
  uint32_t len;
  const char* p = data;
  p = GetVarint32Ptr(p, p + 5, &len);  // +5: we assume "p" is not corrupted
  return Slice(p, len);
}

MemTable::MemTable(const InternalKeyComparator& comparator, size_t threshold)
    : comparator_(comparator), refs_(0), table_(comparator_, &arena_, threshold) {}

MemTable::~MemTable() { assert(refs_ == 0); }

size_t MemTable::ApproximateMemoryUsage() { return arena_.MemoryUsage(); }

int MemTable::KeyComparator::operator()(const char* aptr,
                                        const char* bptr) const {
  // Internal keys are encoded as length-prefixed strings.
  Slice a = GetLengthPrefixedSlice(aptr);
  Slice b = GetLengthPrefixedSlice(bptr);
  return comparator.Compare(a, b);
}

int MemTable::CompareSequence(const char* aptr,
                              const char* bptr) const {
  Slice akey = GetLengthPrefixedSlice(aptr);
  Slice bkey = GetLengthPrefixedSlice(bptr);

  int r = 0;
  const uint64_t anum = DecodeFixed64(akey.data() + akey.size() - 8);
  const uint64_t bnum = DecodeFixed64(bkey.data() + bkey.size() - 8);
  // fprintf(stderr, "anum: %zu bnum: %zu\n", anum, bnum);
  if(anum > bnum) {
    r = -1;
  } else if (anum < bnum) {
    r = +1;
  } 
  return r;
} 

// Encode a suitable internal key target for "target" and return it.
// Uses *scratch as scratch space, and the returned pointer will point
// into this scratch space.
static const char* EncodeKey(std::string* scratch, const Slice& target) {
  scratch->clear();
  PutVarint32(scratch, target.size());
  scratch->append(target.data(), target.size());
  return scratch->data();
}

class MemTableIterator : public Iterator {
 public:
  explicit MemTableIterator(MemTable::Table* table) : iter_(table) {}

  MemTableIterator(const MemTableIterator&) = delete;
  MemTableIterator& operator=(const MemTableIterator&) = delete;

  ~MemTableIterator() override = default;

  bool Valid() const override { return iter_.Valid(); }
  void Seek(const Slice& k) override { iter_.Seek(EncodeKey(&tmp_, k)); }
  void SeekToFirst() override { iter_.SeekToFirst(); }
  void SeekToLast() override { iter_.SeekToLast(); }
  void Next() override { iter_.Next(); }
  void Prev() override { iter_.Prev(); }
  Slice key() const override { return GetLengthPrefixedSlice(iter_.key()); }
  Slice value() const override {
    Slice key_slice = GetLengthPrefixedSlice(iter_.key());
    return GetLengthPrefixedSlice(key_slice.data() + key_slice.size());
  }

  Status status() const override { return Status::OK(); }

 private:
  MemTable::Table::Iterator iter_;
  std::string tmp_;  // For passing to EncodeKey
};

Iterator* MemTable::NewIterator() { return new MemTableIterator(&table_); }

class FIFOIterator : public Iterator {
 public:
  explicit FIFOIterator(MemTable::Table* table) : iter_(table) {}

  FIFOIterator(const FIFOIterator&) = delete;
  FIFOIterator& operator=(const FIFOIterator&) = delete;

  ~FIFOIterator() override = default;

  bool Valid() const override { return iter_.Valid(); }
  void Seek(const Slice& k) override { iter_.Seek(EncodeKey(&tmp_, k)); }
  void SeekToFirst() override { iter_.SeekToFirst(); }
  void SeekToLast() override { iter_.SeekToLast(); }
  void Next() override { iter_.Next(); }
  void Prev() override { iter_.Prev(); }
  Slice key() const override { return GetLengthPrefixedSlice(iter_.key()); }
  Slice value() const override {
    Slice key_slice = GetLengthPrefixedSlice(iter_.key());
    return GetLengthPrefixedSlice(key_slice.data() + key_slice.size());
  }

  Status status() const override { return Status::OK(); }

 private:
  MemTable::Table::FIFO::FIFO_Iterator iter_;
  std::string tmp_;  // For passing to EncodeKey
};

Iterator* MemTable::NewFIFOIterator() { return new FIFOIterator(&table_); }


void MemTable::Add(SequenceNumber s, ValueType type, const Slice& key,
                   const Slice& value) {
  // Format of an entry is concatenation of:
  //  key_size     : varint32 of internal_key.size()
  //  key bytes    : char[internal_key.size()]
  //  tag          : uint64((sequence << 8) | type)
  //  value_size   : varint32 of value.size()
  //  value bytes  : char[value.size()]
  size_t key_size = key.size();
  size_t val_size = value.size();
  size_t internal_key_size = key_size + 8;   // 这里应该是加了tag
  const size_t encoded_len = VarintLength(internal_key_size) +
                             internal_key_size + VarintLength(val_size) +
                             val_size;
  // fprintf(stderr, "encoded_len: %zu\n", encoded_len);

  char* buf = arena_.Allocate(encoded_len);
  char* p = EncodeVarint32(buf, internal_key_size);
  std::memcpy(p, key.data(), key_size);
  p += key_size;
  EncodeFixed64(p, (s << 8) | type);
  p += 8;
  p = EncodeVarint32(p, val_size);
  std::memcpy(p, value.data(), val_size);
  assert(p + val_size == buf + encoded_len);
  // fprintf(stderr, "show: %x\n", buf);

  table_.Insert(buf, encoded_len);   // 这里buf就是插入的key, 迭代器直接找就行;
  // 在这里, insert结束后进行thrawNode!!!;
  // 1. 调用SkipList Iterator 判断是否有两个相同的userkey, 参考get方法;
  // 2. 如果有两个相同的userkey, 传入旧的那个key, 调用SkipList的ThrawNode方法;
      //  根据SkipList找到Node x, 得到x的FIFO_Prev和FIFO_Next;
      //  删除x, 将x放入obslete_中, 注意判断是否是cold_head_ or normal_head_;
  // 3. 结束

  Table::Iterator iter(&table_);
  iter.Seek(buf);
  assert(iter.Valid());
  iter.Next();   // 查找下一个key;
  if(iter.Valid()) {
    // fprintf(stderr, "%s\n", "duplicate!");
    //fprintf(stderr, "%s\n", "yes!");
    const char* entry = iter.key();
    uint32_t key_length;
    const char* key_ptr = GetVarint32Ptr(entry, entry + 5, &key_length);
    // fprintf(stderr, "%s   %s\n", Slice(key_ptr, key_length - 8).ToString().c_str(), key.ToString().c_str());
    // 比较key_ptr和key是否相等;
    if(comparator_.comparator.user_comparator()->Compare(
          Slice(key_ptr, key_length - 8), key) == 0) {
          // fprintf(stderr, "%s\n", "find same user key!");
          // ThrawNode测试方法: 
          // 1. 测试头两个结点相同(立刻删除头结点);
          // 2. 中间删除头结点;
          // 3. 结尾删除尾结点;
          // const char* normal = table_.GetNormal();
          Table::FIFO::FIFO_Iterator iter(&table_);   // 这里对冷热数据的判断拿到memtable中, 更方便;
          iter.SeekToNormal();
          const char* normal_key = iter.key();
          int r = CompareSequence(entry, normal_key);  // 特别注意, 这里是entry与normal_key比较
          // fprintf(stderr, "%d\n", r);

          table_.ThrawNode(entry, r);  // r <= 0, 减少normal区域, 否则减少cold区域;
    }
  }
}

bool MemTable::Get(const LookupKey& key, std::string* value, Status* s) {
  Slice memkey = key.memtable_key();
  Table::Iterator iter(&table_);
  iter.Seek(memkey.data());   // .data()返回一个指向Slice的指针;
  if (iter.Valid()) {
    // entry format is:
    //    klength  varint32
    //    userkey  char[klength]
    //    tag      uint64
    //    vlength  varint32
    //    value    char[vlength]
    // Check that it belongs to same user key.  We do not check the
    // sequence number since the Seek() call above should have skipped
    // all entries with overly large sequence numbers.
    const char* entry = iter.key();
    uint32_t key_length;
    const char* key_ptr = GetVarint32Ptr(entry, entry + 5, &key_length);
    if (comparator_.comparator.user_comparator()->Compare(
            Slice(key_ptr, key_length - 8), key.user_key()) == 0) {
      // Correct user key
      const uint64_t tag = DecodeFixed64(key_ptr + key_length - 8);
      switch (static_cast<ValueType>(tag & 0xff)) {
        case kTypeValue: {
          Slice v = GetLengthPrefixedSlice(key_ptr + key_length);
          value->assign(v.data(), v.size());
          return true;
        }
        case kTypeDeletion:
          *s = Status::NotFound(Slice());
          return true;
      }
    }
  }
  return false;
}

void MemTable::Test() {
  table_.Test();
}

}  // namespace leveldb
