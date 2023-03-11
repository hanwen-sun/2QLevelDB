#include "leveldb/table.h"

#include <map>
#include <string>

#include "gtest/gtest.h"
#include "db/dbformat.h"
#include "db/memtable.h"
#include "db/write_batch_internal.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "leveldb/table_builder.h"
#include "table/block.h"
#include "table/block_builder.h"
#include "table/format.h"
#include "util/random.h"
#include "util/testutil.h"


namespace leveldb {

static const int kValueSize = 1000;

TEST(MemTableTest, Simple) {
  InternalKeyComparator cmp(BytewiseComparator());
  MemTable* memtable = new MemTable(cmp, 1024);
  memtable->Ref();
  WriteBatch batch;
  WriteBatchInternal::SetSequence(&batch, 100);
  batch.Put(std::string("k1"), std::string("v1"));
  batch.Put(std::string("k1"), std::string("v11"));
  batch.Put(std::string("k2"), std::string("v2"));
  batch.Put(std::string("k3"), std::string("v3"));
  batch.Put(std::string("k2"), std::string("v22"));
  batch.Put(std::string("largekey"), std::string("vlarge"));
  // batch.Put(std::string("k1"), std::string("v11"));
  batch.Put(std::string("k3"), std::string("v33"));
  batch.Put(std::string("k1"), std::string("v111"));
  batch.Put(std::string("k4"), std::string("v4"));
  // batch.Put(std::string("k5"), std::string(std::string(100000, 'x')));

  ASSERT_TRUE(WriteBatchInternal::InsertInto(&batch, memtable).ok());

  std::string value;
  LookupKey lkey(std::string("k2"), 110);  // 这里的sequence_number到底是干啥的?   相当于你能使用的最新的sequence_key;
  ASSERT_TRUE(memtable->Get(lkey, &value, nullptr));   // 如果memtable使用过程中出现比该seq_key还新, 则返回false;
  ASSERT_EQ(value, "v22");
  std::fprintf(stderr, "value: %s\n", value.c_str());

  Iterator* iter = memtable->NewIterator();
  iter->SeekToFirst();
  while (iter->Valid()) {
    std::fprintf(stderr, "key: '%s' -> '%s'\n", iter->key().ToString().c_str(),
                 iter->value().ToString().c_str());
    iter->Next();
  }

  Iterator* FIFO_iter = memtable->NewFIFOIterator();
  FIFO_iter->SeekToFirst();
  while (FIFO_iter->Valid()) {
    std::fprintf(stderr, "key: '%s' -> '%s'\n", FIFO_iter->key().ToString().c_str(),
                 FIFO_iter->value().ToString().c_str());
    FIFO_iter->Next();
  }

  FIFO_iter->SeekToLast();
  while (FIFO_iter->Valid()) {
    std::fprintf(stderr, "key: '%s' -> '%s'\n", FIFO_iter->key().ToString().c_str(),
                FIFO_iter->value().ToString().c_str());
    FIFO_iter->Prev();
  }

  // memtable->Test();
  delete iter;
  delete FIFO_iter;
  memtable->Unref(); 
  // delete memtable;
}

TEST(MemTableTest, Insert) {
    
}

}