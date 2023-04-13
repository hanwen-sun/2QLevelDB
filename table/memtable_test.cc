#include "leveldb/table.h"

#include <map>
#include <string>
#include <utility>
#include <cstdlib>

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

TEST(MemTableTest, DISABLED_Simple) {
  InternalKeyComparator cmp(BytewiseComparator());
  
  MemTable* memtable = new MemTable(cmp, 300);
  memtable->Ref();
  WriteBatch batch;
  WriteBatchInternal::SetSequence(&batch, 100);
  batch.Put(std::string("k1"), std::string("v1"));
  batch.Put(std::string("k1"), std::string("v11"));
  batch.Put(std::string("k2"), std::string("v2"));
  batch.Put(std::string("k2"), std::string("v22"));  // shrink hot memory!;  这里会出事!
  batch.Put(std::string("k3"), std::string("v3"));
  batch.Put(std::string("k4"), std::string("v4"));
  batch.Put(std::string("largekey"), std::string("vlarge"));

  batch.Put(std::string("k11"), std::string("v11"));
  batch.Put(std::string("k5"), std::string("v5"));
  batch.Put(std::string("k6"), std::string("v6"));
  batch.Put(std::string("k1"), std::string("v111"));  // shrink cold memory!;  (移动cold_head)
  batch.Put(std::string("k3"), std::string("v33"));  // shrink cold memory!;
  batch.Put(std::string("k1"), std::string("v"));  // shrink hot memory!
  
  
  // batch.Put(std::string("k5"), std::string(std::string(100000, 'x')));

  ASSERT_TRUE(WriteBatchInternal::InsertInto(&batch, memtable).ok());

  //std::fprintf(stderr, "value: %s\n", value.c_str());
  // memtable->Seperate();
  Iterator* iter = memtable->NewIterator();
  iter->SeekToFirst();
  while (iter->Valid()) {
    std::fprintf(stderr, "key: '%s' -> '%s'\n", iter->key().ToString().c_str(),
                 iter->value().ToString().c_str());
    iter->Next();
  }
  fprintf(stderr, "\n");

  Iterator* FIFO_iter = memtable->NewFIFOIterator();
  FIFO_iter->SeekToFirst();
  while (FIFO_iter->Valid()) {
    std::fprintf(stderr, "key: '%s' -> '%s'\n", FIFO_iter->key().ToString().c_str(),
                 FIFO_iter->value().ToString().c_str());
    FIFO_iter->Next();
  }
  fprintf(stderr, "\n");
  /*
  FIFO_iter->SeekToLast();
  while (FIFO_iter->Valid()) {
    std::fprintf(stderr, "key: '%s' -> '%s'\n", FIFO_iter->key().ToString().c_str(),
                FIFO_iter->value().ToString().c_str());
    FIFO_iter->Prev();
  } */
  std::vector<ParsedNormalKey> hot_data;
  memtable->ExtractHot(hot_data);

  for(auto it : hot_data) {
    std::string s = it.DebugString();
    std::cout << s << std::endl;
  }

  memtable->Seperate();
  //delete iter;
  
  //Iterator* iter_ = memtable->NewIterator();   // 这里需要新Iterator吗?
  iter->SeekToFirst();
  while (iter->Valid()) {
    std::fprintf(stderr, "key: '%s' -> '%s'\n", iter->key().ToString().c_str(),
                 iter->value().ToString().c_str());
    iter->Next();
  }
  // memtable->Test();
  delete iter;
  delete FIFO_iter;
  memtable->Unref(); 
  // delete memtable;
}

TEST(MemTableTest, Iterator_Test) {
  InternalKeyComparator cmp(BytewiseComparator());
  
  MemTable* memtable = new MemTable(cmp, 300);
  memtable->Ref();
  WriteBatch batch;
  WriteBatchInternal::SetSequence(&batch, 100);
  batch.Put(std::string("k1"), std::string("v1"));
  batch.Put(std::string("k1"), std::string("v11"));
  batch.Put(std::string("k2"), std::string("v2"));
  batch.Put(std::string("k2"), std::string("v22"));  
  batch.Put(std::string("k3"), std::string("v3"));
  batch.Put(std::string("k4"), std::string("v4"));
  batch.Put(std::string("largekey"), std::string("vlarge"));
  ASSERT_TRUE(WriteBatchInternal::InsertInto(&batch, memtable).ok());

  Iterator* iter = memtable->NewIterator();
  iter->SeekToFirst();
  while (iter->Valid()) {
    std::fprintf(stderr, "key: '%s' -> '%s'\n", iter->key().ToString().c_str(),
                 iter->value().ToString().c_str());
    iter->Next();
  }
  fprintf(stderr, "\n");

  batch.Clear();
  batch.Put(std::string("k11"), std::string("v11"));
  batch.Put(std::string("k5"), std::string("v5"));
  batch.Put(std::string("k6"), std::string("v6"));
  batch.Put(std::string("k1"), std::string("v111"));
  batch.Put(std::string("k3"), std::string("v33"));  
  batch.Put(std::string("k1"), std::string("v"));
  ASSERT_TRUE(WriteBatchInternal::InsertInto(&batch, memtable).ok());

  //iter->SeekToFirst();
  fprintf(stderr, "iterator the second time!\n");
  while (iter->Valid()) {
    std::fprintf(stderr, "key: '%s' -> '%s'\n", iter->key().ToString().c_str(),
                 iter->value().ToString().c_str());
    iter->Next();
  }

  delete iter;
  memtable->Unref();
}

TEST(SeperateTest, DISABLED_near_Insert_1) {
    std::vector<std::pair<std::string, std::string>> data;
    for(int i = 0; i < 5; i++) {
      std::string key = "k" + std::to_string(i);
      std::string value = "v" + std::to_string(i);

      data.push_back({key, value});
    }

    InternalKeyComparator cmp(BytewiseComparator());
    MemTable* memtable = new MemTable(cmp, 300);
    memtable->Ref();
    WriteBatch batch;
    WriteBatchInternal::SetSequence(&batch, 100);

    for(int i = 0; i < 5; i++) {
      for(int j = 0; j < 3; j++) {
          batch.Put(data[i].first, data[i].second + std::to_string(i));    
      }
    }

    ASSERT_TRUE(WriteBatchInternal::InsertInto(&batch, memtable).ok());

    Iterator* FIFO_iter = memtable->NewFIFOIterator();
    FIFO_iter->SeekToFirst();
    fprintf(stderr, "FIFO List:\n");
    while (FIFO_iter->Valid()) {
      std::fprintf(stderr, "key: '%s' -> '%s'\n", FIFO_iter->key().ToString().c_str(),
                 FIFO_iter->value().ToString().c_str());
      FIFO_iter->Next();
    }
    fprintf(stderr, "\n");

    std::vector<ParsedNormalKey> hot_data;
    memtable->ExtractHot(hot_data);

    for(auto it : hot_data) {
      std::string s = it.DebugString();
      std::cout << s << std::endl;
    }

    ASSERT_EQ(false, memtable->Seperate());
    /*Iterator* iter = memtable->NewIterator();
    iter->SeekToFirst();
    fprintf(stderr, "Seperate Done:");*/

    delete FIFO_iter;

    memtable->Unref();
}

TEST(SeperateTest, DISABLED_near_Insert_2) {
    std::vector<std::pair<std::string, std::string>> data;
    for(int i = 0; i < 30; i++) {
      std::string key = "k" + std::to_string(i);
      std::string value = "v" + std::to_string(i);

      data.push_back({key, value});
    }

    InternalKeyComparator cmp(BytewiseComparator());
    MemTable* memtable = new MemTable(cmp, 500);
    memtable->Ref();
    WriteBatch batch;
    WriteBatchInternal::SetSequence(&batch, 100);

    for(int i = 0; i < 30; i++) {
      for(int j = 0; j < 3; j++) {
          batch.Put(data[i].first, data[i].second);  
          data[i].second = data[i].second + std::to_string(i);
      }
    }

    ASSERT_TRUE(WriteBatchInternal::InsertInto(&batch, memtable).ok());

    /*Iterator* FIFO_iter = memtable->NewFIFOIterator();
    FIFO_iter->SeekToFirst();
    fprintf(stderr, "FIFO List:\n");
    while (FIFO_iter->Valid()) {
      std::fprintf(stderr, "key: '%s' -> '%s'\n", FIFO_iter->key().ToString().c_str(),
                 FIFO_iter->value().ToString().c_str());
      FIFO_iter->Next();
    }
    fprintf(stderr, "\n"); */

    std::vector<ParsedNormalKey> hot_data;
    memtable->ExtractHot(hot_data);

    for(auto it : hot_data) {
      std::string s = it.DebugString();
      std::cout << s << std::endl;
    }

    ASSERT_EQ(true, memtable->Seperate());

    Iterator* iter = memtable->NewIterator();   
    iter->SeekToFirst();
    fprintf(stderr, "SkipList List:\n");
    while (iter->Valid()) {
      std::fprintf(stderr, "key: '%s' -> '%s'\n", iter->key().ToString().c_str(),
                 iter->value().ToString().c_str());
      iter->Next();
    }

    delete iter;
    //delete FIFO_iter;

    memtable->Unref();
}

TEST(SeperateTest, DISABLED_Seq_Insert_1) {
  std::vector<std::pair<std::string, std::string>> data;
    for(int i = 0; i < 5; i++) {
      std::string key = "k" + std::to_string(i);
      std::string value = "v" + std::to_string(i);

      data.push_back({key, value});
    }

    InternalKeyComparator cmp(BytewiseComparator());
    MemTable* memtable = new MemTable(cmp, 300);
    memtable->Ref();
    WriteBatch batch;
    WriteBatchInternal::SetSequence(&batch, 100);

    for(int i = 0; i < 3; i++) {
      for(int j = 0; j < 5; j++) {
          batch.Put(data[j].first, data[j].second);  
          data[j].second = data[j].second + std::to_string(j);  
      }
    }

    ASSERT_TRUE(WriteBatchInternal::InsertInto(&batch, memtable).ok());

    Iterator* FIFO_iter = memtable->NewFIFOIterator();
    FIFO_iter->SeekToFirst();
    fprintf(stderr, "FIFO List:\n");
    while (FIFO_iter->Valid()) {
      std::fprintf(stderr, "key: '%s' -> '%s'\n", FIFO_iter->key().ToString().c_str(),
                 FIFO_iter->value().ToString().c_str());
      FIFO_iter->Next();
    }
    fprintf(stderr, "\n");

    std::vector<ParsedNormalKey> hot_data;
    memtable->ExtractHot(hot_data);

    for(auto it : hot_data) {
      std::string s = it.DebugString();
      std::cout << s << std::endl;
    }

    ASSERT_EQ(false, memtable->Seperate());
    /*Iterator* iter = memtable->NewIterator();
    iter->SeekToFirst();
    fprintf(stderr, "Seperate Done:");*/
    /*Iterator* iter = memtable->NewIterator();   
    iter->SeekToFirst();
    fprintf(stderr, "SkipList List:\n");
    while (iter->Valid()) {
      std::fprintf(stderr, "key: '%s' -> '%s'\n", iter->key().ToString().c_str(),
                 iter->value().ToString().c_str());
      iter->Next();
    }*/

    delete FIFO_iter;
    //delete iter;

    memtable->Unref();
}

TEST(SeperateTest, DISABLED_Seq_Insert_2) {
  std::vector<std::pair<std::string, std::string>> data;
    for(int i = 0; i < 30; i++) {
      std::string key = "k" + std::to_string(i);
      std::string value = "v" + std::to_string(i);

      data.push_back({key, value});
    }

    InternalKeyComparator cmp(BytewiseComparator());
    MemTable* memtable = new MemTable(cmp, 300);
    memtable->Ref();
    WriteBatch batch;
    WriteBatchInternal::SetSequence(&batch, 100);

    for(int i = 0; i < 3; i++) {
      for(int j = 0; j < 30; j++) {
          batch.Put(data[j].first, data[j].second);  
          data[j].second = data[j].second + std::to_string(j);  
      }
    }

    ASSERT_TRUE(WriteBatchInternal::InsertInto(&batch, memtable).ok());

    Iterator* FIFO_iter = memtable->NewFIFOIterator();
    FIFO_iter->SeekToFirst();
    fprintf(stderr, "FIFO List:\n");
    while (FIFO_iter->Valid()) {
      std::fprintf(stderr, "key: '%s' -> '%s'\n", FIFO_iter->key().ToString().c_str(),
                 FIFO_iter->value().ToString().c_str());
      FIFO_iter->Next();
    }
    fprintf(stderr, "\n");

    std::vector<ParsedNormalKey> hot_data;
    memtable->ExtractHot(hot_data);

    for(auto it : hot_data) {
      std::string s = it.DebugString();
      std::cout << s << std::endl;
    }

    ASSERT_EQ(true, memtable->Seperate());

    Iterator* iter = memtable->NewIterator();   
    iter->SeekToFirst();
    fprintf(stderr, "SkipList List:\n");
    while (iter->Valid()) {
      std::fprintf(stderr, "key: '%s' -> '%s'\n", iter->key().ToString().c_str(),
                 iter->value().ToString().c_str());
      iter->Next();
    }

    delete FIFO_iter;
    delete iter;

    memtable->Unref();
}

TEST(SeperateTest, DISABLED_Random_Insert_1) {
    std::vector<std::pair<std::string, std::string>> data;
    for(int i = 0; i < 5; i++) {
      std::string key = "k" + std::to_string(i);
      std::string value = "v" + std::to_string(i);

      data.push_back({key, value});
    }

    InternalKeyComparator cmp(BytewiseComparator());
    MemTable* memtable = new MemTable(cmp, 300);
    memtable->Ref();
    WriteBatch batch;
    WriteBatchInternal::SetSequence(&batch, 100);

    srand(22);
    for(int i = 0; i < 30; i++) {
        int j = rand() % 5;

        std::string Value_ = data[j].second;
        for(int k = 0; k < j; k++) {
          Value_ += std::to_string(j);
        }
        
        batch.Put(data[j].first, data[j].second);  
        // data[j].second = data[j].second + std::to_string(j);  
    }

    ASSERT_TRUE(WriteBatchInternal::InsertInto(&batch, memtable).ok());

    Iterator* FIFO_iter = memtable->NewFIFOIterator();
    FIFO_iter->SeekToFirst();
    fprintf(stderr, "FIFO List:\n");
    while (FIFO_iter->Valid()) {
      std::fprintf(stderr, "key: '%s' -> '%s'\n", FIFO_iter->key().ToString().c_str(),
                 FIFO_iter->value().ToString().c_str());
      FIFO_iter->Next();
    }
    fprintf(stderr, "\n");

    std::vector<ParsedNormalKey> hot_data;
    memtable->ExtractHot(hot_data);

    for(auto it : hot_data) {
      std::string s = it.DebugString();
      std::cout << s << std::endl;
    }

    ASSERT_EQ(false, memtable->Seperate());

    Iterator* iter = memtable->NewIterator();   
    iter->SeekToFirst();
    fprintf(stderr, "SkipList List:\n");
    while (iter->Valid()) {
      std::fprintf(stderr, "key: '%s' -> '%s'\n", iter->key().ToString().c_str(),
                 iter->value().ToString().c_str());
      iter->Next();
    }

    delete FIFO_iter;
    delete iter;

    memtable->Unref();
}

TEST(SeperateTest, DISABLED_random_insert_2) {
    std::vector<std::pair<std::string, std::string>> data;
    for(int i = 0; i < 10; i++) {
      std::string key = "k" + std::to_string(i);
      std::string value = "v" + std::to_string(i);

      data.push_back({key, value});
    }

    InternalKeyComparator cmp(BytewiseComparator());
    MemTable* memtable = new MemTable(cmp, 300);
    memtable->Ref();
    WriteBatch batch;
    WriteBatchInternal::SetSequence(&batch, 100);

    srand(22);
    for(int i = 0; i < 30; i++) {
        int j = rand() % 10;
        int k = rand() % 3;
        std::string Value_ = data[j].second;
        while(k--) {
          Value_ += std::to_string(j);
        }
        
        batch.Put(data[j].first, Value_);  
        // data[j].second = data[j].second + std::to_string(j);  
    }

    ASSERT_TRUE(WriteBatchInternal::InsertInto(&batch, memtable).ok());

    Iterator* FIFO_iter = memtable->NewFIFOIterator();
    FIFO_iter->SeekToFirst();
    fprintf(stderr, "FIFO List:\n");
    while (FIFO_iter->Valid()) {
      std::fprintf(stderr, "key: '%s' -> '%s'\n", FIFO_iter->key().ToString().c_str(),
                 FIFO_iter->value().ToString().c_str());
      FIFO_iter->Next();
    }
    fprintf(stderr, "\n");

    std::vector<ParsedNormalKey> hot_data;
    memtable->ExtractHot(hot_data);

    for(auto it : hot_data) {
      std::string s = it.DebugString();
      std::cout << s << std::endl;
    }

    ASSERT_EQ(true, memtable->Seperate());

    Iterator* iter = memtable->NewIterator();   
    iter->SeekToFirst();
    fprintf(stderr, "SkipList List:\n");
    while (iter->Valid()) {
      std::fprintf(stderr, "key: '%s' -> '%s'\n", iter->key().ToString().c_str(),
                 iter->value().ToString().c_str());
      iter->Next();
    }

    delete FIFO_iter;
    delete iter;

    memtable->Unref();
}

TEST(SeperateTest, Only_Hot) {
  InternalKeyComparator cmp(BytewiseComparator());
  
  MemTable* memtable = new MemTable(cmp, 3000);
  memtable->Ref();
  WriteBatch batch;
  WriteBatchInternal::SetSequence(&batch, 100);

  batch.Put(std::string("k1"), std::string("v1"));
  batch.Put(std::string("k1"), std::string("v11"));
  batch.Put(std::string("k2"), std::string("v2"));
  batch.Put(std::string("k2"), std::string("v22"));  // shrink hot memory!;  这里会出事!
  batch.Put(std::string("k3"), std::string("v3"));
  batch.Put(std::string("k4"), std::string("v4"));
  batch.Put(std::string("largekey"), std::string("vlarge"));

  batch.Put(std::string("k11"), std::string("v11"));
  batch.Put(std::string("k5"), std::string("v5"));
  batch.Put(std::string("k6"), std::string("v6"));
  batch.Put(std::string("k1"), std::string("v111"));  // shrink cold memory!;  (移动cold_head)
  batch.Put(std::string("k3"), std::string("v33"));  // shrink cold memory!;
  batch.Put(std::string("k1"), std::string("v"));  // shrink hot memory!

  ASSERT_TRUE(WriteBatchInternal::InsertInto(&batch, memtable).ok());

  //std::fprintf(stderr, "value: %s\n", value.c_str());
  EXPECT_EQ(false, memtable->Seperate());
  Iterator* iter = memtable->NewIterator();
  iter->SeekToFirst();
  while (iter->Valid()) {
    std::fprintf(stderr, "key: '%s' -> '%s'\n", iter->key().ToString().c_str(),
                 iter->value().ToString().c_str());
    iter->Next();
  }
  fprintf(stderr, "\n");
  memtable->Seperate();
  iter->SeekToFirst();
  // EXPECT_EQ(false, iter->Valid());
  while (iter->Valid()) {
    std::fprintf(stderr, "key: '%s' -> '%s'\n", iter->key().ToString().c_str(),
                 iter->value().ToString().c_str());
    iter->Next();
  }
}

}
