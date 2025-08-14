#include "../config.h"
#include "common/types.h"
#include "common/value.h"
#include "storage/index/index_bptree.h"
#include "storage/buffer/buffer_pool_manager.h"
#include "storage/disk/disk_manager.h"
#include <algorithm>
#include <random>

#include <cassert>
#include <unordered_map>
#include <vector>
#include <unordered_set>
#include <shared_mutex>

#include "gtest/gtest.h"
using namespace wsdb;

class BPTreeTest : public ::testing::Test
{
protected:
  void SetUp() override
  {
    // std::cout << "Setting up test..." << std::endl;

    // Create unique test file name
    test_file_name_ = "bptree_test_" + std::to_string(rand()) + ".idx";

    // std::cout << "Using test file: " << test_file_name_ << std::endl;

    // Create and open test disk file
    disk_manager_ = std::make_unique<DiskManager>();

    // Remove file if it exists
    try {
      DiskManager::DestroyFile(test_file_name_);
    } catch (...) {
      // Ignore if file doesn't exist
    }

    // std::cout << "Creating file..." << std::endl;
    DiskManager::CreateFile(test_file_name_);
    file_id_ = disk_manager_->OpenFile(test_file_name_);
    // std::cout << "File created and opened with ID: " << file_id_ << std::endl;

    log_manager_         = std::make_unique<LogManager>(disk_manager_.get());
    buffer_pool_manager_ = std::make_unique<BufferPoolManager>(disk_manager_.get(), log_manager_.get(), REPLACER_LRU_K);
    // std::cout << "Buffer pool manager created..." << std::endl;

    // Create a simple integer schema for testing
    std::vector<RTField> fields;
    RTField              key_field;
    key_field.field_.field_name_ = "key";
    key_field.field_.field_type_ = TYPE_INT;
    key_field.field_.field_size_ = 4;
    key_field.field_.table_id_   = file_id_;
    fields.push_back(key_field);

    schema_ = std::make_unique<RecordSchema>(fields);
    // std::cout << "Schema created..." << std::endl;

    // Create B+ tree index with the file_id from DiskManager
    // std::cout << "Creating BPTreeIndex..." << std::endl;
    index_ = std::make_unique<BPTreeIndex>(disk_manager_.get(), buffer_pool_manager_.get(), file_id_, schema_.get());
    // std::cout << "BPTreeIndex created successfully!" << std::endl;
  }

  void TearDown() override
  {
    index_.reset();
    buffer_pool_manager_.reset();
    if (file_id_ != INVALID_FILE_ID) {
      disk_manager_->CloseFile(file_id_);
    }
    disk_manager_.reset();
    // Clean up test file
    try {
      DiskManager::DestroyFile(test_file_name_);
    } catch (...) {
      // Ignore cleanup errors
    }
  }

  // Helper to create a record with given key
  auto CreateRecord(int key) -> RecordUptr
  {
    std::vector<ValueSptr> values;

    // Create key value
    auto key_data                      = new char[4];
    *reinterpret_cast<int *>(key_data) = key;
    values.emplace_back(ValueFactory::CreateValue(TYPE_INT, key_data, 4));
    delete[] key_data;

    // std::cout << "Creating record with key: " << key << std::endl;
    auto record = std::make_unique<Record>(schema_.get(), values, INVALID_RID);
    // std::cout << "Record created successfully" << std::endl;
    return record;
  }

  // Helper to extract key from record
  int ExtractKey(const Record &record)
  {
    auto value = record.GetValueAt(0);
    return dynamic_cast<const IntValue *>(value.get())->Get();
  }

  // Helper to create RID
  auto CreateRID(int page_id, int slot_id) -> RID
  {
    return RID{static_cast<page_id_t>(page_id), static_cast<slot_id_t>(slot_id)};
  }

  // Helper function to validate tree structure
  bool ValidateTree(BPTreeIndex *index, const RecordSchema *schema)
  {
    // This is just a placeholder - we'd need access to internal methods
    // For now, let's just return true
    return true;
  }

  static constexpr int      BUFFER_POOL_SIZE = 100;
  static constexpr idx_id_t INDEX_ID         = 1;

  std::unique_ptr<DiskManager>       disk_manager_;
  std::unique_ptr<LogManager>        log_manager_;
  std::unique_ptr<BufferPoolManager> buffer_pool_manager_;
  std::unique_ptr<RecordSchema>      schema_;
  std::unique_ptr<BPTreeIndex>       index_;
  file_id_t                          file_id_;
  std::string                        test_file_name_;
};

// Simple test to debug the split issue
TEST_F(BPTreeTest, DebugSplit)
{
  // Insert just enough keys to force a split and see what happens
  std::vector<int> keys = {5, 1, 8, 3, 7, 2, 9, 4, 6, 0, 10};  // 11 keys should force a split

  for (int key : keys) {
    // std::cout << "\rInserting key: " << key;
    auto record = CreateRecord(key);
    auto rid    = CreateRID(key + 1, 0);

    index_->Insert(*record, rid);

    // Try to search immediately
    auto results = index_->Search(*record);
    if (results.size() != 1) {
      std::cout << "ERROR: Failed to find key " << key << " after insertion!" << std::endl;
      // Let's search for all previously inserted keys
      for (int prev_key : keys) {
        if (prev_key == key)
          break;  // Stop at current key
        auto prev_record  = CreateRecord(prev_key);
        auto prev_results = index_->Search(*prev_record);
        if (prev_results.size() != 1) {
          std::cout << "ERROR: Previously inserted key " << prev_key << " is now missing!" << std::endl;
        }
      }
      FAIL() << "Key not found after insertion";
    }
  }
}

// More aggressive test to trigger multi-level splits
TEST_F(BPTreeTest, DebugMultilevelSplit)
{
  // Insert enough keys to force multiple levels (more than 100 to ensure internal node splits)
  std::vector<int> keys = {77,
      90,
      12,
      45,
      23,
      67,
      89,
      34,
      56,
      78,
      11,
      22,
      33,
      44,
      55,
      66,
      88,
      99,
      100,
      101,
      102,
      103,
      104,
      105,
      106,
      107,
      108,
      109,
      110};  // More than enough to force splits

  // Shuffle to force complex split patterns
  std::random_device rd;
  std::mt19937       g(rd());
  // std::shuffle(keys.begin(), keys.end(), g);
  int inserted_count = 0;
  for (int key : keys) {
    // std::cout << "\rInserting key: " << key;
    auto record = CreateRecord(key);
    auto rid    = CreateRID(key + 1, 0);

    index_->Insert(*record, rid);
    inserted_count++;
    // Try to search immediately
    auto results = index_->Search(*record);
    if (results.size() != 1) {
      std::cout << "ERROR: Failed to find key " << key << " after insertion!" << std::endl;
      FAIL() << "Key not found after insertion";
    }

    // Also verify a few random previous keys are still findable
    if (inserted_count > 5) {
      int  random_index = std::uniform_int_distribution<int>(0, inserted_count - 1)(g);
      int  prev_key     = keys[random_index];
      auto prev_record  = CreateRecord(prev_key);
      auto prev_results = index_->Search(*prev_record);
      if (prev_results.size() != 1) {
        std::cout << "ERROR: Previously inserted key " << prev_key << " is now missing!" << std::endl;
        FAIL() << "Previously inserted key not found";
      }
    }
  }
}

// Test basic insert operation
TEST_F(BPTreeTest, InsertSingle)
{
  auto record = CreateRecord(1);
  auto rid    = CreateRID(1, 0);

  // Insert should succeed
  EXPECT_NO_THROW(index_->Insert(*record, rid));

  // Search should find the record
  auto results = index_->Search(*record);
  EXPECT_EQ(results.size(), 1);
  EXPECT_EQ(results[0].PageID(), rid.PageID());
  EXPECT_EQ(results[0].SlotID(), rid.SlotID());
}

// Test with small shuffle to narrow down the issue
TEST_F(BPTreeTest, InsertBatchSmallShuffle)
{
  const int                               NUM_RECORDS = 50;
  std::vector<std::pair<RecordUptr, RID>> test_data;

  // Create test data in random order
  std::vector<int> keys;
  for (int i = 0; i < NUM_RECORDS; ++i) {
    keys.push_back(i);
  }
  std::random_device rd;
  std::mt19937       g(rd());
  std::shuffle(keys.begin(), keys.end(), g);

  for (int key : keys) {
    auto record = CreateRecord(key);
    auto rid    = CreateRID(key / 10 + 1, key % 10);
    test_data.emplace_back(std::move(record), rid);
  }

  // Insert all records
  for (const auto &[record, rid] : test_data) {
    EXPECT_NO_THROW(index_->Insert(*record, rid));
    // printf("\rInserted key: %d", ExtractKey(*record));

    // search key immediately
    auto results = index_->Search(*record);
    EXPECT_EQ(results.size(), 1);
    if (results.size() != 1) {
      printf("Failed to find key: %d\n", ExtractKey(*record));
      exit(1);
    }
    EXPECT_EQ(results[0].PageID(), rid.PageID());
    EXPECT_EQ(results[0].SlotID(), rid.SlotID());
  }
}

// Test batch insert operation
TEST_F(BPTreeTest, InsertBatch)
{
  const int                               NUM_RECORDS = 10000;
  std::vector<std::pair<RecordUptr, RID>> test_data;

  // Create test data in random order
  std::vector<int> keys;
  for (int i = 0; i < NUM_RECORDS; ++i) {
    keys.push_back(i);
  }
  std::random_device rd;
  std::mt19937       g(rd());
  std::shuffle(keys.begin(), keys.end(), g);

  for (int key : keys) {
    auto record = CreateRecord(key);
    auto rid    = CreateRID(key / 10 + 1, key % 10);  // Distribute across pages
    test_data.emplace_back(std::move(record), rid);
  }

  // Insert all records
  for (const auto &[record, rid] : test_data) {
    EXPECT_NO_THROW(index_->Insert(*record, rid));
    // printf("\rInserted key: %d", ExtractKey(*record));
    fflush(stdout);
    // search key
    auto results = index_->Search(*record);
    EXPECT_EQ(results.size(), 1);
    if (results.size() != 1) {
      printf("Failed to find key: %d\n", ExtractKey(*record));
      results = index_->Search(*record);
      exit(1);
    }
    EXPECT_EQ(results[0].PageID(), rid.PageID());
    EXPECT_EQ(results[0].SlotID(), rid.SlotID());
  }

  // Verify all records can be found
  for (int i = 0; i < NUM_RECORDS; ++i) {
    auto query_record = CreateRecord(i);
    auto results      = index_->Search(*query_record);
    EXPECT_EQ(results.size(), 1);
    if (results.size() != 1) {
      printf("Failed to find key: %d\n", i);
      results = index_->Search(*query_record);
      exit(1);
    }
    EXPECT_EQ(results[0].PageID(), i / 10 + 1);
    EXPECT_EQ(results[0].SlotID(), i % 10);
  }
}

// Test point search
TEST_F(BPTreeTest, SearchPoint)
{
  // Insert some test data (even numbers only)
  for (int i = 0; i < 10; ++i) {
    auto record = CreateRecord(i * 2);
    auto rid    = CreateRID(i + 1, 0);
    EXPECT_NO_THROW(index_->Insert(*record, rid));
  }

  // Search for existing keys
  for (int i = 0; i < 10; ++i) {
    auto query_record = CreateRecord(i * 2);
    auto results      = index_->Search(*query_record);
    EXPECT_EQ(results.size(), 1);
    EXPECT_EQ(results[0].PageID(), i + 1);
    EXPECT_EQ(results[0].SlotID(), 0);
  }

  // Search for non-existing keys
  for (int i = 0; i < 10; ++i) {
    auto query_record = CreateRecord(i * 2 + 1);
    auto results      = index_->Search(*query_record);
    EXPECT_TRUE(results.empty());
  }
}

// Test empty tree search
TEST_F(BPTreeTest, SearchEmpty)
{
  auto query_record = CreateRecord(1);
  auto results      = index_->Search(*query_record);
  EXPECT_TRUE(results.empty());
}

// Test iterator functionality
TEST_F(BPTreeTest, Iterator)
{
  const int NUM_RECORDS = 1000;

  // Insert test data in random order
  std::vector<int> keys;
  for (int i = 0; i < NUM_RECORDS; ++i) {
    keys.push_back(i);
  }
  std::random_device rd;
  std::mt19937       g(rd());
  std::shuffle(keys.begin(), keys.end(), g);

  for (int key : keys) {
    auto record = CreateRecord(key);
    auto rid    = CreateRID(key + 1, 0);
    EXPECT_NO_THROW(index_->Insert(*record, rid));
  }

  // Test full scan using iterator
  auto             iter     = index_->Begin();
  auto             end_iter = index_->End();
  std::vector<int> scanned_keys;

  while (iter->IsValid()) {
    auto key_record = iter->GetKey();
    scanned_keys.push_back(ExtractKey(key_record));
    iter->Next();

    // Prevent infinite loop
    if (scanned_keys.size() > NUM_RECORDS)
      break;
  }

  // Should scan all keys in sorted order
  EXPECT_EQ(scanned_keys.size(), NUM_RECORDS);
  for (int i = 0; i < NUM_RECORDS; ++i) {
    EXPECT_EQ(scanned_keys[i], i);
  }
}

// Test delete operation
TEST_F(BPTreeTest, DeleteSingle)
{
  // Insert test data
  auto record = CreateRecord(1);
  auto rid    = CreateRID(1, 0);
  EXPECT_NO_THROW(index_->Insert(*record, rid));

  // Verify it exists
  auto search_results = index_->Search(*record);
  EXPECT_EQ(search_results.size(), 1);

  // Delete it
  EXPECT_TRUE(index_->Delete(*record));

  // Verify it's gone
  search_results = index_->Search(*record);
  EXPECT_TRUE(search_results.empty());
}

// Test generalized range search where boundaries may not exist in the index
TEST_F(BPTreeTest, GeneralizedRangeSearch)
{
  const int NUM_RECORDS = 50;

  // Insert test data with gaps (only even numbers from 0 to 100)
  std::vector<int> keys;
  for (int i = 0; i < NUM_RECORDS; ++i) {
    keys.push_back(i * 2);  // 0, 2, 4, 6, ..., 98
  }

  // Insert in random order to stress test the range search
  std::random_device rd;
  std::mt19937       g(rd());
  std::shuffle(keys.begin(), keys.end(), g);

  for (int key : keys) {
    auto record = CreateRecord(key);
    auto rid    = CreateRID(key + 1, 0);
    EXPECT_NO_THROW(index_->Insert(*record, rid));
  }

  // Test case 1: Range where both boundaries don't exist in the index
  // Search for range [7, 25] - should find keys 8, 10, 12, 14, 16, 18, 20, 22, 24
  auto start_record = CreateRecord(7);
  auto end_record   = CreateRecord(25);
  auto results      = index_->SearchRange(*start_record, *end_record);

  std::vector<int> expected_keys = {8, 10, 12, 14, 16, 18, 20, 22, 24};
  EXPECT_EQ(results.size(), expected_keys.size());

  for (size_t i = 0; i < results.size(); ++i) {
    EXPECT_EQ(results[i].PageID(), expected_keys[i] + 1);
    EXPECT_EQ(results[i].SlotID(), 0);
  }

  // Test case 2: Range where lower bound doesn't exist but upper bound does
  // Search for range [21, 30] - should find keys 22, 24, 26, 28, 30
  start_record = CreateRecord(21);
  end_record   = CreateRecord(30);
  results      = index_->SearchRange(*start_record, *end_record);

  expected_keys = {22, 24, 26, 28, 30};
  EXPECT_EQ(results.size(), expected_keys.size());

  for (size_t i = 0; i < results.size(); ++i) {
    EXPECT_EQ(results[i].PageID(), expected_keys[i] + 1);
    EXPECT_EQ(results[i].SlotID(), 0);
  }

  // Test case 3: Range where lower bound exists but upper bound doesn't
  // Search for range [10, 17] - should find keys 10, 12, 14, 16
  start_record = CreateRecord(10);
  end_record   = CreateRecord(17);
  results      = index_->SearchRange(*start_record, *end_record);

  expected_keys = {10, 12, 14, 16};
  EXPECT_EQ(results.size(), expected_keys.size());

  for (size_t i = 0; i < results.size(); ++i) {
    EXPECT_EQ(results[i].PageID(), expected_keys[i] + 1);
    EXPECT_EQ(results[i].SlotID(), 0);
  }

  // Test case 4: Range completely outside the data
  // Search for range [101, 200] - should find nothing
  start_record = CreateRecord(101);
  end_record   = CreateRecord(200);
  results      = index_->SearchRange(*start_record, *end_record);
  EXPECT_TRUE(results.empty());

  // Test case 5: Range completely before the data
  // Search for range [-10, -1] - should find nothing
  start_record = CreateRecord(-10);
  end_record   = CreateRecord(-1);
  results      = index_->SearchRange(*start_record, *end_record);
  EXPECT_TRUE(results.empty());

  // Test case 6: Range with single gap
  // Search for range [3, 5] - should find key 4
  start_record = CreateRecord(3);
  end_record   = CreateRecord(5);
  results      = index_->SearchRange(*start_record, *end_record);

  EXPECT_EQ(results.size(), 1);
  EXPECT_EQ(results[0].PageID(), 5);  // Key 4 is at page 5
}

// Test bounded iterator functionality with range that may not exist
TEST_F(BPTreeTest, BoundedIteratorTest)
{
  const int NUM_RECORDS = 30;

  // Insert test data with gaps (multiples of 3: 0, 3, 6, 9, ..., 87)
  std::vector<int> keys;
  for (int i = 0; i < NUM_RECORDS; ++i) {
    keys.push_back(i * 3);
  }

  // Insert in random order
  std::random_device rd;
  std::mt19937       g(rd());
  std::shuffle(keys.begin(), keys.end(), g);

  for (int key : keys) {
    auto record = CreateRecord(key);
    auto rid    = CreateRID(key + 1, 0);
    EXPECT_NO_THROW(index_->Insert(*record, rid));
  }

  // Test case 1: BeginRange with boundaries that don't exist in the index
  // Range [7, 25] should iterate over keys 9, 12, 15, 18, 21, 24
  auto start_record = CreateRecord(7);
  auto end_record   = CreateRecord(25);
  auto iter         = index_->Begin(*start_record);

  std::vector<int> expected_keys = {9, 12, 15, 18, 21, 24};
  std::vector<int> actual_keys;

  while (iter->IsValid()) {
    auto key_record = iter->GetKey();
    // compare if the key is greater than the upper bound
    if (Record::Compare(key_record, *end_record) > 0) {
      break;  // Stop if we exceed the upper bound
    }
    actual_keys.push_back(ExtractKey(key_record));
    iter->Next();

    // Prevent infinite loop
    if (actual_keys.size() > NUM_RECORDS) {
      break;
    }
  }

  EXPECT_EQ(actual_keys.size(), expected_keys.size());
  for (size_t i = 0; i < actual_keys.size(); ++i) {
    EXPECT_EQ(actual_keys[i], expected_keys[i]);
  }

  // Test case 2: BeginRange with empty result
  // Range [1, 2] should find nothing
  start_record = CreateRecord(1);
  end_record   = CreateRecord(2);
  iter         = index_->Begin(*start_record);

  actual_keys.clear();
  while (iter->IsValid()) {
    auto key_record = iter->GetKey();
    if (Record::Compare(key_record, *end_record) > 0) {
      break;  // Stop if we exceed the upper bound
    }
    actual_keys.push_back(ExtractKey(key_record));
    iter->Next();

    // Prevent infinite loop
    if (actual_keys.size() > NUM_RECORDS) {
      break;
    }
  }

  EXPECT_TRUE(actual_keys.empty());

  // Test case 3: BeginRange with single result
  // Range [5, 7] should find key 6
  start_record = CreateRecord(5);
  end_record   = CreateRecord(7);
  iter         = index_->Begin(*start_record);

  actual_keys.clear();
  while (iter->IsValid()) {
    auto key_record = iter->GetKey();
    if (Record::Compare(key_record, *end_record) > 0) {
      break;  // Stop if we exceed the upper bound
    }
    actual_keys.push_back(ExtractKey(key_record));
    iter->Next();

    // Prevent infinite loop
    if (actual_keys.size() > NUM_RECORDS) {
      break;
    }
  }

  EXPECT_EQ(actual_keys.size(), 1);
  EXPECT_EQ(actual_keys[0], 6);

  // Test case 4: BeginRange spanning multiple leaf pages
  // Range [10, 70] should find many keys
  start_record = CreateRecord(10);
  end_record   = CreateRecord(70);
  iter         = index_->Begin(*start_record);

  actual_keys.clear();
  expected_keys.clear();

  // Expected keys: 12, 15, 18, 21, ..., 69
  for (int i = 12; i <= 69; i += 3) {
    expected_keys.push_back(i);
  }

  while (iter->IsValid()) {
    auto key_record = iter->GetKey();
    if (Record::Compare(key_record, *end_record) > 0) {
      break;  // Stop if we exceed the upper bound
    }
    actual_keys.push_back(ExtractKey(key_record));
    iter->Next();

    // Prevent infinite loop
    if (actual_keys.size() > NUM_RECORDS) {
      break;
    }
  }

  EXPECT_EQ(actual_keys.size(), expected_keys.size());
  for (size_t i = 0; i < actual_keys.size(); ++i) {
    EXPECT_EQ(actual_keys[i], expected_keys[i]);
  }

  // Test case 5: Begin(key) with key that doesn't exist
  // Begin(7) should start from key 9
  auto begin_record = CreateRecord(7);
  iter              = index_->Begin(*begin_record);

  EXPECT_TRUE(iter->IsValid());
  auto key_record = iter->GetKey();
  EXPECT_EQ(ExtractKey(key_record), 9);

  // Test case 6: Begin(key) with key that exists
  // Begin(15) should start from key 15
  begin_record = CreateRecord(15);
  iter         = index_->Begin(*begin_record);

  EXPECT_TRUE(iter->IsValid());
  key_record = iter->GetKey();
  EXPECT_EQ(ExtractKey(key_record), 15);

  // Test case 7: Verify iterator bounds are respected across leaf pages
  // Create a bounded iterator and verify it stops at the correct boundary
  start_record = CreateRecord(20);
  end_record   = CreateRecord(40);
  iter         = index_->Begin(*start_record);

  actual_keys.clear();
  while (iter->IsValid()) {
    auto key_record = iter->GetKey();
    int  key        = ExtractKey(key_record);
    if (Record::Compare(key_record, *end_record) > 0) {
      break;  // Stop if we exceed the upper bound
    }
    actual_keys.push_back(key);

    // Verify that we never exceed the upper bound
    EXPECT_LE(key, 40);

    iter->Next();

    // Prevent infinite loop
    if (actual_keys.size() > NUM_RECORDS) {
      break;
    }
  }

  // Should find keys: 21, 24, 27, 30, 33, 36, 39
  expected_keys = {21, 24, 27, 30, 33, 36, 39};
  EXPECT_EQ(actual_keys.size(), expected_keys.size());
  for (size_t i = 0; i < actual_keys.size(); ++i) {
    EXPECT_EQ(actual_keys[i], expected_keys[i]);
  }
}

// Debug delete batch operation
TEST_F(BPTreeTest, DeleteBatchDebug)
{
  const int NUM_RECORDS = 20;  // Smaller test set for debugging

  // Insert test data
  for (int i = 0; i < NUM_RECORDS; ++i) {
    auto record = CreateRecord(i);
    auto rid    = CreateRID(i + 1, 0);
    EXPECT_NO_THROW(index_->Insert(*record, rid));
  }

  // Verify all keys exist before deletion
  for (int i = 0; i < NUM_RECORDS; ++i) {
    auto query_record = CreateRecord(i);
    auto results      = index_->Search(*query_record);
    EXPECT_EQ(results.size(), 1);
    if (results.size() != 1) {
      std::cout << "Key " << i << " not found before deletion!" << std::endl;
    }
  }

  // Delete every other record
  for (int i = 0; i < NUM_RECORDS; i += 2) {
    auto record = CreateRecord(i);
    EXPECT_TRUE(index_->Delete(*record));

    // Check all remaining keys after each deletion
    for (int j = 0; j < NUM_RECORDS; ++j) {
      auto query_record = CreateRecord(j);
      auto results      = index_->Search(*query_record);

      if (j % 2 == 0 && j <= i) {
        // Should be deleted
        if (results.size() != 0) {
          std::cout << "ERROR: Key " << j << " should be deleted but still exists!" << std::endl;
        }
      } else if (j % 2 == 1) {
        // Should still exist
        if (results.size() != 1) {
          std::cout << "ERROR: Key " << j << " should exist but not found after deleting " << i << "!" << std::endl;
          FAIL() << "Key " << j << " missing after deleting " << i;
        }
      }
    }
  }

  // Final verification
  for (int i = 0; i < NUM_RECORDS; ++i) {
    auto query_record = CreateRecord(i);
    auto results      = index_->Search(*query_record);

    if (i % 2 == 0) {
      // Should be deleted
      EXPECT_TRUE(results.empty());
    } else {
      // Should still exist
      EXPECT_EQ(results.size(), 1);
      if (results.size() != 1) {
        std::cout << "Final check failed for key: " << i << std::endl;
      }
    }
  }
}

// Test batch delete operation
TEST_F(BPTreeTest, DeleteBatch)
{
  const int NUM_RECORDS = 10000;

  std::vector<int> keys(NUM_RECORDS);
  std::iota(keys.begin(), keys.end(), 0);  // Fill with 0, 1, 2, ..., NUM_RECORDS-1
  std::shuffle(keys.begin(), keys.end(), std::mt19937(std::random_device()()));
  // Insert test data
  for (auto key : keys) {
    auto record = CreateRecord(key);
    auto rid    = CreateRID(key + 1, 0);
    EXPECT_NO_THROW(index_->Insert(*record, rid));
  }

  // check if all records are inserted
  for (int i = 0; i < NUM_RECORDS; ++i) {
    auto query_record = CreateRecord(i);
    auto results      = index_->Search(*query_record);
    EXPECT_EQ(results.size(), 1);
    if (results.size() != 1) {
      printf("Failed to find key: %d\n", i);
      results = index_->Search(*query_record);
      exit(1);
    }
    EXPECT_EQ(results[0].PageID(), i + 1);
    EXPECT_EQ(results[0].SlotID(), 0);
  }

  // Delete every other record
  for (int i = 0; i < NUM_RECORDS; i += 2) {
    auto record = CreateRecord(i);
    auto suc    = index_->Delete(*record);
    EXPECT_TRUE(suc);
    if (!suc) {
      printf("(a) Failed to delete key: %d\n", i);
      index_->Delete(*record);
      exit(1);
    } else {
      // use a iterator to check if recorded before deletion is still there
      // auto iter = index_->Begin();
      // printf("After deleting key: %d\n", i);
      // while (iter->IsValid()) {
      //   auto key = iter->GetKey();
      //   auto rid = iter->GetRID();
      //   printf("Key: %d, PageID: %d, SlotID: %d\n", ExtractKey(key), rid.PageID(), rid.SlotID());
      //   iter->Next();
      // }
      // printf("------------------------\n");
    }
    auto result = index_->Search(*record);
    if (result.size() != 0) {
      printf("(b) Failed to delete key: %d\n", i);
      result = index_->Search(*record);
      exit(1);
    }
  }

  // Verify deleted records are gone and remaining records exist
  for (int i = 0; i < NUM_RECORDS; ++i) {
    auto query_record = CreateRecord(i);
    auto results      = index_->Search(*query_record);

    if (i % 2 == 0) {
      // Should be deleted
      EXPECT_TRUE(results.empty());
      if (!results.empty()) {
        printf("Failed to delete key: %d\n", i);
        results = index_->Search(*query_record);
        exit(1);
      }
    } else {
      // Should still exist
      EXPECT_EQ(results.size(), 1);
      if (results.size() != 1) {
        printf("Failed to find key: %d\n", i);
        results = index_->Search(*query_record);
        exit(1);
      }
      EXPECT_EQ(results[0].PageID(), i + 1);
      EXPECT_EQ(results[0].SlotID(), 0);
    }
  }
}

// Test range search
TEST_F(BPTreeTest, SearchRange)
{
  const int NUM_RECORDS = 20;

  // Insert test data (even numbers only)
  for (int i = 0; i < NUM_RECORDS; ++i) {
    auto record = CreateRecord(i * 2);
    auto rid    = CreateRID(i + 1, 0);
    EXPECT_NO_THROW(index_->Insert(*record, rid));
  }

  // Test range search [5, 15]
  auto start_record = CreateRecord(5);
  auto end_record   = CreateRecord(15);

  auto results = index_->SearchRange(*start_record, *end_record);

  // Should find keys: 6, 8, 10, 12, 14 (5 records)
  EXPECT_EQ(results.size(), 5);

  // Results should be in sorted order
  std::vector<int> expected_pages = {4, 5, 6, 7, 8};  // Pages for keys 6, 8, 10, 12, 14
  for (size_t i = 0; i < results.size(); ++i) {
    EXPECT_EQ(results[i].PageID(), expected_pages[i]);
    EXPECT_EQ(results[i].SlotID(), 0);
  }
}

// Test range search with empty result
TEST_F(BPTreeTest, SearchRangeEmpty)
{
  // Insert some data
  auto record1 = CreateRecord(10);
  auto record2 = CreateRecord(20);
  auto rid1    = CreateRID(1, 0);
  auto rid2    = CreateRID(2, 0);
  EXPECT_NO_THROW(index_->Insert(*record1, rid1));
  EXPECT_NO_THROW(index_->Insert(*record2, rid2));

  // Search range that has no matches
  auto start_record = CreateRecord(11);
  auto end_record   = CreateRecord(19);

  auto results = index_->SearchRange(*start_record, *end_record);
  EXPECT_TRUE(results.empty());
}

// Test range search on empty tree
TEST_F(BPTreeTest, SearchRangeEmptyTree)
{
  auto start_record = CreateRecord(1);
  auto end_record   = CreateRecord(10);

  auto results = index_->SearchRange(*start_record, *end_record);
  EXPECT_TRUE(results.empty());
}

// Test edge cases
TEST_F(BPTreeTest, EdgeCases)
{
  // Test with single record
  auto single_record = CreateRecord(42);
  auto rid           = CreateRID(1, 0);
  EXPECT_NO_THROW(index_->Insert(*single_record, rid));

  auto results = index_->Search(*single_record);
  EXPECT_EQ(results.size(), 1);
  EXPECT_EQ(results[0].PageID(), 1);
  EXPECT_EQ(results[0].SlotID(), 0);

  // Test range search with single record
  auto start_record  = CreateRecord(40);
  auto end_record    = CreateRecord(50);
  auto range_results = index_->SearchRange(*start_record, *end_record);
  EXPECT_EQ(range_results.size(), 1);
  EXPECT_EQ(range_results[0].PageID(), 1);
  EXPECT_EQ(range_results[0].SlotID(), 0);

  // Delete single record
  EXPECT_TRUE(index_->Delete(*single_record));
  results = index_->Search(*single_record);
  EXPECT_TRUE(results.empty());

  // Range search on empty tree
  range_results = index_->SearchRange(*start_record, *end_record);
  EXPECT_TRUE(range_results.empty());

  // Test IsEmpty and Size
  EXPECT_TRUE(index_->IsEmpty());
  EXPECT_EQ(index_->Size(), 0);
}

// Test large dataset performance
TEST_F(BPTreeTest, LargeDataset)
{
  const int NUM_RECORDS = 100000;

  // Insert large dataset
  for (int i = 0; i < NUM_RECORDS; ++i) {
    auto record = CreateRecord(i);
    auto rid    = CreateRID(i / 100 + 1, i % 100);
    EXPECT_NO_THROW(index_->Insert(*record, rid));
    printf("\rInserted %d/%d", i + 1, NUM_RECORDS);
    fflush(stdout);
  }
  std::cout << std::endl;

  // Verify all records exist
  for (int i = 0; i < NUM_RECORDS; ++i) {
    auto query_record = CreateRecord(i);
    auto results      = index_->Search(*query_record);
    EXPECT_EQ(results.size(), 1);
    EXPECT_EQ(results[0].PageID(), i / 100 + 1);
    EXPECT_EQ(results[0].SlotID(), i % 100);
  }

  // Test large range query
  auto start_record = CreateRecord(100);
  auto end_record   = CreateRecord(10000);
  auto results      = index_->SearchRange(*start_record, *end_record);
  EXPECT_EQ(results.size(), 9901);  // Keys from 100 to 10000 inclusive

  // Verify range results are in order
  for (size_t i = 0; i < results.size(); ++i) {
    int expected_key = 100 + static_cast<int>(i);
    EXPECT_EQ(results[i].PageID(), expected_key / 100 + 1);
    EXPECT_EQ(results[i].SlotID(), expected_key % 100);
  }
}

// Test mixed operations (insert, delete, search)
TEST_F(BPTreeTest, MixedOperations)
{
  const int          NUM_OPERATIONS = 10000;
  std::random_device rd;
  std::mt19937       gen(rd());

  // first randomly insert keys from 0 to 999
  std::vector<int>        keys;
  std::vector<RID>        values;
  std::unordered_set<int> inserted_keys;

  for (int i = 0; i < NUM_OPERATIONS; ++i) {
    keys.push_back(i);
    values.push_back(CreateRID(i / 10 + 1, i % 10));
  }
  std::shuffle(keys.begin(), keys.end(), gen);

  // Insert all keys initially
  for (int i = 0; i < NUM_OPERATIONS; ++i) {
    auto record = CreateRecord(keys[i]);
    auto rid    = values[i];
    EXPECT_NO_THROW(index_->Insert(*record, rid));
    inserted_keys.insert(keys[i]);
  }

  // Verify all keys were inserted
  for (int i = 0; i < NUM_OPERATIONS; ++i) {
    auto query_record = CreateRecord(keys[i]);
    auto results      = index_->Search(*query_record);
    EXPECT_EQ(results.size(), 1);
    EXPECT_EQ(results[0].PageID(), values[i].PageID());
    EXPECT_EQ(results[0].SlotID(), values[i].SlotID());
  }

  // Now perform NUM_OPERATIONS random operations
  std::uniform_int_distribution<int> op_dist(0, 2);  // 0=insert, 1=delete, 2=search
  std::uniform_int_distribution<int> key_dist(0, NUM_OPERATIONS - 1);

  int insert_count = 0;
  int delete_count = 0;
  int search_count = 0;

  for (int op = 0; op < NUM_OPERATIONS; ++op) {
    int  operation = op_dist(gen);
    int  key_index = key_dist(gen);
    int  key       = keys[key_index];
    auto record    = CreateRecord(key);
    auto rid       = values[key_index];

    switch (operation) {
      case 0:  // Insert
        if (inserted_keys.find(key) == inserted_keys.end()) {
          // Key not present, insert it
          EXPECT_NO_THROW(index_->Insert(*record, rid));
          inserted_keys.insert(key);
          insert_count++;
        } else {
          // Key already exists, this should be handled gracefully
          // Most B+ tree implementations either ignore duplicate inserts or handle them
          // We'll just try to insert and not fail the test if it throws
          continue;  // Skip duplicate insert
        }
        break;

      case 1:  // Delete
        if (inserted_keys.find(key) != inserted_keys.end()) {
          // Key exists, delete it
          EXPECT_TRUE(index_->Delete(*record));
          inserted_keys.erase(key);
          delete_count++;
        } else {
          // Key doesn't exist, deletion should handle this gracefully
          EXPECT_FALSE(index_->Delete(*record));
          continue;
        }
        break;

      case 2:  // Search
      {
        auto results = index_->Search(*record);
        if (inserted_keys.find(key) != inserted_keys.end()) {
          // Key should be found
          EXPECT_EQ(results.size(), 1);
          if (results.size() == 1) {
            EXPECT_EQ(results[0].PageID(), rid.PageID());
            EXPECT_EQ(results[0].SlotID(), rid.SlotID());
          }
        } else {
          // Key should not be found
          EXPECT_TRUE(results.empty());
        }
        search_count++;
      } break;
    }
  }

  // Final verification: search for all keys and verify consistency
  for (int i = 0; i < NUM_OPERATIONS; ++i) {
    auto query_record = CreateRecord(keys[i]);
    auto results      = index_->Search(*query_record);

    if (inserted_keys.find(keys[i]) != inserted_keys.end()) {
      // Should be found
      EXPECT_EQ(results.size(), 1);
      if (results.size() == 1) {
        EXPECT_EQ(results[0].PageID(), values[i].PageID());
        EXPECT_EQ(results[0].SlotID(), values[i].SlotID());
      }
    } else {
      // Should not be found
      EXPECT_TRUE(results.empty());
    }
  }

  // Print operation statistics
  std::cout << "Mixed operations completed:" << std::endl;
  std::cout << "  Inserts: " << insert_count << std::endl;
  std::cout << "  Deletes: " << delete_count << std::endl;
  std::cout << "  Searches: " << search_count << std::endl;
  std::cout << "  Final keys in tree: " << inserted_keys.size() << std::endl;
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
