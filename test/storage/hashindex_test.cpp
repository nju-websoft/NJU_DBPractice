#include "../config.h"
#include "common/types.h"
#include "common/value.h"
#include "storage/index/index_hash.h"
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

class HashIndexTest : public ::testing::Test
{
protected:
  void SetUp() override
  {
    // Create unique test file name
    test_file_name_ = "hash_index_test_" + std::to_string(rand()) + ".idx";

    // Create and open test disk file
    disk_manager_ = std::make_unique<DiskManager>();

    // Remove file if it exists
    try {
      DiskManager::DestroyFile(test_file_name_);
    } catch (...) {
      // Ignore if file doesn't exist
    }

    DiskManager::CreateFile(test_file_name_);
    file_id_ = disk_manager_->OpenFile(test_file_name_);

    log_manager_         = std::make_unique<LogManager>(disk_manager_.get());
    buffer_pool_manager_ = std::make_unique<BufferPoolManager>(disk_manager_.get(), log_manager_.get(), REPLACER_LRU_K);

    // Create a simple integer schema for testing
    std::vector<RTField> fields;
    RTField              key_field;
    key_field.field_.field_name_ = "key";
    key_field.field_.field_type_ = TYPE_INT;
    key_field.field_.field_size_ = 4;
    key_field.field_.table_id_   = file_id_;
    fields.push_back(key_field);

    schema_ = std::make_unique<RecordSchema>(fields);

    // Create hash index with the file_id from DiskManager
    index_ = std::make_unique<HashIndex>(disk_manager_.get(), buffer_pool_manager_.get(), file_id_, schema_.get());
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

    auto record = std::make_unique<Record>(schema_.get(), values, INVALID_RID);
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

  static constexpr int      BUFFER_POOL_SIZE = 100;
  static constexpr idx_id_t INDEX_ID         = 1;

  std::unique_ptr<DiskManager>       disk_manager_;
  std::unique_ptr<LogManager>        log_manager_;
  std::unique_ptr<BufferPoolManager> buffer_pool_manager_;
  std::unique_ptr<RecordSchema>      schema_;
  std::unique_ptr<HashIndex>         index_;
  file_id_t                          file_id_;
  std::string                        test_file_name_;
};

// Test basic insert operation
TEST_F(HashIndexTest, InsertSingle)
{
  auto record = CreateRecord(1);
  auto rid    = CreateRID(1, 0);

  // Insert should succeed
  EXPECT_NO_THROW(index_->Insert(*record, rid));

  // Verify index properties
  EXPECT_EQ(index_->Size(), 1);
  EXPECT_FALSE(index_->IsEmpty());
  EXPECT_EQ(index_->GetHeight(), 2);  // Header + bucket pages

  // Search should find the record
  auto results = index_->Search(*record);
  EXPECT_EQ(results.size(), 1);
  EXPECT_EQ(results[0].PageID(), rid.PageID());
  EXPECT_EQ(results[0].SlotID(), rid.SlotID());
}

// Test batch insert operation
TEST_F(HashIndexTest, InsertBatch)
{
  const int                               NUM_RECORDS = 1000;
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
    
    // Verify immediate search
    auto results = index_->Search(*record);
    EXPECT_EQ(results.size(), 1);
    if (results.size() != 1) {
      printf("Failed to find key: %d\n", ExtractKey(*record));
      FAIL() << "Key not found after insertion";
    }
    EXPECT_EQ(results[0].PageID(), rid.PageID());
    EXPECT_EQ(results[0].SlotID(), rid.SlotID());
  }

  // Verify final index properties
  EXPECT_EQ(index_->Size(), NUM_RECORDS);
  EXPECT_FALSE(index_->IsEmpty());

  // Verify all records can be found
  for (int i = 0; i < NUM_RECORDS; ++i) {
    auto query_record = CreateRecord(i);
    auto results      = index_->Search(*query_record);
    EXPECT_EQ(results.size(), 1);
    if (results.size() != 1) {
      printf("Failed to find key: %d\n", i);
      FAIL() << "Key not found in final verification";
    }
    EXPECT_EQ(results[0].PageID(), i / 10 + 1);
    EXPECT_EQ(results[0].SlotID(), i % 10);
  }
}

// Test point search
TEST_F(HashIndexTest, SearchPoint)
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

// Test empty index search
TEST_F(HashIndexTest, SearchEmpty)
{
  auto query_record = CreateRecord(1);
  auto results      = index_->Search(*query_record);
  EXPECT_TRUE(results.empty());
  EXPECT_TRUE(index_->IsEmpty());
  EXPECT_EQ(index_->Size(), 0);
}

// Test iterator functionality
TEST_F(HashIndexTest, Iterator)
{
  const int NUM_RECORDS = 100;

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
    auto rid        = iter->GetRID();
    int  key        = ExtractKey(key_record);
    scanned_keys.push_back(key);
    iter->Next();
  }

  // Should scan all keys (order not guaranteed for hash index)
  EXPECT_EQ(scanned_keys.size(), NUM_RECORDS);
  
  // Verify all keys are present
  std::sort(scanned_keys.begin(), scanned_keys.end());
  for (int i = 0; i < NUM_RECORDS; ++i) {
    EXPECT_EQ(scanned_keys[i], i);
  }
}

// Test iterator with empty index
TEST_F(HashIndexTest, IteratorEmpty)
{
  auto iter = index_->Begin();
  EXPECT_FALSE(iter->IsValid());
}

// Test iterator with single element
TEST_F(HashIndexTest, IteratorSingle)
{
  auto record = CreateRecord(42);
  auto rid    = CreateRID(1, 0);
  index_->Insert(*record, rid);

  auto iter = index_->Begin();
  EXPECT_TRUE(iter->IsValid());
  
  auto key_record = iter->GetKey();
  auto found_rid  = iter->GetRID();
  
  EXPECT_EQ(ExtractKey(key_record), 42);
  EXPECT_EQ(found_rid.PageID(), 1);
  EXPECT_EQ(found_rid.SlotID(), 0);
  
  iter->Next();
  EXPECT_FALSE(iter->IsValid());
}

// Test delete operation
TEST_F(HashIndexTest, DeleteSingle)
{
  // Insert test data
  auto record = CreateRecord(1);
  auto rid    = CreateRID(1, 0);
  EXPECT_NO_THROW(index_->Insert(*record, rid));

  // Verify it exists
  auto search_results = index_->Search(*record);
  EXPECT_EQ(search_results.size(), 1);
  EXPECT_EQ(index_->Size(), 1);
  EXPECT_FALSE(index_->IsEmpty());

  // Delete it
  EXPECT_TRUE(index_->Delete(*record));

  // Verify it's gone
  search_results = index_->Search(*record);
  EXPECT_TRUE(search_results.empty());
  EXPECT_EQ(index_->Size(), 0);
  EXPECT_TRUE(index_->IsEmpty());
}

// Test delete non-existing key
TEST_F(HashIndexTest, DeleteNonExisting)
{
  // Try to delete from empty index
  auto record = CreateRecord(1);
  EXPECT_FALSE(index_->Delete(*record));
  
  // Insert some data
  auto record2 = CreateRecord(2);
  auto rid2    = CreateRID(2, 0);
  index_->Insert(*record2, rid2);
  
  // Try to delete non-existing key
  EXPECT_FALSE(index_->Delete(*record));
  
  // Verify existing data is still there
  auto results = index_->Search(*record2);
  EXPECT_EQ(results.size(), 1);
  EXPECT_EQ(index_->Size(), 1);
}

// Test batch delete operation
TEST_F(HashIndexTest, DeleteBatch)
{
  const int NUM_RECORDS = 500;

  // Insert test data
  for (int i = 0; i < NUM_RECORDS; ++i) {
    auto record = CreateRecord(i);
    auto rid    = CreateRID(i + 1, 0);
    EXPECT_NO_THROW(index_->Insert(*record, rid));
  }

  // Verify all records are inserted
  EXPECT_EQ(index_->Size(), NUM_RECORDS);
  for (int i = 0; i < NUM_RECORDS; ++i) {
    auto query_record = CreateRecord(i);
    auto results      = index_->Search(*query_record);
    EXPECT_EQ(results.size(), 1);
  }

  // Delete every other record
  for (int i = 0; i < NUM_RECORDS; i += 2) {
    auto record = CreateRecord(i);
    EXPECT_TRUE(index_->Delete(*record));
  }

  // Verify correct final size
  EXPECT_EQ(index_->Size(), NUM_RECORDS / 2);

  // Verify deleted records are gone and remaining records exist
  for (int i = 0; i < NUM_RECORDS; ++i) {
    auto query_record = CreateRecord(i);
    auto results      = index_->Search(*query_record);
    
    if (i % 2 == 0) {
      // Should be deleted
      EXPECT_TRUE(results.empty());
    } else {
      // Should still exist
      EXPECT_EQ(results.size(), 1);
      EXPECT_EQ(results[0].PageID(), i + 1);
      EXPECT_EQ(results[0].SlotID(), 0);
    }
  }
}

// Test range search (note: hash index range search is inefficient)
TEST_F(HashIndexTest, SearchRange)
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

  // Verify all results are within range
  std::vector<int> found_keys;
  for (const auto &result : results) {
    // Extract key from result (page_id - 1) * 2
    int key = (result.PageID() - 1) * 2;
    found_keys.push_back(key);
    EXPECT_GE(key, 5);
    EXPECT_LE(key, 15);
  }

  // Verify expected keys are found
  std::sort(found_keys.begin(), found_keys.end());
  std::vector<int> expected_keys = {6, 8, 10, 12, 14};
  EXPECT_EQ(found_keys, expected_keys);
}

// Test range search with empty result
TEST_F(HashIndexTest, SearchRangeEmpty)
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

// Test range search on empty index
TEST_F(HashIndexTest, SearchRangeEmptyIndex)
{
  auto start_record = CreateRecord(1);
  auto end_record   = CreateRecord(10);

  auto results = index_->SearchRange(*start_record, *end_record);
  EXPECT_TRUE(results.empty());
}

// Test duplicate key handling
TEST_F(HashIndexTest, DuplicateKeys)
{
  auto record = CreateRecord(1);
  auto rid1   = CreateRID(1, 0);
  auto rid2   = CreateRID(2, 0);

  // Insert same key with different RIDs
  EXPECT_NO_THROW(index_->Insert(*record, rid1));
  EXPECT_NO_THROW(index_->Insert(*record, rid2));

  // Should find both RIDs
  auto results = index_->Search(*record);
  EXPECT_EQ(results.size(), 2);
  
  // Verify both RIDs are found
  bool found_rid1 = false, found_rid2 = false;
  for (const auto &result : results) {
    if (result.PageID() == 1 && result.SlotID() == 0) {
      found_rid1 = true;
    } else if (result.PageID() == 2 && result.SlotID() == 0) {
      found_rid2 = true;
    }
  }
  EXPECT_TRUE(found_rid1);
  EXPECT_TRUE(found_rid2);
  
  // Size should be 2
  EXPECT_EQ(index_->Size(), 2);
}

// Test clear operation
TEST_F(HashIndexTest, Clear)
{
  // Insert some test data
  for (int i = 0; i < 10; ++i) {
    auto record = CreateRecord(i);
    auto rid    = CreateRID(i + 1, 0);
    EXPECT_NO_THROW(index_->Insert(*record, rid));
  }

  // Verify data exists
  EXPECT_EQ(index_->Size(), 10);
  EXPECT_FALSE(index_->IsEmpty());

  // Clear the index
  index_->Clear();

  // Verify index is empty
  EXPECT_EQ(index_->Size(), 0);
  EXPECT_TRUE(index_->IsEmpty());

  // Verify no data can be found
  for (int i = 0; i < 10; ++i) {
    auto query_record = CreateRecord(i);
    auto results      = index_->Search(*query_record);
    EXPECT_TRUE(results.empty());
  }
}

// Test mixed operations (insert, delete, search)
TEST_F(HashIndexTest, MixedOperations)
{
  const int NUM_OPERATIONS = 1000;
  std::unordered_map<int, int> key_counts;  // Track how many times each key is inserted
  std::random_device rd;
  std::mt19937 g(rd());
  std::uniform_int_distribution<int> key_dist(1, 500);
  std::uniform_int_distribution<int> op_dist(1, 100);

  for (int i = 0; i < NUM_OPERATIONS; ++i) {
    int key = key_dist(g);
    int op = op_dist(g);
    
    if (op <= 50) {  // 50% insert
      auto record = CreateRecord(key);
      auto rid = CreateRID(key, key_counts[key]);  // Use count as slot_id to make RIDs unique
      index_->Insert(*record, rid);
      key_counts[key]++;
    } else if (op <= 80) {  // 30% delete
      auto record = CreateRecord(key);
      bool deleted = index_->Delete(*record);
      if (deleted) {
        key_counts.erase(key);  // Remove all instances
      }
    } else {  // 20% search
      auto record = CreateRecord(key);
      auto results = index_->Search(*record);
      bool should_exist = key_counts.find(key) != key_counts.end();
      if (should_exist) {
        EXPECT_FALSE(results.empty());
        EXPECT_EQ(results.size(), key_counts[key]);
      } else {
        EXPECT_TRUE(results.empty());
      }
    }
  }

  // Calculate expected total size
  size_t expected_size = 0;
  for (const auto &[key, count] : key_counts) {
    expected_size += count;
  }

  // Final verification
  EXPECT_EQ(index_->Size(), expected_size);
  for (const auto &[key, count] : key_counts) {
    auto record = CreateRecord(key);
    auto results = index_->Search(*record);
    EXPECT_EQ(results.size(), count);
  }
}

// Test hash distribution
TEST_F(HashIndexTest, HashDistribution)
{
  const int NUM_RECORDS = 1000;
  
  // Insert sequential keys to test hash distribution
  for (int i = 0; i < NUM_RECORDS; ++i) {
    auto record = CreateRecord(i);
    auto rid = CreateRID(i + 1, 0);
    EXPECT_NO_THROW(index_->Insert(*record, rid));
  }
  
  // All records should be findable
  for (int i = 0; i < NUM_RECORDS; ++i) {
    auto query_record = CreateRecord(i);
    auto results = index_->Search(*query_record);
    EXPECT_EQ(results.size(), 1);
    EXPECT_EQ(results[0].PageID(), i + 1);
    EXPECT_EQ(results[0].SlotID(), 0);
  }
  
  EXPECT_EQ(index_->Size(), NUM_RECORDS);
}

// Test edge cases
TEST_F(HashIndexTest, EdgeCases)
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
  EXPECT_TRUE(index_->IsEmpty());
}

// Test with large dataset
TEST_F(HashIndexTest, LargeDataset)
{
  const int NUM_RECORDS = 10000;
  std::vector<int> keys;
  
  // Create keys
  for (int i = 0; i < NUM_RECORDS; ++i) {
    keys.push_back(i);
  }
  
  // Shuffle for random insertion order
  std::random_device rd;
  std::mt19937 g(rd());
  std::shuffle(keys.begin(), keys.end(), g);
  
  // Insert all records
  for (int key : keys) {
    auto record = CreateRecord(key);
    auto rid = CreateRID(key + 1, 0);
    EXPECT_NO_THROW(index_->Insert(*record, rid));
  }
  
  EXPECT_EQ(index_->Size(), NUM_RECORDS);
  
  // Verify all records can be found
  for (int i = 0; i < NUM_RECORDS; ++i) {
    auto query_record = CreateRecord(i);
    auto results = index_->Search(*query_record);
    EXPECT_EQ(results.size(), 1);
    EXPECT_EQ(results[0].PageID(), i + 1);
    EXPECT_EQ(results[0].SlotID(), 0);
  }
  
  // Test iterator with large dataset
  auto iter = index_->Begin();
  std::vector<int> scanned_keys;
  
  while (iter->IsValid()) {
    auto key_record = iter->GetKey();
    int key = ExtractKey(key_record);
    scanned_keys.push_back(key);
    iter->Next();
  }
  
  EXPECT_EQ(scanned_keys.size(), NUM_RECORDS);
  
  // Verify all keys are present
  std::sort(scanned_keys.begin(), scanned_keys.end());
  for (int i = 0; i < NUM_RECORDS; ++i) {
    EXPECT_EQ(scanned_keys[i], i);
  }
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
