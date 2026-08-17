/*------------------------------------------------------------------------------
 - Copyright (c) 2024. Websoft research group, Nanjing University.
 -
 - This program is free software: you can redistribute it and/or modify
 - it under the terms of the GNU General Public License as published by
 - the Free Software Foundation, either version 3 of the License, or
 - (at your option) any later version.
 -
 - This program is distributed in the hope that it will be useful,
 - but WITHOUT ANY WARRANTY; without even the implied warranty of
 - MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 - GNU General Public License for more details.
 -
 - You should have received a copy of the GNU General Public License
 - along with this program.  If not, see <https://www.gnu.org/licenses/>.
 -----------------------------------------------------------------------------*/

//
// Phase 4.3: Snapshot Isolation Integration Tests
// Tests end-to-end MVCC snapshot isolation semantics
//

#include <gtest/gtest.h>
#include <thread>
#include <chrono>
#include <filesystem>
#include "concurrency/txn_manager.h"
#include "concurrency/lock_manager.h"
#include "concurrency/version_manager.h"
#include "concurrency/timestamp_oracle.h"
#include "concurrency/transaction.h"
#include "log/log_manager.h"
#include "storage/buffer/buffer_pool_manager.h"
#include "storage/disk/disk_manager.h"
#include "system/table/table_manager.h"
#include "execution/executor_delete.h"
#include "execution/executor_insert.h"
#include "execution/executor_seqscan.h"
#include "execution/executor_update.h"
#include "execution/mvcc_write_coordinator.h"
#include "system/context.h"

namespace njudb {

/**
 * @brief Test fixture for snapshot isolation tests
 * 
 * Sets up MVCC concurrency control components for testing:
 * - LockManager (for write locks in MVCC)
 * - TimestampOracle (for timestamp generation)
 * - VersionManager (for version chain management)
 * - TxnManager (for transaction lifecycle)
 */
class SnapshotIsolationTest : public ::testing::Test {
protected:
  void SetUp() override {
    // Initialize concurrency control components
    lock_mgr_ = std::make_unique<LockManager>();
    ts_oracle_ = std::make_unique<TimestampOracle>();
    version_mgr_ = std::make_unique<VersionManager>();
    
    // Initialize transaction manager
    // LogManager is nullptr for these tests (not using logging)
    // VersionManager is not passed to TxnManager (accessed separately)
    txn_mgr_ = std::make_unique<TxnManager>(
      nullptr,  // log_manager
      lock_mgr_.get(),
      ts_oracle_.get()
    );
  }

  void TearDown() override {
    // Cleanup in reverse order
    txn_mgr_.reset();
    version_mgr_.reset();
    ts_oracle_.reset();
    lock_mgr_.reset();
  }

  // Helper: Create a simple test record (for version data)
  // Note: We use nullptr for most tests since we're focusing on MVCC logic, not record content
  auto CreateTestRecord() -> RecordUptr {
    return nullptr;  // Simplified for testing version chain logic
  }

  // Test infrastructure
  std::unique_ptr<LockManager> lock_mgr_;
  std::unique_ptr<TimestampOracle> ts_oracle_;
  std::unique_ptr<VersionManager> version_mgr_;
  std::unique_ptr<TxnManager> txn_mgr_;
};

/**
 * @brief Test 1: Basic Snapshot Isolation - Uncommitted Changes Invisible
 * 
 * T0: BEGIN, CREATE initial version, COMMIT
 * T1: BEGIN (MVCC)
 * T2: BEGIN (MVCC) - T2 starts after T1
 * T2: UPDATE (CREATE version v2)
 * T2: COMMIT (v2 becomes visible)
 * T1: Should NOT see v2 (snapshot isolation - T1 sees snapshot from its start time)
 * T1: COMMIT
 * T3: BEGIN - should see v2 (latest committed version)
 */
/**
 * @brief Test 2: Aborted Transactions Never Visible
 * 
 * T1: BEGIN, CREATE version
 * T1: ABORT
 * T2: BEGIN, READ -> should not see T1's aborted version
 */
/**
 * @brief Test 3: Multiple Concurrent Readers
 * 
 * All readers should see consistent snapshot without blocking each other
 */
/**
 * @brief Test 4: Version Chain Ordering
 * 
 * Ensure newer versions are found before older ones
 */
/**
 * @brief Test 5: Delete Markers
 * 
 * Test that delete operations create proper tombstone versions
 */
/**
 * @brief Test 6: Timestamp Monotonicity
 * 
 * Ensure timestamps are always increasing
 */
TEST_F(SnapshotIsolationTest, TimestampMonotonicity) {
  std::vector<timestamp_t> timestamps;
  
  // Create 10 transactions and collect their read timestamps
  for (int i = 0; i < 10; i++) {
    txn_mgr_->Begin(i, IsolationLevel::REPEATABLE_READ, ConcurrencyProtocol::MVCC);
    auto *txn = txn_mgr_->GetTransaction(i);
    timestamps.push_back(txn->GetReadTimestamp());
    txn_mgr_->Commit(i);
  }
  
  // Verify strictly increasing
  for (size_t i = 1; i < timestamps.size(); i++) {
    EXPECT_GT(timestamps[i], timestamps[i-1]);
  }
}

TEST(SnapshotIsolationVisibilityTest, UncommittedInsertIsNotVisible) {
  const std::string test_dir = "./test_mvcc_uncommitted_insert";
  const std::string table_name = "insert_visibility";
  std::filesystem::remove_all(test_dir);
  std::filesystem::create_directories(test_dir);

  DiskManager disk_manager;
  BufferPoolManager buffer_pool_manager(&disk_manager, nullptr);
  TableManager table_manager(&disk_manager, &buffer_pool_manager);
  VersionManager version_manager;
  TimestampOracle timestamp_oracle;
  TxnManager txn_manager(nullptr, nullptr, &timestamp_oracle);

  RTField value_field;
  value_field.field_.field_name_ = "value";
  value_field.field_.field_type_ = TYPE_INT;
  value_field.field_.field_size_ = sizeof(int32_t);
  RecordSchema schema({value_field});

  table_manager.CreateTable(test_dir, table_name, schema, NARY_MODEL);
  auto table = table_manager.OpenTable(test_dir, table_name, NARY_MODEL);
  table->SetVersionManager(&version_manager);

  txn_manager.Begin(1, IsolationLevel::REPEATABLE_READ, ConcurrencyProtocol::MVCC);
  auto *writer = txn_manager.GetTransaction(1);
  std::vector<ValueSptr> values{ValueFactory::CreateIntValue(100)};
  Record inserted(&table->GetSchema(), values, INVALID_RID);
  RID rid = MVCCWriteCoordinator::Insert(table.get(), {}, inserted, writer, &version_manager);

  txn_manager.Begin(2, IsolationLevel::REPEATABLE_READ, ConcurrencyProtocol::MVCC);
  auto *other_reader = txn_manager.GetTransaction(2);
  EXPECT_THROW(table->GetRecord(rid, other_reader), NJUDBException_);

  auto own_read = table->GetRecord(rid, writer);
  EXPECT_TRUE(inserted == *own_read);

  txn_manager.Commit(1);
  EXPECT_THROW(table->GetRecord(rid, other_reader), NJUDBException_);
  txn_manager.Commit(2);

  txn_manager.Begin(3, IsolationLevel::REPEATABLE_READ, ConcurrencyProtocol::MVCC);
  auto *new_reader = txn_manager.GetTransaction(3);
  auto committed_read = table->GetRecord(rid, new_reader);
  EXPECT_TRUE(inserted == *committed_read);
  txn_manager.Commit(3);

  table_manager.CloseTable(test_dir, *table);
  table.reset();
  table_manager.DropTable(test_dir, table_name);
  std::filesystem::remove_all(test_dir);
}

TEST(SnapshotIsolationVisibilityTest, MVCCReadRequiresVersionManager) {
  const std::string test_dir = "./test_mvcc_requires_version_manager";
  const std::string table_name = "missing_version_manager";
  std::filesystem::remove_all(test_dir);
  std::filesystem::create_directories(test_dir);

  DiskManager disk_manager;
  BufferPoolManager buffer_pool_manager(&disk_manager, nullptr);
  TableManager table_manager(&disk_manager, &buffer_pool_manager);
  TimestampOracle timestamp_oracle;
  TxnManager txn_manager(nullptr, nullptr, &timestamp_oracle);

  RTField value_field;
  value_field.field_.field_name_ = "value";
  value_field.field_.field_type_ = TYPE_INT;
  value_field.field_.field_size_ = sizeof(int32_t);
  RecordSchema schema({value_field});

  table_manager.CreateTable(test_dir, table_name, schema, NARY_MODEL);
  auto table = table_manager.OpenTable(test_dir, table_name, NARY_MODEL);
  std::vector<ValueSptr> values{ValueFactory::CreateIntValue(100)};
  Record record(&table->GetSchema(), values, INVALID_RID);
  auto rid = table->InsertRecord(record);

  txn_manager.Begin(1, IsolationLevel::REPEATABLE_READ, ConcurrencyProtocol::MVCC);
  auto *reader = txn_manager.GetTransaction(1);
  EXPECT_THROW(table->GetRecord(rid, reader), NJUDBException_);
  txn_manager.Commit(1);

  table_manager.CloseTable(test_dir, *table);
  table.reset();
  table_manager.DropTable(test_dir, table_name);
  std::filesystem::remove_all(test_dir);
}

TEST(SnapshotIsolationVisibilityTest, DeleteVisibilityUsesUndoTuple) {
  const std::string test_dir = "./test_mvcc_delete_visibility";
  const std::string table_name = "delete_visibility";
  std::filesystem::remove_all(test_dir);
  std::filesystem::create_directories(test_dir);

  DiskManager disk_manager;
  BufferPoolManager buffer_pool_manager(&disk_manager, nullptr);
  TableManager table_manager(&disk_manager, &buffer_pool_manager);
  VersionManager version_manager;
  TimestampOracle timestamp_oracle;
  TxnManager txn_manager(nullptr, nullptr, &timestamp_oracle);

  RTField value_field;
  value_field.field_.field_name_ = "value";
  value_field.field_.field_type_ = TYPE_INT;
  value_field.field_.field_size_ = sizeof(int32_t);
  RecordSchema schema({value_field});

  table_manager.CreateTable(test_dir, table_name, schema, NARY_MODEL);
  auto table = table_manager.OpenTable(test_dir, table_name, NARY_MODEL);
  table->SetVersionManager(&version_manager);

  std::vector<ValueSptr> values{ValueFactory::CreateIntValue(100)};
  Record original(&table->GetSchema(), values, INVALID_RID);
  RID rid = table->InsertRecord(original);

  txn_manager.Begin(1, IsolationLevel::REPEATABLE_READ, ConcurrencyProtocol::MVCC);
  auto *old_reader = txn_manager.GetTransaction(1);

  txn_manager.Begin(2, IsolationLevel::REPEATABLE_READ, ConcurrencyProtocol::MVCC);
  auto *deleter = txn_manager.GetTransaction(2);
  Record original_with_rid(original);
  original_with_rid.SetRID(rid);
  MVCCWriteCoordinator::Delete(table.get(), {}, original_with_rid, deleter, &version_manager);

  auto before_commit = table->GetRecord(rid, old_reader);
  EXPECT_TRUE(original == *before_commit);

  txn_manager.Commit(2);
  auto historical_read = table->GetRecord(rid, old_reader);
  EXPECT_TRUE(original == *historical_read);

  txn_manager.Begin(3, IsolationLevel::REPEATABLE_READ, ConcurrencyProtocol::MVCC);
  auto *new_reader = txn_manager.GetTransaction(3);
  EXPECT_THROW(table->GetRecord(rid, new_reader), NJUDBException_);

  txn_manager.Commit(1);
  txn_manager.Commit(3);
  table_manager.CloseTable(test_dir, *table);
  table.reset();
  table_manager.DropTable(test_dir, table_name);
  std::filesystem::remove_all(test_dir);
}

TEST(MVCCExecutorUndoRegistrationTest, EachWriteRegistersExactlyOneUndoRecord) {
  const std::string test_dir = "./test_mvcc_executor_undo_registration";
  const std::string table_name = "undo_registration";
  std::filesystem::remove_all(test_dir);
  std::filesystem::create_directories(test_dir);

  DiskManager disk_manager;
  BufferPoolManager buffer_pool_manager(&disk_manager, nullptr);
  TableManager table_manager(&disk_manager, &buffer_pool_manager);
  VersionManager version_manager;
  TimestampOracle timestamp_oracle;
  TxnManager txn_manager(nullptr, nullptr, &timestamp_oracle);

  RTField value_field;
  value_field.field_.field_name_ = "value";
  value_field.field_.field_type_ = TYPE_INT;
  value_field.field_.field_size_ = sizeof(int32_t);
  RecordSchema schema({value_field});

  table_manager.CreateTable(test_dir, table_name, schema, NARY_MODEL);
  auto table = table_manager.OpenTable(test_dir, table_name, NARY_MODEL);
  table->SetVersionManager(&version_manager);

  txn_manager.Begin(1, IsolationLevel::REPEATABLE_READ, ConcurrencyProtocol::MVCC);
  auto *insert_txn = txn_manager.GetTransaction(1);
  Context insert_context(insert_txn, nullptr, nullptr, nullptr, -1, nullptr, &version_manager, &timestamp_oracle);
  std::vector<RecordUptr> inserts;
  inserts.push_back(std::make_unique<Record>(
      &table->GetSchema(), std::vector<ValueSptr>{ValueFactory::CreateIntValue(100)}, INVALID_RID));
  InsertExecutor insert_executor(table.get(), {}, std::move(inserts), &insert_context);
  insert_executor.Init();
  ASSERT_EQ(insert_txn->GetUndoRecords().size(), 1);
  txn_manager.Commit(1);

  txn_manager.Begin(2, IsolationLevel::REPEATABLE_READ, ConcurrencyProtocol::MVCC);
  auto *update_txn = txn_manager.GetTransaction(2);
  Context update_context(update_txn, nullptr, nullptr, nullptr, -1, nullptr, &version_manager, &timestamp_oracle);
  std::vector<std::pair<RTField, ValueSptr>> updates{
      {table->GetSchema().GetFieldAt(0), ValueFactory::CreateIntValue(200)}};
  UpdateExecutor update_executor(
      std::make_unique<SeqScanExecutor>(table.get(), &update_context), table.get(), {}, std::move(updates),
      &update_context);
  update_executor.Init();
  ASSERT_EQ(update_txn->GetUndoRecords().size(), 1);
  auto *update_undo = update_txn->GetUndoRecords().front();
  txn_manager.Abort(2);
  EXPECT_EQ(update_undo->GetCommitTimestamp(), ABORTED_TS);

  table_manager.CloseTable(test_dir, *table);
  table.reset();
  table_manager.DropTable(test_dir, table_name);
  std::filesystem::remove_all(test_dir);
}

/**
 * @brief Test 8: Commit Timestamp Assignment
 * 
 * Verify commit timestamps are assigned correctly
 */
TEST_F(SnapshotIsolationTest, CommitTimestampAssignment) {
  RID test_rid(8, 0);
  
  txn_mgr_->Begin(0, IsolationLevel::REPEATABLE_READ, ConcurrencyProtocol::MVCC);
  auto *txn = txn_mgr_->GetTransaction(0);
  
  timestamp_t read_ts = txn->GetReadTimestamp();
  EXPECT_GT(read_ts, INVALID_TS);
  
  // Before commit, no commit timestamp
  EXPECT_EQ(txn->GetCommitTimestamp(), INVALID_TS);
  
  // Create a version so we can check its commit timestamp after transaction commits
  auto *version = version_mgr_->CreateVersion(txn, test_rid, nullptr);
  EXPECT_NE(version, nullptr);
  EXPECT_EQ(version->GetCommitTimestamp(), MAX_TS);  // Uncommitted
  
  // Commit
  txn_mgr_->Commit(0);
  
  // After commit, the version's commit timestamp should be assigned and greater than read timestamp
  // We check the version instead of the transaction since transaction is deleted after commit
  timestamp_t commit_ts = version->GetCommitTimestamp();
  EXPECT_GT(commit_ts, read_ts);
  EXPECT_NE(commit_ts, MAX_TS);
  EXPECT_NE(commit_ts, ABORTED_TS);
}

/**
 * @brief Test 9: Version Visibility With Multiple Updates
 * 
 * Complex scenario with multiple transactions updating the same record
 */
} // namespace njudb

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
