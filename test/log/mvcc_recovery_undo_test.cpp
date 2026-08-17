//===--------------------------------------------------------------------===//
//
// Test: MVCC Recovery Undo Test
//
// Purpose: Verify that MVCC transactions are properly handled during recovery
//
// ACID Issue Coverage: Issue #7 - MVCC transactions not rolled back after crash
//
// Note: These are simplified unit tests that verify the recovery logic processes
// MVCC transactions correctly by checking the Active Transaction Table (ATT).
// Full integration testing with physical undo requires the complete database 
// infrastructure (TableManager, IndexManager, DatabaseHandle) and is covered
// in test/log/recovery_test.cpp
//
//===--------------------------------------------------------------------===//

#include <gtest/gtest.h>
#include <filesystem>
#include "log/recovery.h"
#include "log/log_manager.h"
#include "storage/disk/disk_manager.h"
#include "storage/buffer/buffer_pool_manager.h"
#include "concurrency/txn_manager.h"
#include "concurrency/timestamp_oracle.h"

namespace fs = std::filesystem;
using namespace njudb;

class MVCCRecoveryTest : public ::testing::Test {
 protected:
  void SetUp() override {
    test_dir_ = fs::temp_directory_path() / "mvcc_recovery_test";
    fs::create_directories(test_dir_);
    
    test_log_path_ = test_dir_ / "test.log";
    test_db_path_ = test_dir_ / "test.db";
    
    // Clean up any existing files
    if (fs::exists(test_log_path_)) {
      fs::remove(test_log_path_);
    }
    if (fs::exists(test_db_path_)) {
      fs::remove(test_db_path_);
    }
  }
  
  void TearDown() override {
    if (fs::exists(test_dir_)) {
      fs::remove_all(test_dir_);
    }
  }
  
  fs::path test_dir_;
  fs::path test_log_path_;
  fs::path test_db_path_;
};

//===--------------------------------------------------------------------===//
// Test 1: Verify MVCC Transactions Are In ATT After Analysis
//===--------------------------------------------------------------------===//

TEST_F(MVCCRecoveryTest, MVCCTransactionInATT) {
  // This test verifies that MVCC transactions are properly tracked in ATT
  // and that the recovery logic processes them
  
  {
    DiskManager disk_manager;
    LogManager log_manager(&disk_manager, test_log_path_);
    BufferPoolManager bpm(&disk_manager, &log_manager);
    TimestampOracle timestamp_oracle;
  TxnManager txn_manager(&log_manager, nullptr, &timestamp_oracle);
    
    // Start MVCC transaction
    txn_manager.Begin(1, IsolationLevel::REPEATABLE_READ,
                     ConcurrencyProtocol::MVCC);
    auto *txn = txn_manager.GetTransaction(1);
    ASSERT_NE(txn, nullptr);
    
    // Log BEGIN is automatic, no COMMIT (simulate crash)
    log_manager.FlushLog();
  }
  
  // Phase 2: Recovery - verify MVCC transaction in ATT
  {
    DiskManager disk_manager;
    LogManager log_manager(&disk_manager, test_log_path_);
    BufferPoolManager bpm(&disk_manager, &log_manager);
    TimestampOracle timestamp_oracle;
  TxnManager txn_manager(&log_manager, nullptr, &timestamp_oracle);
    
    // Create recovery manager
    Recovery recovery(&disk_manager, &bpm, &log_manager, &txn_manager);
    
    // Run analysis phase only
    recovery.AnalyzeLog();
    
    // Verify: MVCC transaction is in ATT
    const auto &att = recovery.GetATT();
    EXPECT_EQ(att.size(), 1) << "Uncommitted MVCC transaction should be in ATT";
    
    auto it = att.find(1);
    ASSERT_NE(it, att.end());
    EXPECT_EQ(it->second.protocol_, ConcurrencyProtocol::MVCC)
      << "Transaction protocol should be preserved in ATT";
  }
}

//===--------------------------------------------------------------------===//
// Test 2: Both 2PL and MVCC Transactions in ATT
//===--------------------------------------------------------------------===//

TEST_F(MVCCRecoveryTest, MixedProtocolsInATT) {
  {
    DiskManager disk_manager;
    LogManager log_manager(&disk_manager, test_log_path_);
    BufferPoolManager bpm(&disk_manager, &log_manager);
    TimestampOracle timestamp_oracle;
  TxnManager txn_manager(&log_manager, nullptr, &timestamp_oracle);
    
    // Start 2PL transaction
    txn_manager.Begin(1, IsolationLevel::REPEATABLE_READ,
                     ConcurrencyProtocol::TWO_PHASE_LOCKING);
    
    // Start MVCC transaction
    txn_manager.Begin(2, IsolationLevel::REPEATABLE_READ,
                     ConcurrencyProtocol::MVCC);
    
    // Commit 2PL transaction
    auto *txn1 = txn_manager.GetTransaction(1);
    LogRecord commit1(1, txn1->GetPrevLSN(), LogRecordType::COMMIT);
    log_manager.AppendLogRecord(commit1);
    txn_manager.Commit(1);
    
    // DON'T commit MVCC transaction
    log_manager.FlushLog();
  }
  
  // Recovery
  {
    DiskManager disk_manager;
    LogManager log_manager(&disk_manager, test_log_path_);
    BufferPoolManager bpm(&disk_manager, &log_manager);
    TimestampOracle timestamp_oracle;
  TxnManager txn_manager(&log_manager, nullptr, &timestamp_oracle);
    
    Recovery recovery(&disk_manager, &bpm, &log_manager, &txn_manager);
    recovery.AnalyzeLog();
    
    const auto &att = recovery.GetATT();
    
    // Only MVCC transaction should be in ATT
    EXPECT_EQ(att.size(), 1);
    EXPECT_NE(att.find(2), att.end()) << "Uncommitted MVCC txn should be in ATT";
    EXPECT_EQ(att.find(1), att.end()) << "Committed 2PL txn should not be in ATT";
  }
}

//===--------------------------------------------------------------------===//
// Test 3: Committed MVCC Transaction Not In ATT
//===--------------------------------------------------------------------===//

TEST_F(MVCCRecoveryTest, CommittedMVCCNotInATT) {
  {
    DiskManager disk_manager;
    LogManager log_manager(&disk_manager, test_log_path_);
    BufferPoolManager bpm(&disk_manager, &log_manager);
    TimestampOracle timestamp_oracle;
  TxnManager txn_manager(&log_manager, nullptr, &timestamp_oracle);
    
    // Start and commit MVCC transaction
    txn_manager.Begin(1, IsolationLevel::REPEATABLE_READ,
                     ConcurrencyProtocol::MVCC);
    auto *txn = txn_manager.GetTransaction(1);
    
    LogRecord commit(1, txn->GetPrevLSN(), LogRecordType::COMMIT);
    log_manager.AppendLogRecord(commit);
    txn_manager.Commit(1);
    
    log_manager.FlushLog();
  }
  
  // Recovery
  {
    DiskManager disk_manager;
    LogManager log_manager(&disk_manager, test_log_path_);
    BufferPoolManager bpm(&disk_manager, &log_manager);
    TimestampOracle timestamp_oracle;
  TxnManager txn_manager(&log_manager, nullptr, &timestamp_oracle);
    
    Recovery recovery(&disk_manager, &bpm, &log_manager, &txn_manager);
    recovery.AnalyzeLog();
    
    const auto &att = recovery.GetATT();
    
    // Committed transaction should NOT be in ATT
    EXPECT_EQ(att.size(), 0) << "Committed MVCC txn should not be in ATT";
  }
}

//===--------------------------------------------------------------------===//
// Test 4: Multiple Uncommitted MVCC Transactions
//===--------------------------------------------------------------------===//

TEST_F(MVCCRecoveryTest, MultipleUncommittedMVCC) {
  {
    DiskManager disk_manager;
    LogManager log_manager(&disk_manager, test_log_path_);
    BufferPoolManager bpm(&disk_manager, &log_manager);
    TimestampOracle timestamp_oracle;
  TxnManager txn_manager(&log_manager, nullptr, &timestamp_oracle);
    
    // Start 3 MVCC transactions, commit only 1
    txn_manager.Begin(1, IsolationLevel::REPEATABLE_READ,
                     ConcurrencyProtocol::MVCC);
    
    txn_manager.Begin(2, IsolationLevel::REPEATABLE_READ,
                     ConcurrencyProtocol::MVCC);
    
    txn_manager.Begin(3, IsolationLevel::REPEATABLE_READ,
                     ConcurrencyProtocol::MVCC);
    
    // Commit only txn 2
    auto *txn2 = txn_manager.GetTransaction(2);
    LogRecord commit(2, txn2->GetPrevLSN(), LogRecordType::COMMIT);
    log_manager.AppendLogRecord(commit);
    txn_manager.Commit(2);
    
    log_manager.FlushLog();
  }
  
  // Recovery
  {
    DiskManager disk_manager;
    LogManager log_manager(&disk_manager, test_log_path_);
    BufferPoolManager bpm(&disk_manager, &log_manager);
    TimestampOracle timestamp_oracle;
  TxnManager txn_manager(&log_manager, nullptr, &timestamp_oracle);
    
    Recovery recovery(&disk_manager, &bpm, &log_manager, &txn_manager);
    recovery.AnalyzeLog();
    
    const auto &att = recovery.GetATT();
    
    // Only uncommitted transactions should be in ATT
    EXPECT_EQ(att.size(), 2) << "Two uncommitted MVCC txns should be in ATT";
    EXPECT_NE(att.find(1), att.end()) << "Txn 1 should be in ATT";
    EXPECT_EQ(att.find(2), att.end()) << "Committed txn 2 should not be in ATT";
    EXPECT_NE(att.find(3), att.end()) << "Txn 3 should be in ATT";
  }
}
