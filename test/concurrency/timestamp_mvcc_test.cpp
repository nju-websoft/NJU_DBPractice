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
// Created for Lab05 Phase 4.2 - Timestamp Management and MVCC Integration Tests
//

#include <gtest/gtest.h>
#include "concurrency/timestamp_oracle.h"
#include "concurrency/txn_manager.h"
#include "concurrency/version_manager.h"
#include "concurrency/lock_manager.h"
#include "log/log_manager.h"

namespace njudb {

//===--------------------------------------------------------------------===//
// TimestampOracle Tests
//===--------------------------------------------------------------------===//

TEST(TimestampOracleTest, MonotonicTimestamps) {
  TimestampOracle oracle;

  timestamp_t current = oracle.GetCurrentTimestamp();
  EXPECT_EQ(oracle.GetCurrentTimestamp(), current);
  
  timestamp_t ts1 = oracle.GetNextTimestamp();
  EXPECT_EQ(ts1, current);
  timestamp_t current_after_ts1 = oracle.GetCurrentTimestamp();
  EXPECT_GT(current_after_ts1, ts1);
  EXPECT_EQ(oracle.GetCurrentTimestamp(), current_after_ts1);

  timestamp_t ts2 = oracle.GetNextTimestamp();
  timestamp_t ts3 = oracle.GetNextTimestamp();
  
  EXPECT_LT(ts1, ts2);
  EXPECT_LT(ts2, ts3);
}

TEST(TimestampOracleTest, RegisterAndUnregister) {
  TimestampOracle oracle;
  
  EXPECT_EQ(oracle.GetActiveTransactionCount(), 0);
  
  timestamp_t ts1 = oracle.GetNextTimestamp();
  timestamp_t ts2 = oracle.GetNextTimestamp();
  
  oracle.RegisterTransaction(ts1);
  EXPECT_EQ(oracle.GetActiveTransactionCount(), 1);
  
  oracle.RegisterTransaction(ts2);
  EXPECT_EQ(oracle.GetActiveTransactionCount(), 2);
  
  oracle.UnregisterTransaction(ts1);
  EXPECT_EQ(oracle.GetActiveTransactionCount(), 1);
  
  oracle.UnregisterTransaction(ts2);
  EXPECT_EQ(oracle.GetActiveTransactionCount(), 0);
}

TEST(TimestampOracleTest, Watermark) {
  TimestampOracle oracle;
  
  oracle.GetNextTimestamp();  // 1
  timestamp_t ts2 = oracle.GetNextTimestamp();  // 2
  timestamp_t ts3 = oracle.GetNextTimestamp();  // 3
  
  // No active transactions: watermark should be current timestamp
  timestamp_t watermark = oracle.GetWatermark();
  EXPECT_GE(watermark, ts3);
  
  // Register out of timestamp order.
  oracle.RegisterTransaction(ts3);  // Active: ts3
  oracle.RegisterTransaction(ts2);  // Active: ts2, ts3

  // Watermark should be minimum active (ts2), regardless of registration order.
  watermark = oracle.GetWatermark();
  EXPECT_EQ(watermark, ts2);
  
  // Unregister ts2
  oracle.UnregisterTransaction(ts2);  // Active: ts3
  
  // Watermark should now be ts3
  watermark = oracle.GetWatermark();
  EXPECT_EQ(watermark, ts3);

  // After all transactions finish, watermark should return to current timestamp.
  oracle.UnregisterTransaction(ts3);
  EXPECT_EQ(oracle.GetActiveTransactionCount(), 0);
  EXPECT_EQ(oracle.GetWatermark(), oracle.GetCurrentTimestamp());
}

//===--------------------------------------------------------------------===//
// TxnManager with TimestampOracle Tests
//===--------------------------------------------------------------------===//

TEST(TxnManagerMVCCTest, TimestampAssignment) {
  LogManager *log_mgr = nullptr;
  LockManager lock_mgr;
  TimestampOracle ts_oracle;
  TxnManager txn_mgr(log_mgr, &lock_mgr, &ts_oracle);
  
  // Begin MVCC transaction
  txn_mgr.Begin(0, IsolationLevel::REPEATABLE_READ, ConcurrencyProtocol::MVCC);
  auto *txn = txn_mgr.GetTransaction(0);
  
  ASSERT_NE(txn, nullptr);
  EXPECT_GT(txn->GetTimestamp(), 0);
  EXPECT_GT(txn->GetReadTimestamp(), 0);
  EXPECT_EQ(txn->GetTimestamp(), txn->GetReadTimestamp());
  
  // Verify registered with timestamp oracle
  EXPECT_EQ(ts_oracle.GetActiveTransactionCount(), 1);

  txn_mgr.Commit(0);
  EXPECT_EQ(ts_oracle.GetActiveTransactionCount(), 0);
}

TEST(TxnManagerMVCCTest, TimestampAssignmentRequiresOracleForMVCC) {
  LogManager *log_mgr = nullptr;
  LockManager lock_mgr;
  TxnManager txn_mgr(log_mgr, &lock_mgr, nullptr);

  EXPECT_THROW(
      txn_mgr.Begin(0, IsolationLevel::REPEATABLE_READ, ConcurrencyProtocol::MVCC), NJUDBException_);
  EXPECT_EQ(txn_mgr.GetTransaction(0), nullptr);
}

TEST(TxnManagerMVCCTest, SharedManagerSupportsDifferentDatabaseProtocols) {
  LogManager *log_mgr = nullptr;
  LockManager lock_mgr;
  TimestampOracle ts_oracle;
  TxnManager txn_mgr(log_mgr, &lock_mgr, &ts_oracle);

  txn_mgr.Begin(0, IsolationLevel::REPEATABLE_READ, ConcurrencyProtocol::MVCC);
  txn_mgr.Begin(1, IsolationLevel::REPEATABLE_READ, ConcurrencyProtocol::TWO_PHASE_LOCKING);

  ASSERT_NE(txn_mgr.GetTransaction(0), nullptr);
  ASSERT_NE(txn_mgr.GetTransaction(1), nullptr);
  EXPECT_EQ(txn_mgr.GetTransaction(0)->GetProtocol(), ConcurrencyProtocol::MVCC);
  EXPECT_EQ(txn_mgr.GetTransaction(1)->GetProtocol(), ConcurrencyProtocol::TWO_PHASE_LOCKING);
  EXPECT_EQ(ts_oracle.GetActiveTransactionCount(), 1);

  txn_mgr.Commit(1);
  EXPECT_EQ(ts_oracle.GetActiveTransactionCount(), 1);
  txn_mgr.Commit(0);
  EXPECT_EQ(ts_oracle.GetActiveTransactionCount(), 0);
}

TEST(TxnManagerMVCCTest, CommitTimestampAssignment) {
  LogManager *log_mgr = nullptr;
  LockManager lock_mgr;
  TimestampOracle ts_oracle;
  TxnManager txn_mgr(log_mgr, &lock_mgr, &ts_oracle);
  
  RID test_rid(1, 0);
  VersionManager version_mgr;
  
  // Begin MVCC transaction
  txn_mgr.Begin(0, IsolationLevel::REPEATABLE_READ, ConcurrencyProtocol::MVCC);
  auto *txn = txn_mgr.GetTransaction(0);
  
  timestamp_t read_ts = txn->GetReadTimestamp();
  
  // Create a version so we can check its commit timestamp after commit
  auto *version = version_mgr.CreateVersion(txn, test_rid, nullptr);
  EXPECT_NE(version, nullptr);
  EXPECT_EQ(version->GetCommitTimestamp(), MAX_TS);  // Uncommitted
  
  // Commit transaction (note: txn is deleted after this)
  txn_mgr.Commit(0);
  
  // Verify commit timestamp assigned and greater than read timestamp
  // We check the version's commit timestamp instead of the deleted transaction
  timestamp_t commit_ts = version->GetCommitTimestamp();
  EXPECT_GT(commit_ts, 0);
  EXPECT_GT(commit_ts, read_ts);
  
  // Verify unregistered from timestamp oracle
  EXPECT_EQ(ts_oracle.GetActiveTransactionCount(), 0);
}

TEST(TxnManagerMVCCTest, AbortUnregisters) {
  LogManager *log_mgr = nullptr;
  LockManager lock_mgr;
  TimestampOracle ts_oracle;
  TxnManager txn_mgr(log_mgr, &lock_mgr, &ts_oracle);
  
  // Begin MVCC transaction
  txn_mgr.Begin(0, IsolationLevel::REPEATABLE_READ, ConcurrencyProtocol::MVCC);
  
  EXPECT_EQ(ts_oracle.GetActiveTransactionCount(), 1);
  
  // Abort transaction
  txn_mgr.Abort(0);
  
  // Verify unregistered from timestamp oracle
  EXPECT_EQ(ts_oracle.GetActiveTransactionCount(), 0);
}

TEST(TxnManagerMVCCTest, MultipleTransactionOrdering) {
  LogManager *log_mgr = nullptr;
  LockManager lock_mgr;
  TimestampOracle ts_oracle;
  TxnManager txn_mgr(log_mgr, &lock_mgr, &ts_oracle);
  
  // Begin multiple transactions
  txn_mgr.Begin(0, IsolationLevel::REPEATABLE_READ, ConcurrencyProtocol::MVCC);
  txn_mgr.Begin(1, IsolationLevel::REPEATABLE_READ, ConcurrencyProtocol::MVCC);
  txn_mgr.Begin(2, IsolationLevel::REPEATABLE_READ, ConcurrencyProtocol::MVCC);
  
  auto *txn0 = txn_mgr.GetTransaction(0);
  auto *txn1 = txn_mgr.GetTransaction(1);
  auto *txn2 = txn_mgr.GetTransaction(2);
  
  // Timestamps should be monotonically increasing
  EXPECT_LT(txn0->GetReadTimestamp(), txn1->GetReadTimestamp());
  EXPECT_LT(txn1->GetReadTimestamp(), txn2->GetReadTimestamp());
  
  // All registered
  EXPECT_EQ(ts_oracle.GetActiveTransactionCount(), 3);
  
  // Watermark should be txn0's timestamp
  EXPECT_EQ(ts_oracle.GetWatermark(), txn0->GetReadTimestamp());
  
  // Commit txn0
  txn_mgr.Commit(0);
  EXPECT_EQ(ts_oracle.GetActiveTransactionCount(), 2);
  
  // Watermark should now be txn1's timestamp
  EXPECT_EQ(ts_oracle.GetWatermark(), txn1->GetReadTimestamp());
  
  // Clean up
  txn_mgr.Commit(1);
  txn_mgr.Commit(2);
}

//===--------------------------------------------------------------------===//
// VersionManager Integration Tests
//===--------------------------------------------------------------------===//

TEST(VersionManagerTest, CreateAndRetrieveVersion) {
  VersionManager version_mgr;
  LogManager *log_mgr = nullptr;
  LockManager lock_mgr;
  TimestampOracle ts_oracle;
  TxnManager txn_mgr(log_mgr, &lock_mgr, &ts_oracle);
  
  RID rid(1, 0);
  
  // Begin transaction
  txn_mgr.Begin(0, IsolationLevel::REPEATABLE_READ, ConcurrencyProtocol::MVCC);
  auto *txn = txn_mgr.GetTransaction(0);
  
  // Create version
  auto *undo_record = version_mgr.CreateVersion(txn, rid, nullptr);
  ASSERT_NE(undo_record, nullptr);
  
  // Verify version tracked in transaction
  EXPECT_EQ(txn->GetUndoRecords().size(), 1);
  EXPECT_EQ(txn->GetUndoRecords()[0], undo_record);
  
  // Verify version chain exists
  EXPECT_TRUE(version_mgr.HasVersionChain(rid));
  
  // Commit to make version visible
  undo_record->Commit(txn->GetCommitTimestamp());
  txn_mgr.Commit(0);
  
  // Retrieve visible version (from later transaction)
  txn_mgr.Begin(1, IsolationLevel::REPEATABLE_READ, ConcurrencyProtocol::MVCC);
  auto *txn1 = txn_mgr.GetTransaction(1);
  
  auto visible = version_mgr.ResolveVisibility(rid, txn1->GetReadTimestamp(), txn1->GetTxnId());
  EXPECT_EQ(visible.state, VersionVisibilityState::READ_PAGE);
  
  txn_mgr.Commit(1);
}

TEST(VersionManagerTest, CreateDeleteVersion) {
  VersionManager version_mgr;
  LogManager *log_mgr = nullptr;
  LockManager lock_mgr;
  TimestampOracle ts_oracle;
  TxnManager txn_mgr(log_mgr, &lock_mgr, &ts_oracle);
  
  RID rid(1, 0);
  
  // Create initial version
  txn_mgr.Begin(0, IsolationLevel::REPEATABLE_READ, ConcurrencyProtocol::MVCC);
  auto *txn0 = txn_mgr.GetTransaction(0);
  auto *v1 = version_mgr.CreateVersion(txn0, rid, nullptr);
  
  // Manually commit v1 at ts=100 (don't use txn_mgr.Commit to control exact timestamp)
  v1->Commit(100);
  
  // Create delete version
  txn_mgr.Begin(1, IsolationLevel::REPEATABLE_READ, ConcurrencyProtocol::MVCC);
  auto *txn1 = txn_mgr.GetTransaction(1);
  auto *delete_marker = version_mgr.CreateDeleteVersion(txn1, rid, nullptr);
  
  EXPECT_TRUE(delete_marker->IsDeleted());
  
  // Manually commit delete marker at ts=200
  delete_marker->Commit(200);
  
  // This synthetic chain stores nullptr old tuples, so it cannot reconstruct
  // the pre-delete value. The real table path always supplies the old tuple.
  auto before_delete = version_mgr.ResolveVisibility(rid, 150, 999);
  EXPECT_EQ(before_delete.state, VersionVisibilityState::NOT_VISIBLE);

  auto after_delete = version_mgr.ResolveVisibility(rid, 250, 999);
  EXPECT_EQ(after_delete.state, VersionVisibilityState::NOT_VISIBLE);
}

TEST(VersionManagerTest, Statistics) {
  VersionManager version_mgr;
  
  EXPECT_EQ(version_mgr.GetVersionChainCount(), 0);
  EXPECT_EQ(version_mgr.GetTotalVersionCount(), 0);
  
  // Create some version chains
  LogManager *log_mgr = nullptr;
  LockManager lock_mgr;
  TimestampOracle ts_oracle;
  TxnManager txn_mgr(log_mgr, &lock_mgr, &ts_oracle);
  
  // Create first version for RID(1, 0)
  txn_mgr.Begin(0, IsolationLevel::REPEATABLE_READ, ConcurrencyProtocol::MVCC);
  auto *txn0 = txn_mgr.GetTransaction(0);
  version_mgr.CreateVersion(txn0, RID(1, 0), nullptr);
  txn_mgr.Commit(0);
  
  // Create version for RID(1, 1)
  txn_mgr.Begin(1, IsolationLevel::REPEATABLE_READ, ConcurrencyProtocol::MVCC);
  auto *txn1 = txn_mgr.GetTransaction(1);
  version_mgr.CreateVersion(txn1, RID(1, 1), nullptr);
  txn_mgr.Commit(1);
  
  // Create second version for RID(1, 0) - now that first is committed
  txn_mgr.Begin(2, IsolationLevel::REPEATABLE_READ, ConcurrencyProtocol::MVCC);
  auto *txn2 = txn_mgr.GetTransaction(2);
  version_mgr.CreateVersion(txn2, RID(1, 0), nullptr);
  txn_mgr.Commit(2);
  
  EXPECT_EQ(version_mgr.GetVersionChainCount(), 2);  // Two unique RIDs
  EXPECT_EQ(version_mgr.GetTotalVersionCount(), 3);   // Three total versions
}

TEST(VersionManagerTest, PruneVersions) {
  VersionManager version_mgr;
  LogManager *log_mgr = nullptr;
  LockManager lock_mgr;
  TimestampOracle ts_oracle;
  TxnManager txn_mgr(log_mgr, &lock_mgr, &ts_oracle);
  
  RID rid(1, 0);
  
  // Create multiple versions at different timestamps
  for (int i = 0; i < 5; i++) {
    txn_mgr.Begin(i, IsolationLevel::REPEATABLE_READ, ConcurrencyProtocol::MVCC);
    auto *txn = txn_mgr.GetTransaction(i);
    auto *undo = version_mgr.CreateVersion(txn, rid, nullptr);
    undo->Commit(100 + i * 100);
    txn_mgr.Commit(i);
  }
  
  EXPECT_EQ(version_mgr.GetTotalVersionCount(), 5);
  
  // Prune versions older than timestamp 300
  size_t pruned = version_mgr.PruneVersions(300);
  
  // Should have pruned some versions
  EXPECT_GT(pruned, 0);
  EXPECT_LT(version_mgr.GetTotalVersionCount(), 5);
}

//===--------------------------------------------------------------------===//
// Integration Test: Complete MVCC Workflow
//===--------------------------------------------------------------------===//

TEST(MVCCIntegrationTest, SnapshotIsolationSimulation) {
  VersionManager version_mgr;
  LogManager *log_mgr = nullptr;
  LockManager lock_mgr;
  TimestampOracle ts_oracle;
  TxnManager txn_mgr(log_mgr, &lock_mgr, &ts_oracle);
  
  RID rid(1, 0);
  
  // T1: Begin, create version v1, commit
  txn_mgr.Begin(1, IsolationLevel::REPEATABLE_READ, ConcurrencyProtocol::MVCC);
  auto *t1 = txn_mgr.GetTransaction(1);
  auto *v1 = version_mgr.CreateVersion(t1, rid, nullptr);
  timestamp_t t1_commit = ts_oracle.GetNextTimestamp();
  v1->Commit(t1_commit);
  txn_mgr.Commit(1);
  
  // T2: Begin (should read v1)
  txn_mgr.Begin(2, IsolationLevel::REPEATABLE_READ, ConcurrencyProtocol::MVCC);
  auto *t2 = txn_mgr.GetTransaction(2);
  timestamp_t t2_start = t2->GetReadTimestamp();
  
  auto visible = version_mgr.ResolveVisibility(rid, t2_start, t2->GetTxnId());
  EXPECT_EQ(visible.state, VersionVisibilityState::READ_PAGE);
  
  // T3: Begin, create version v2, commit
  txn_mgr.Begin(3, IsolationLevel::REPEATABLE_READ, ConcurrencyProtocol::MVCC);
  auto *t3 = txn_mgr.GetTransaction(3);
  auto *v2 = version_mgr.CreateVersion(t3, rid, nullptr);
  timestamp_t t3_commit = ts_oracle.GetNextTimestamp();
  v2->Commit(t3_commit);
  txn_mgr.Commit(3);
  
  visible = version_mgr.ResolveVisibility(rid, t2_start, t2->GetTxnId());
  EXPECT_EQ(visible.state, VersionVisibilityState::NOT_VISIBLE);
  
  txn_mgr.Commit(2);
  
  // T4: Begin (should read v2)
  txn_mgr.Begin(4, IsolationLevel::REPEATABLE_READ, ConcurrencyProtocol::MVCC);
  auto *t4 = txn_mgr.GetTransaction(4);
  
  visible = version_mgr.ResolveVisibility(rid, t4->GetReadTimestamp(), t4->GetTxnId());
  EXPECT_EQ(visible.state, VersionVisibilityState::READ_PAGE);
  
  txn_mgr.Commit(4);
}

}  // namespace njudb

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
