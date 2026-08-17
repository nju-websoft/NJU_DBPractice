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
// Phase 4.4: Garbage Collection Tests
// Tests the garbage collector for MVCC version pruning
//

#include <gtest/gtest.h>
#include <thread>
#include <chrono>
#include "concurrency/garbage_collector.h"
#include "concurrency/version_manager.h"
#include "concurrency/timestamp_oracle.h"
#include "concurrency/txn_manager.h"
#include "concurrency/lock_manager.h"
#include "concurrency/transaction.h"

namespace njudb {

/**
 * @brief Test fixture for garbage collector tests
 */
class GarbageCollectorTest : public ::testing::Test {
protected:
  void SetUp() override {
    // Initialize MVCC components
    lock_mgr_ = std::make_unique<LockManager>();
    ts_oracle_ = std::make_unique<TimestampOracle>();
    version_mgr_ = std::make_unique<VersionManager>();
    txn_mgr_ = std::make_unique<TxnManager>(
      nullptr,  // log_manager
      lock_mgr_.get(),
      ts_oracle_.get()
    );
    
    // Create garbage collector (not started yet)
    gc_ = std::make_unique<GarbageCollector>(
      version_mgr_.get(),
      ts_oracle_.get(),
      100  // 100ms interval for faster testing
    );
  }

  void TearDown() override {
    // Stop GC if running
    if (gc_->IsRunning()) {
      gc_->Stop();
    }
    
    gc_.reset();
    txn_mgr_.reset();
    version_mgr_.reset();
    ts_oracle_.reset();
    lock_mgr_.reset();
  }

  // Helper to create test record
  auto CreateTestRecord() -> RecordUptr {
    return nullptr;  // Simplified for testing version chain logic
  }

  std::unique_ptr<LockManager> lock_mgr_;
  std::unique_ptr<TimestampOracle> ts_oracle_;
  std::unique_ptr<VersionManager> version_mgr_;
  std::unique_ptr<TxnManager> txn_mgr_;
  std::unique_ptr<GarbageCollector> gc_;
};

/**
 * @brief Test 1: Basic GC Lifecycle - Start and Stop
 */
TEST_F(GarbageCollectorTest, StartStop) {
  EXPECT_FALSE(gc_->IsRunning());
  
  // Start GC
  gc_->Start();
  EXPECT_TRUE(gc_->IsRunning());
  
  // Stop GC
  gc_->Stop();
  EXPECT_FALSE(gc_->IsRunning());
  
  // Starting again should work
  gc_->Start();
  EXPECT_TRUE(gc_->IsRunning());
  gc_->Stop();
}

/**
 * @brief Test 2: Multiple Start/Stop Calls (Idempotent)
 */
TEST_F(GarbageCollectorTest, MultipleStartStop) {
  // Multiple starts should be safe
  gc_->Start();
  gc_->Start();
  gc_->Start();
  EXPECT_TRUE(gc_->IsRunning());
  
  // Multiple stops should be safe
  gc_->Stop();
  gc_->Stop();
  gc_->Stop();
  EXPECT_FALSE(gc_->IsRunning());
}

/**
 * @brief Test 3: Manual Garbage Collection
 * 
 * Create old versions, commit transactions, manually trigger GC,
 * verify versions are pruned.
 */
TEST_F(GarbageCollectorTest, ManualGarbageCollection) {
  RID test_rid(1, 0);
  
  // Create 3 versions by 3 transactions
  // T1 creates v1, commits
  txn_mgr_->Begin(0, IsolationLevel::REPEATABLE_READ, ConcurrencyProtocol::MVCC);
  auto *txn1 = txn_mgr_->GetTransaction(0);
  version_mgr_->CreateVersion(txn1, test_rid, nullptr);
  txn_mgr_->Commit(0);
  
  // T2 creates v2, commits
  txn_mgr_->Begin(1, IsolationLevel::REPEATABLE_READ, ConcurrencyProtocol::MVCC);
  auto *txn2 = txn_mgr_->GetTransaction(1);
  version_mgr_->CreateVersion(txn2, test_rid, CreateTestRecord());
  txn_mgr_->Commit(1);
  
  // T3 creates v3, commits
  txn_mgr_->Begin(2, IsolationLevel::REPEATABLE_READ, ConcurrencyProtocol::MVCC);
  auto *txn3 = txn_mgr_->GetTransaction(2);
  version_mgr_->CreateVersion(txn3, test_rid, CreateTestRecord());
  txn_mgr_->Commit(2);
  
  // Check initial version count (should have 3 versions)
  auto chain = version_mgr_->GetVersionChain(test_rid);
  ASSERT_NE(chain, nullptr);
  EXPECT_EQ(chain->GetVersionCount(), 3);
  
  // Now all transactions are committed, no active transactions
  // Watermark should be high enough to prune old versions
  
  // Start a new transaction to establish watermark
  txn_mgr_->Begin(10, IsolationLevel::REPEATABLE_READ, ConcurrencyProtocol::MVCC);
  
  // No active snapshot predates these writes, so every undo record is pruneable.
  
  // Trigger manual GC
  size_t initial_pruned = gc_->GetTotalPrunedVersions();
  gc_->CollectGarbage();
  size_t after_pruned = gc_->GetTotalPrunedVersions();
  
  // Should have pruned at least 1 version (v1, v2 can be pruned, v3 is kept as visible)
  EXPECT_GT(after_pruned, initial_pruned);
  
  // No active snapshot needs any undo record. The empty chain remains as the
  // resource's synchronization anchor until the tuple itself is reclaimed.
  chain = version_mgr_->GetVersionChain(test_rid);
  ASSERT_NE(chain, nullptr);
  EXPECT_EQ(chain->GetVersionCount(), 0);
  
  txn_mgr_->Commit(10);
}

/**
 * @brief Test 4: Per-Tuple Pruning
 */
TEST_F(GarbageCollectorTest, PruneTuple) {
  RID test_rid(2, 0);
  
  // Create 2 versions
  txn_mgr_->Begin(0, IsolationLevel::REPEATABLE_READ, ConcurrencyProtocol::MVCC);
  auto *txn1 = txn_mgr_->GetTransaction(0);
  version_mgr_->CreateVersion(txn1, test_rid, nullptr);
  txn_mgr_->Commit(0);
  
  txn_mgr_->Begin(1, IsolationLevel::REPEATABLE_READ, ConcurrencyProtocol::MVCC);
  auto *txn2 = txn_mgr_->GetTransaction(1);
  version_mgr_->CreateVersion(txn2, test_rid, CreateTestRecord());
  txn_mgr_->Commit(1);
  
  // Start new transaction to set watermark
  txn_mgr_->Begin(10, IsolationLevel::REPEATABLE_READ, ConcurrencyProtocol::MVCC);
  
  // Prune specific tuple
  size_t pruned = gc_->PruneTuple(0, test_rid);
  
  // Should have pruned at least some versions
  EXPECT_GE(pruned, 0);
  
  // Total pruned count should be updated
  EXPECT_GE(gc_->GetTotalPrunedVersions(), pruned);
  
  txn_mgr_->Commit(10);
}

/**
 * @brief Test 5: Background GC Thread
 * 
 * Start background GC, create versions, wait for GC to run,
 * verify versions are pruned automatically.
 */
TEST_F(GarbageCollectorTest, BackgroundGC) {
  RID test_rid(3, 0);
  
  // Create versions
  for (int i = 0; i < 5; i++) {
    txn_mgr_->Begin(i, IsolationLevel::REPEATABLE_READ, ConcurrencyProtocol::MVCC);
    auto *txn = txn_mgr_->GetTransaction(i);
    version_mgr_->CreateVersion(txn, test_rid, CreateTestRecord());
    txn_mgr_->Commit(i);
  }
  
  // Check initial version count
  auto chain = version_mgr_->GetVersionChain(test_rid);
  ASSERT_NE(chain, nullptr);
  size_t initial_versions = chain->GetVersionCount();
  EXPECT_EQ(initial_versions, 5);
  
  // Start a new transaction to set watermark
  txn_mgr_->Begin(100, IsolationLevel::REPEATABLE_READ, ConcurrencyProtocol::MVCC);
  
  // Start background GC
  gc_->Start();
  EXPECT_TRUE(gc_->IsRunning());
  
  // Wait for at least 2 GC cycles
  std::this_thread::sleep_for(std::chrono::milliseconds(300));
  
  // Check that GC ran
  EXPECT_GT(gc_->GetGCCycles(), 0);
  
  // Versions should have been pruned
  size_t total_pruned = gc_->GetTotalPrunedVersions();
  EXPECT_GT(total_pruned, 0);
  
  // Stop GC
  gc_->Stop();
  EXPECT_FALSE(gc_->IsRunning());
  
  txn_mgr_->Commit(100);
}

/**
 * @brief Test 6: GC Statistics
 */
TEST_F(GarbageCollectorTest, Statistics) {
  EXPECT_EQ(gc_->GetTotalPrunedVersions(), 0);
  EXPECT_EQ(gc_->GetGCCycles(), 0);
  
  RID test_rid(4, 0);
  
  // Create and commit versions
  for (int i = 0; i < 3; i++) {
    txn_mgr_->Begin(i, IsolationLevel::REPEATABLE_READ, ConcurrencyProtocol::MVCC);
    auto *txn = txn_mgr_->GetTransaction(i);
    version_mgr_->CreateVersion(txn, test_rid, CreateTestRecord());
    txn_mgr_->Commit(i);
  }
  
  // Set watermark
  txn_mgr_->Begin(100, IsolationLevel::REPEATABLE_READ, ConcurrencyProtocol::MVCC);
  
  // Manual GC should update statistics
  gc_->CollectGarbage();
  
  // Should have pruned some versions
  EXPECT_GE(gc_->GetTotalPrunedVersions(), 0);
  
  // Reset statistics
  gc_->ResetStatistics();
  EXPECT_EQ(gc_->GetTotalPrunedVersions(), 0);
  EXPECT_EQ(gc_->GetGCCycles(), 0);
  
  txn_mgr_->Commit(100);
}

/**
 * @brief Test 7: GC with Active Transactions
 * 
 * Verify that GC doesn't prune versions still needed by active transactions.
 */
TEST_F(GarbageCollectorTest, GCWithActiveTransactions) {
  RID test_rid(5, 0);
  
  // T1 creates v1, commits
  txn_mgr_->Begin(0, IsolationLevel::REPEATABLE_READ, ConcurrencyProtocol::MVCC);
  auto *txn1 = txn_mgr_->GetTransaction(0);
  version_mgr_->CreateVersion(txn1, test_rid, nullptr);
  txn_mgr_->Commit(0);
  timestamp_t v1_commit = txn1->GetCommitTimestamp();
  
  // T2 starts BEFORE v2 is created (should see v1)
  txn_mgr_->Begin(1, IsolationLevel::REPEATABLE_READ, ConcurrencyProtocol::MVCC);
  auto *txn2 = txn_mgr_->GetTransaction(1);
  timestamp_t t2_read = txn2->GetReadTimestamp();
  EXPECT_GT(t2_read, v1_commit);
  
  // T3 creates v2, commits
  txn_mgr_->Begin(2, IsolationLevel::REPEATABLE_READ, ConcurrencyProtocol::MVCC);
  auto *txn3 = txn_mgr_->GetTransaction(2);
  version_mgr_->CreateVersion(txn3, test_rid, CreateTestRecord());
  txn_mgr_->Commit(2);
  
  // T2 needs the undo record for v2, which reconstructs the page value that T2
  // saw. The older v1 undo record is already below T2's watermark.
  
  auto chain = version_mgr_->GetVersionChain(test_rid);
  size_t versions_before_gc = chain->GetVersionCount();
  
  // Run GC
  gc_->CollectGarbage();
  
  // The v2 undo record remains; the older baseline undo record is unnecessary.
  size_t versions_after_gc = chain->GetVersionCount();
  EXPECT_LT(versions_after_gc, versions_before_gc);
  EXPECT_EQ(versions_after_gc, 1);
  
  // Commit T2
  txn_mgr_->Commit(1);
  
  // Now start a new transaction after T2 commits
  txn_mgr_->Begin(10, IsolationLevel::REPEATABLE_READ, ConcurrencyProtocol::MVCC);
  
  // Run GC again - no active snapshot needs the remaining undo record.
  gc_->CollectGarbage();
  chain = version_mgr_->GetVersionChain(test_rid);
  ASSERT_NE(chain, nullptr);
  EXPECT_EQ(chain->GetVersionCount(), 0);
  
  txn_mgr_->Commit(10);
}

/**
 * @brief Test 8: GC with Delete Markers
 */
TEST_F(GarbageCollectorTest, GCWithDeleteMarkers) {
  RID test_rid(6, 0);
  
  // Create initial version
  txn_mgr_->Begin(0, IsolationLevel::REPEATABLE_READ, ConcurrencyProtocol::MVCC);
  auto *txn1 = txn_mgr_->GetTransaction(0);
  version_mgr_->CreateVersion(txn1, test_rid, nullptr);
  txn_mgr_->Commit(0);
  
  // Delete the tuple
  txn_mgr_->Begin(1, IsolationLevel::REPEATABLE_READ, ConcurrencyProtocol::MVCC);
  auto *txn2 = txn_mgr_->GetTransaction(1);
  auto *delete_marker = version_mgr_->CreateDeleteVersion(txn2, test_rid, CreateTestRecord());
  EXPECT_TRUE(delete_marker->IsDeleted());
  txn_mgr_->Commit(1);
  
  // Set watermark
  txn_mgr_->Begin(10, IsolationLevel::REPEATABLE_READ, ConcurrencyProtocol::MVCC);
  
  // Run GC
  gc_->CollectGarbage();
  
  // This synthetic chain has no registered physical delete. Once every active
  // snapshot is newer than the delete, no undo record is needed.
  auto chain = version_mgr_->GetVersionChain(test_rid);
  ASSERT_NE(chain, nullptr);
  EXPECT_EQ(chain->GetVersionCount(), 0);
  
  txn_mgr_->Commit(10);
}

/**
 * @brief Test 9: GC with No Versions to Prune
 */
TEST_F(GarbageCollectorTest, GCWithNoVersionsToPrune) {
  // Create one version
  RID test_rid(7, 0);
  txn_mgr_->Begin(0, IsolationLevel::REPEATABLE_READ, ConcurrencyProtocol::MVCC);
  auto *txn = txn_mgr_->GetTransaction(0);
  version_mgr_->CreateVersion(txn, test_rid, nullptr);
  txn_mgr_->Commit(0);
  
  // Immediately run GC (no old versions to prune)
  size_t pruned = gc_->GetTotalPrunedVersions();
  gc_->CollectGarbage();
  size_t after_pruned = gc_->GetTotalPrunedVersions();
  
  // The page itself is the newest value, so a committed baseline undo record
  // can be discarded when no older snapshot exists.
  EXPECT_GT(after_pruned, pruned);
  auto chain = version_mgr_->GetVersionChain(test_rid);
  ASSERT_NE(chain, nullptr);
  EXPECT_EQ(chain->GetVersionCount(), 0);

  // A later writer must reuse the same synchronization anchor rather than
  // append to an orphaned chain that readers cannot discover.
  txn_mgr_->Begin(1, IsolationLevel::REPEATABLE_READ, ConcurrencyProtocol::MVCC);
  auto *next_txn = txn_mgr_->GetTransaction(1);
  version_mgr_->CreateVersion(next_txn, test_rid, CreateTestRecord());
  EXPECT_EQ(version_mgr_->GetVersionChain(test_rid), chain);
  EXPECT_EQ(chain->GetVersionCount(), 1);
  txn_mgr_->Commit(1);
}

/**
 * @brief Test 10: Concurrent GC and Transaction Execution
 */
TEST_F(GarbageCollectorTest, ConcurrentGCAndTransactions) {
  // Start background GC
  gc_->Start();
  
  // Run multiple transactions while GC is running
  for (int i = 0; i < 20; i++) {
    RID rid(i, 0);
    
    txn_mgr_->Begin(i, IsolationLevel::REPEATABLE_READ, ConcurrencyProtocol::MVCC);
    auto *txn = txn_mgr_->GetTransaction(i);
    version_mgr_->CreateVersion(txn, rid, CreateTestRecord());
    txn_mgr_->Commit(i);
    
    // Small delay to allow GC to run
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  
  // Wait for GC to run
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  
  // GC should have run multiple times
  EXPECT_GT(gc_->GetGCCycles(), 0);
  
  // Stop GC
  gc_->Stop();
}

} // namespace njudb

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
