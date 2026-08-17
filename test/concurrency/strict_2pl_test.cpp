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
// Comprehensive Strict 2PL Integration Tests
//

#include "concurrency/lock_manager.h"
#include "concurrency/txn_manager.h"
#include "log/log_manager.h"
#include "storage/disk/disk_manager.h"
#include "../../common/error.h"
#include <gtest/gtest.h>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <mutex>
#include <thread>
#include <vector>

namespace njudb {

class Strict2PLTest : public ::testing::Test {
protected:
  void SetUp() override {
    // Lab05 is intentionally independent from the Lab06 WAL implementation.
    txn_mgr_ = new TxnManager(nullptr, nullptr, nullptr);
    lock_mgr_ = new LockManager(DeadlockPolicy::DETECTION, txn_mgr_);
    txn_mgr_->SetLockManager(lock_mgr_);
  }

  void TearDown() override {
    delete lock_mgr_;
    delete txn_mgr_;
  }

  TxnManager *txn_mgr_;
  LockManager *lock_mgr_;
};

// Test 1: Strict 2PL - Locks held until commit
TEST_F(Strict2PLTest, LocksHeldUntilCommit) {
  
  // Start two transactions
  txn_mgr_->Begin(1, IsolationLevel::SERIALIZABLE);
  txn_mgr_->Begin(2, IsolationLevel::SERIALIZABLE);
  
  auto txn1 = txn_mgr_->GetTransaction(1);
  auto txn2 = txn_mgr_->GetTransaction(2);
  
  table_id_t table_id = 1;
  
  // T1 acquires X lock
  EXPECT_TRUE(lock_mgr_->LockTable(txn1, LockMode::EXCLUSIVE, table_id));
  EXPECT_EQ(txn1->GetState(), TxnState::GROWING);
  
  // T2 tries to acquire conflicting lock - should block
  bool t2_acquired = false;
  std::thread t2_thread([&]() {
    t2_acquired = lock_mgr_->LockTable(txn2, LockMode::SHARED, table_id);
  });
  
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  EXPECT_FALSE(t2_acquired);  // T2 still waiting
  
  // T1 commits - locks released
  txn_mgr_->Commit(1);
  t2_thread.join();
  
  // Now T2 should have acquired lock
  EXPECT_TRUE(t2_acquired);
  EXPECT_EQ(txn2->GetState(), TxnState::GROWING);
  
  txn_mgr_->Commit(2);
}

TEST_F(Strict2PLTest, ConcurrentCommitAbortRunsOneEndPath) {
  txn_mgr_->Begin(100, IsolationLevel::SERIALIZABLE);
  auto txn_handle = txn_mgr_->GetTransactionShared(100);
  ASSERT_NE(txn_handle, nullptr);
  ASSERT_TRUE(lock_mgr_->LockTable(txn_handle.get(), LockMode::EXCLUSIVE, 100));

  std::thread commit_thread([&]() { txn_mgr_->Commit(100); });
  std::thread abort_thread([&]() { txn_mgr_->Abort(100); });
  commit_thread.join();
  abort_thread.join();

  EXPECT_EQ(txn_mgr_->GetTransaction(100), nullptr);
  EXPECT_TRUE(
      txn_handle->GetState() == TxnState::COMMITTED || txn_handle->GetState() == TxnState::ABORTED);
  EXPECT_EQ(txn_handle->GetExclusiveTableLockSet()->count(100), 0);
}

// Test 2: Multiple transactions - serializable execution
TEST_F(Strict2PLTest, SerializableExecution) {
  
  const int NUM_TXNS = 5;
  const table_id_t table_id = 1;
  
  std::vector<std::thread> threads;
  std::atomic<int> completed{0};
  std::atomic<int> active_writers{0};
  std::atomic<int> shared_value{0};
  std::mutex order_latch;
  std::vector<txn_id_t> execution_order;
  
  for (int i = 0; i < NUM_TXNS; i++) {
    threads.emplace_back([&, i]() {
      txn_id_t txn_id = i + 1;
      txn_mgr_->Begin(txn_id, IsolationLevel::SERIALIZABLE);
      auto txn = txn_mgr_->GetTransaction(txn_id);
      
      // Each transaction tries to acquire X lock on same table
      bool acquired = lock_mgr_->LockTable(txn, LockMode::EXCLUSIVE, table_id);
      
      if (acquired) {
        EXPECT_EQ(active_writers.fetch_add(1), 0);

        int old_value = shared_value.load();
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        shared_value.store(old_value + 1);

        {
          std::lock_guard<std::mutex> lock(order_latch);
          execution_order.push_back(txn_id);
        }

        EXPECT_EQ(active_writers.fetch_sub(1), 1);
        
        // Commit
        txn_mgr_->Commit(txn_id);
        completed++;
      } else {
        txn_mgr_->Abort(txn_id);
      }
    });
  }
  
  for (auto &t : threads) {
    t.join();
  }
  
  // All transactions should complete (serialized by locks)
  EXPECT_EQ(completed.load(), NUM_TXNS);
  EXPECT_EQ(shared_value.load(), NUM_TXNS);
  EXPECT_EQ(execution_order.size(), NUM_TXNS);

  std::vector<int> seen(NUM_TXNS + 1, 0);
  for (auto txn_id : execution_order) {
    ASSERT_GE(txn_id, 1);
    ASSERT_LE(txn_id, NUM_TXNS);
    seen[txn_id]++;
  }
  for (int txn_id = 1; txn_id <= NUM_TXNS; txn_id++) {
    EXPECT_EQ(seen[txn_id], 1);
  }
}

// Test 3: Complex Multi-Resource Multi-Transaction Scenario
TEST_F(Strict2PLTest, ComplexMultiResourceScenario) {
  // This test involves multiple transactions accessing tables, pages, and tuples
  // Tests: hierarchical locking, cross-resource conflicts, concurrent access patterns
  
  txn_mgr_->Begin(1, IsolationLevel::SERIALIZABLE);
  txn_mgr_->Begin(2, IsolationLevel::SERIALIZABLE);
  txn_mgr_->Begin(3, IsolationLevel::SERIALIZABLE);
  txn_mgr_->Begin(4, IsolationLevel::SERIALIZABLE);
  txn_mgr_->Begin(5, IsolationLevel::SERIALIZABLE);
  txn_mgr_->Begin(6, IsolationLevel::SERIALIZABLE);
  txn_mgr_->Begin(7, IsolationLevel::SERIALIZABLE);
  txn_mgr_->Begin(8, IsolationLevel::SERIALIZABLE);
  
  auto txn1 = txn_mgr_->GetTransaction(1);
  auto txn2 = txn_mgr_->GetTransaction(2);
  auto txn3 = txn_mgr_->GetTransaction(3);
  auto txn4 = txn_mgr_->GetTransaction(4);
  auto txn5 = txn_mgr_->GetTransaction(5);
  auto txn6 = txn_mgr_->GetTransaction(6);
  auto txn7 = txn_mgr_->GetTransaction(7);
  auto txn8 = txn_mgr_->GetTransaction(8);
  
  RID rid1(1, 0), rid2(1, 1), rid3(2, 0), rid4(2, 1);
  RID rid5(3, 0), rid6(3, 1), rid7(4, 0);
  
  // Scenario: T1 and T2 read from table 1, T3 and T4 write to table 2
  // This should work fine with proper hierarchical locking
  
  // T1: Read tuples from table 1 (needs IS on table, S on page, S on tuples)
  EXPECT_TRUE(lock_mgr_->LockTable(txn1, LockMode::INTENTION_SHARED, 1));
  EXPECT_TRUE(lock_mgr_->LockPage(txn1, LockMode::SHARED, 1));
  EXPECT_TRUE(lock_mgr_->LockTuple(txn1, LockMode::SHARED, rid1));
  EXPECT_TRUE(lock_mgr_->LockTuple(txn1, LockMode::SHARED, rid2));
  
  // T2: Also read from table 1 (should not conflict with T1)
  EXPECT_TRUE(lock_mgr_->LockTable(txn2, LockMode::INTENTION_SHARED, 1));
  EXPECT_TRUE(lock_mgr_->LockPage(txn2, LockMode::SHARED, 1));
  EXPECT_TRUE(lock_mgr_->LockTuple(txn2, LockMode::SHARED, rid1));
  EXPECT_TRUE(lock_mgr_->LockTuple(txn2, LockMode::SHARED, rid2));
  
  // T3: Write to table 2 (needs IX on table, X on page, X on tuples)
  EXPECT_TRUE(lock_mgr_->LockTable(txn3, LockMode::INTENTION_EXCLUSIVE, 2));
  EXPECT_TRUE(lock_mgr_->LockPage(txn3, LockMode::EXCLUSIVE, 2));
  EXPECT_TRUE(lock_mgr_->LockTuple(txn3, LockMode::EXCLUSIVE, rid3));
  
  // T4: Try to write to same table 2, different tuple (should block on page lock)
  std::atomic<bool> t4_got_page_lock{false};
  std::atomic<bool> t4_finished{false};
  
  std::thread t4_thread([&]() {
    // This should block because T3 holds X lock on page 2
    bool result = lock_mgr_->LockTable(txn4, LockMode::INTENTION_EXCLUSIVE, 2);
    EXPECT_TRUE(result); // Table IX should succeed (compatible with T3's IX)
    
    result = lock_mgr_->LockPage(txn4, LockMode::EXCLUSIVE, 2);
    t4_got_page_lock = result;
    
    if (result) {
      EXPECT_TRUE(lock_mgr_->LockTuple(txn4, LockMode::EXCLUSIVE, rid4));
    }
    t4_finished = true;
  });
  
  // Give T4 time to try acquiring locks
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  EXPECT_FALSE(t4_got_page_lock); // Should be blocked
  EXPECT_FALSE(t4_finished);
  
  // T3 commits, releasing its locks
  txn_mgr_->Commit(3);
  
  // Now T4 should succeed
  t4_thread.join();
  EXPECT_TRUE(t4_got_page_lock);
  EXPECT_TRUE(t4_finished);

  // T5 and T6: Write different tuples in the same table. Their table IX locks
  // are compatible, and tuple X locks on different RIDs should not conflict.
  EXPECT_TRUE(lock_mgr_->LockTable(txn5, LockMode::INTENTION_EXCLUSIVE, 3));
  EXPECT_TRUE(lock_mgr_->LockTuple(txn5, LockMode::EXCLUSIVE, rid5));

  std::atomic<bool> t6_got_tuple_lock{false};
  std::atomic<bool> t6_finished{false};

  std::thread t6_thread([&]() {
    EXPECT_TRUE(lock_mgr_->LockTable(txn6, LockMode::INTENTION_EXCLUSIVE, 3));
    t6_got_tuple_lock = lock_mgr_->LockTuple(txn6, LockMode::EXCLUSIVE, rid6);
    t6_finished = true;
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  EXPECT_TRUE(t6_got_tuple_lock);
  EXPECT_TRUE(t6_finished);
  t6_thread.join();

  // T7 and T8: Conflict on the same tuple. Abort should release T7's locks and
  // wake T8 without requiring a commit path.
  EXPECT_TRUE(lock_mgr_->LockTable(txn7, LockMode::INTENTION_EXCLUSIVE, 4));
  EXPECT_TRUE(lock_mgr_->LockTuple(txn7, LockMode::EXCLUSIVE, rid7));

  std::atomic<bool> t8_got_tuple_lock{false};
  std::atomic<bool> t8_finished{false};

  std::thread t8_thread([&]() {
    EXPECT_TRUE(lock_mgr_->LockTable(txn8, LockMode::INTENTION_EXCLUSIVE, 4));
    t8_got_tuple_lock = lock_mgr_->LockTuple(txn8, LockMode::EXCLUSIVE, rid7);
    t8_finished = true;
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  EXPECT_FALSE(t8_got_tuple_lock);
  EXPECT_FALSE(t8_finished);

  txn_mgr_->Abort(7);
  t8_thread.join();
  EXPECT_TRUE(t8_got_tuple_lock);
  EXPECT_TRUE(t8_finished);
  
  // Commit remaining transactions
  txn_mgr_->Commit(1);
  txn_mgr_->Commit(2);
  txn_mgr_->Commit(4);
  txn_mgr_->Commit(5);
  txn_mgr_->Commit(6);
  txn_mgr_->Commit(8);
}

// Test 4: Cross-Resource Deadlock Detection
TEST_F(Strict2PLTest, CrossResourceDeadlock) {
  // Test deadlock detection across different resource types
  // T1 holds table, waits for tuple
  // T2 holds tuple, waits for table
  // This should be detected as a deadlock
  
  txn_mgr_->Begin(1, IsolationLevel::SERIALIZABLE);
  txn_mgr_->Begin(2, IsolationLevel::SERIALIZABLE);
  
  auto txn1 = txn_mgr_->GetTransaction(1);
  auto txn2 = txn_mgr_->GetTransaction(2);
  RID rid(2, 0);
  
  // T1 locks table 1
  EXPECT_TRUE(lock_mgr_->LockTable(txn1, LockMode::EXCLUSIVE, 1));
  
  // T2 locks tuple R under table 2's IX parent lock
  EXPECT_TRUE(lock_mgr_->LockTable(txn2, LockMode::INTENTION_EXCLUSIVE, 2));
  EXPECT_TRUE(lock_mgr_->LockTuple(txn2, LockMode::EXCLUSIVE, rid));
  
  // Create cross-resource deadlock:
  // T1 waits for T2's tuple lock, then T2 waits for T1's table lock.
  std::atomic<bool> t1_done{false};
  std::atomic<bool> t2_done{false};
  std::atomic<bool> t1_result{false};
  std::atomic<bool> t2_result{false};
  
  std::thread t1_thread([&]() {
    try {
      EXPECT_TRUE(lock_mgr_->LockTable(txn1, LockMode::INTENTION_EXCLUSIVE, 2));
      t1_result = lock_mgr_->LockTuple(txn1, LockMode::EXCLUSIVE, rid);
    } catch (const std::exception &) {
      t1_result = false;
    }
    t1_done = true;
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  EXPECT_FALSE(t1_done);
  
  std::thread t2_thread([&]() {
    try {
      // T2 tries to lock table 1 (held by T1)
      t2_result = lock_mgr_->LockTable(txn2, LockMode::EXCLUSIVE, 1);
    } catch (const std::exception &) {
      t2_result = false;
    }
    t2_done = true;
  });

  for (int i = 0; i < 100 && !t1_done && !t2_done; i++) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  if (!t1_done && !t2_done) {
    txn_mgr_->Abort(1);
    txn_mgr_->Abort(2);
    t1_thread.join();
    t2_thread.join();
    FAIL() << "Cross-resource deadlock detection did not abort a victim before timeout";
  }

  bool t1_aborted = txn1->GetState() == TxnState::ABORTED || (t1_done && !t1_result);
  bool t2_aborted = txn2->GetState() == TxnState::ABORTED || (t2_done && !t2_result);
  EXPECT_NE(t1_aborted, t2_aborted);

  if (t1_aborted) {
    txn_mgr_->Abort(1);
    EXPECT_EQ(txn_mgr_->GetTransaction(1), nullptr);

    for (int i = 0; i < 100 && !t2_done; i++) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    ASSERT_TRUE(t2_done);
    EXPECT_TRUE(t2_result);
    EXPECT_EQ(txn2->GetExclusiveTableLockSet()->count(1), 1);
    EXPECT_EQ(txn2->GetIntentionExclusiveTableLockSet()->count(2), 1);
    EXPECT_EQ(txn2->GetExclusiveTupleLockSet()->count(rid), 1);
    txn_mgr_->Commit(2);
  } else {
    txn_mgr_->Abort(2);
    EXPECT_EQ(txn_mgr_->GetTransaction(2), nullptr);

    for (int i = 0; i < 100 && !t1_done; i++) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    ASSERT_TRUE(t1_done);
    EXPECT_TRUE(t1_result);
    EXPECT_EQ(txn1->GetExclusiveTableLockSet()->count(1), 1);
    EXPECT_EQ(txn1->GetIntentionExclusiveTableLockSet()->count(2), 1);
    EXPECT_EQ(txn1->GetExclusiveTupleLockSet()->count(rid), 1);
    txn_mgr_->Commit(1);
  }
  
  t1_thread.join();
  t2_thread.join();
}

// Test 5: Multi-Transaction Concurrent Access Pattern
TEST_F(Strict2PLTest, MultiTransactionConcurrentPattern) {
  // T1 and T2 hold compatible S locks on the same table.
  // T3 requests X and must wait until both holders release via txn lifecycle.
  
  txn_mgr_->Begin(1, IsolationLevel::SERIALIZABLE);
  txn_mgr_->Begin(2, IsolationLevel::SERIALIZABLE);
  txn_mgr_->Begin(3, IsolationLevel::SERIALIZABLE);
  
  auto txn1 = txn_mgr_->GetTransaction(1);
  auto txn2 = txn_mgr_->GetTransaction(2);
  auto txn3 = txn_mgr_->GetTransaction(3);
  
  // T1 and T2: Shared readers on table 1
  EXPECT_TRUE(lock_mgr_->LockTable(txn1, LockMode::SHARED, 1));
  EXPECT_TRUE(lock_mgr_->LockTable(txn2, LockMode::SHARED, 1));

  // T3: Try to get exclusive lock on table 1 (should block)
  std::atomic<bool> t3_acquired{false};
  std::atomic<bool> t3_done{false};
  std::thread t3_thread([&]() {
    t3_acquired = lock_mgr_->LockTable(txn3, LockMode::EXCLUSIVE, 1);
    t3_done = true;
  });
  
  // Wait and verify T3 is blocked
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  EXPECT_FALSE(t3_acquired);
  EXPECT_FALSE(t3_done);
  
  // Commit T1. T3 must still wait because T2 still holds a shared lock.
  txn_mgr_->Commit(1);
  EXPECT_EQ(txn_mgr_->GetTransaction(1), nullptr);
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  EXPECT_FALSE(t3_acquired);
  EXPECT_FALSE(t3_done);
  
  // Abort T2. T3 should now be woken because all conflicting holders are gone.
  txn_mgr_->Abort(2);
  EXPECT_EQ(txn_mgr_->GetTransaction(2), nullptr);
  
  // Now T3 should proceed
  t3_thread.join();
  EXPECT_TRUE(t3_acquired);
  EXPECT_TRUE(t3_done);
  EXPECT_EQ(txn3->GetExclusiveTableLockSet()->count(1), 1);
  
  // Cleanup
  txn_mgr_->Commit(3);
}

// Test 6: Lock Upgrade with Hierarchical Locking
TEST_F(Strict2PLTest, HierarchicalLockUpgrade) {
  txn_mgr_->Begin(1, IsolationLevel::SERIALIZABLE);
  txn_mgr_->Begin(2, IsolationLevel::SERIALIZABLE);
  auto txn1 = txn_mgr_->GetTransaction(1);
  auto txn2 = txn_mgr_->GetTransaction(2);
  
  RID rid1(1, 0);
  
  // T1 starts as a reader across all hierarchy levels.
  EXPECT_TRUE(lock_mgr_->LockTable(txn1, LockMode::INTENTION_SHARED, 1));
  EXPECT_TRUE(lock_mgr_->LockPage(txn1, LockMode::SHARED, 1));
  EXPECT_TRUE(lock_mgr_->LockTuple(txn1, LockMode::SHARED, rid1));
  
  // T2 is another reader on the same tuple. This should block T1's tuple S->X upgrade.
  // The direct LockManager API does not enforce parent page locks, so T2 only
  // takes the table intention lock needed to participate in table-level hierarchy.
  EXPECT_TRUE(lock_mgr_->LockTable(txn2, LockMode::INTENTION_SHARED, 1));
  EXPECT_TRUE(lock_mgr_->LockTuple(txn2, LockMode::SHARED, rid1));
  
  // Upgrade table and page first. SIX preserves T1's read intent while adding write intent.
  EXPECT_TRUE(lock_mgr_->LockTable(txn1, LockMode::SHARED_INTENTION_EXCLUSIVE, 1));
  EXPECT_TRUE(lock_mgr_->LockPage(txn1, LockMode::EXCLUSIVE, 1));
  
  std::atomic<bool> t1_tuple_upgraded{false};
  std::thread upgrade_thread([&]() {
    t1_tuple_upgraded = lock_mgr_->LockTuple(txn1, LockMode::EXCLUSIVE, rid1);
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  EXPECT_FALSE(t1_tuple_upgraded);

  txn_mgr_->Commit(2);
  EXPECT_EQ(txn_mgr_->GetTransaction(2), nullptr);

  upgrade_thread.join();
  EXPECT_TRUE(t1_tuple_upgraded);
  
  // Verify final state
  EXPECT_EQ(txn1->GetIntentionSharedTableLockSet()->count(1), 0);
  EXPECT_EQ(txn1->GetSharedIntentionExclusiveTableLockSet()->count(1), 1);
  EXPECT_EQ(txn1->GetSharedPageLockSet()->count(1), 0);
  EXPECT_EQ(txn1->GetExclusivePageLockSet()->count(1), 1);
  EXPECT_EQ(txn1->GetSharedTupleLockSet()->count(rid1), 0);
  EXPECT_EQ(txn1->GetExclusiveTupleLockSet()->count(rid1), 1);
  EXPECT_EQ(txn1->GetState(), TxnState::GROWING);
  
  txn_mgr_->Commit(1);
  EXPECT_EQ(txn_mgr_->GetTransaction(1), nullptr);
}

} // namespace njudb

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
