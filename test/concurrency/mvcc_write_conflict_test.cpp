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
// ACID Issue #3: MVCC Write-Write Conflict Detection Tests
// Purpose: Verify that concurrent writes to the same tuple are properly detected
//

#include <gtest/gtest.h>
#include <thread>
#include <vector>
#include <atomic>
#include <chrono>
#include "concurrency/version_manager.h"
#include "concurrency/transaction.h"
#include "concurrency/timestamp_oracle.h"
#include "common/record.h"
#include "../../common/error.h"

namespace njudb {

// Helper function to create a simple record for testing
static auto CreateTestRecord(int value) -> RecordUptr {
  // For testing, we can use nullptr or a simple placeholder
  // In real scenario, this would have actual schema and data
  return nullptr;
}

//===--------------------------------------------------------------------===//
// Test 1: Sequential Write-Write Conflict
//===--------------------------------------------------------------------===//

TEST(MVCCWriteConflictTest, SequentialWriteConflict) {
  VersionManager version_mgr;
  RID rid(1, 0);
  
  // Transaction 1: Start update
  auto txn1 = std::make_unique<Transaction>(100, IsolationLevel::REPEATABLE_READ, 
                                             ConcurrencyProtocol::MVCC);
  txn1->SetTimestamp(1000);
  txn1->SetReadTimestamp(1000);
  
  // T1 creates first version (uncommitted)
  auto old_tuple1 = CreateTestRecord(10);
  auto *version1 = version_mgr.CreateVersion(txn1.get(), rid, std::move(old_tuple1));
  EXPECT_NE(version1, nullptr);
  EXPECT_EQ(version1->GetCommitTimestamp(), MAX_TS); // Uncommitted
  
  // Transaction 2: Attempt concurrent update on same tuple
  auto txn2 = std::make_unique<Transaction>(101, IsolationLevel::REPEATABLE_READ,
                                             ConcurrencyProtocol::MVCC);
  txn2->SetTimestamp(1001);
  txn2->SetReadTimestamp(1001);
  
  // T2 should detect write-write conflict
  auto old_tuple2 = CreateTestRecord(20);
  EXPECT_THROW({
    version_mgr.CreateVersion(txn2.get(), rid, std::move(old_tuple2));
  }, NJUDBException_);
  
  // NOTE: After exception, old_tuple2 is moved-from (nullptr).
  // The data was freed when the exception unwound, which is correct behavior.
  // The important part is that no half-created UndoRecord exists.
  
  // Verify only one version exists (T1's version, not T2's)
  auto chain = version_mgr.GetVersionChain(rid);
  ASSERT_NE(chain, nullptr);
  EXPECT_EQ(chain->GetVersionCount(), 1);
  
  // After T1 commits, T2 can proceed
  version1->Commit(1500);
  
  // Now T2 should succeed
  auto old_tuple3 = CreateTestRecord(20);
  auto *version2 = version_mgr.CreateVersion(txn2.get(), rid, std::move(old_tuple3));
  EXPECT_NE(version2, nullptr);
  EXPECT_EQ(chain->GetVersionCount(), 2);
}

//===--------------------------------------------------------------------===//
// Test 2: Concurrent Write-Write Conflict (Race Condition Test)
//===--------------------------------------------------------------------===//

TEST(MVCCWriteConflictTest, ConcurrentWriteConflict) {
  VersionManager version_mgr;
  RID rid(1, 0);
  
  std::atomic<int> success_count{0};
  std::atomic<int> conflict_count{0};
  std::atomic<bool> start_flag{false};
  
  const int NUM_THREADS = 10;
  std::vector<std::thread> threads;
  
  // Launch multiple threads trying to update the same tuple
  for (int i = 0; i < NUM_THREADS; i++) {
    threads.emplace_back([&, i]() {
      // Wait for all threads to be ready
      while (!start_flag.load()) {
        std::this_thread::yield();
      }
      
      auto txn = std::make_unique<Transaction>(
        100 + i, 
        IsolationLevel::REPEATABLE_READ,
        ConcurrencyProtocol::MVCC
      );
      txn->SetTimestamp(1000 + i);
      txn->SetReadTimestamp(1000 + i);
      
      try {
        auto old_tuple = CreateTestRecord(i);
        auto *version = version_mgr.CreateVersion(txn.get(), rid, std::move(old_tuple));
        
        if (version != nullptr) {
          success_count.fetch_add(1);
          // Simulate some work before commit
          std::this_thread::sleep_for(std::chrono::milliseconds(1));
          version->Commit(2000 + i);
        }
      } catch (const NJUDBException_ &e) {
        if (e.type_ == NJUDB_MVCC_WRITE_CONFLICT) {
          conflict_count.fetch_add(1);
        } else {
          // Unexpected exception
          FAIL() << "Unexpected exception: " << e.what();
        }
      }
    });
  }
  
  // Start all threads simultaneously
  start_flag.store(true);
  
  // Wait for all threads to complete
  for (auto &thread : threads) {
    thread.join();
  }
  
  // Verify: Exactly ONE thread should succeed
  EXPECT_EQ(success_count.load(), 1) 
    << "Expected exactly 1 success, got " << success_count.load();
  
  // Verify: All other threads should get conflicts
  EXPECT_EQ(conflict_count.load(), NUM_THREADS - 1)
    << "Expected " << (NUM_THREADS - 1) << " conflicts, got " << conflict_count.load();
  
  // Verify: Only one version in chain
  auto chain = version_mgr.GetVersionChain(rid);
  ASSERT_NE(chain, nullptr);
  EXPECT_EQ(chain->GetVersionCount(), 1);
}

//===--------------------------------------------------------------------===//
// Test 3: Write-Write Conflict on Delete
//===--------------------------------------------------------------------===//

TEST(MVCCWriteConflictTest, DeleteWriteConflict) {
  VersionManager version_mgr;
  RID rid(1, 0);
  
  // Transaction 1: Delete tuple
  auto txn1 = std::make_unique<Transaction>(100, IsolationLevel::REPEATABLE_READ,
                                             ConcurrencyProtocol::MVCC);
  txn1->SetTimestamp(1000);
  txn1->SetReadTimestamp(1000);
  
  auto old_tuple1 = CreateTestRecord(10);
  auto *delete_version = version_mgr.CreateDeleteVersion(txn1.get(), rid, std::move(old_tuple1));
  EXPECT_NE(delete_version, nullptr);
  EXPECT_TRUE(delete_version->IsDeleted());
  
  // Transaction 2: Attempt to update same tuple (should conflict with delete)
  auto txn2 = std::make_unique<Transaction>(101, IsolationLevel::REPEATABLE_READ,
                                             ConcurrencyProtocol::MVCC);
  txn2->SetTimestamp(1001);
  txn2->SetReadTimestamp(1001);
  
  auto old_tuple2 = CreateTestRecord(20);
  EXPECT_THROW({
    version_mgr.CreateVersion(txn2.get(), rid, std::move(old_tuple2));
  }, NJUDBException_);
  
  // Transaction 3: Attempt to delete same tuple (should also conflict)
  auto txn3 = std::make_unique<Transaction>(102, IsolationLevel::REPEATABLE_READ,
                                             ConcurrencyProtocol::MVCC);
  txn3->SetTimestamp(1002);
  txn3->SetReadTimestamp(1002);
  
  auto old_tuple3 = CreateTestRecord(30);
  EXPECT_THROW({
    version_mgr.CreateDeleteVersion(txn3.get(), rid, std::move(old_tuple3));
  }, NJUDBException_);
}

//===--------------------------------------------------------------------===//
// Test 4: No False Positives (Committed Version Should Allow New Write)
//===--------------------------------------------------------------------===//

TEST(MVCCWriteConflictTest, NoFalsePositive) {
  VersionManager version_mgr;
  RID rid(1, 0);
  
  // Transaction 1: Update and commit
  auto txn1 = std::make_unique<Transaction>(100, IsolationLevel::REPEATABLE_READ,
                                             ConcurrencyProtocol::MVCC);
  txn1->SetTimestamp(1000);
  txn1->SetReadTimestamp(1000);
  
  auto old_tuple1 = CreateTestRecord(10);
  auto *version1 = version_mgr.CreateVersion(txn1.get(), rid, std::move(old_tuple1));
  EXPECT_NE(version1, nullptr);
  
  // COMMIT T1
  version1->Commit(1500);
  
  // Transaction 2: Should be able to update committed tuple
  auto txn2 = std::make_unique<Transaction>(101, IsolationLevel::REPEATABLE_READ,
                                             ConcurrencyProtocol::MVCC);
  txn2->SetTimestamp(2000);
  txn2->SetReadTimestamp(2000);
  
  auto old_tuple2 = CreateTestRecord(20);
  auto *version2 = version_mgr.CreateVersion(txn2.get(), rid, std::move(old_tuple2));
  EXPECT_NE(version2, nullptr); // Should succeed without exception
  
  // Verify both versions exist
  auto chain = version_mgr.GetVersionChain(rid);
  EXPECT_EQ(chain->GetVersionCount(), 2);
}

//===--------------------------------------------------------------------===//
// Test 5: TOCTOU Protection (Double-Check Mechanism)
//===--------------------------------------------------------------------===//

TEST(MVCCWriteConflictTest, TOCTOUProtection) {
  VersionManager version_mgr;
  RID rid(1, 0);
  
  std::atomic<bool> t1_proceed{false};
  std::atomic<int> exceptions_caught{0};
  std::atomic<int> successes{0};
  
  std::thread t1([&]() {
    auto txn1 = std::make_unique<Transaction>(100, IsolationLevel::REPEATABLE_READ,
                                               ConcurrencyProtocol::MVCC);
    txn1->SetTimestamp(1000);
    txn1->SetReadTimestamp(1000);
    
    // T1 tries to insert
    try {
      auto old_tuple = CreateTestRecord(10);
      auto *version = version_mgr.CreateVersion(txn1.get(), rid, std::move(old_tuple));
      if (version != nullptr) {
        successes.fetch_add(1);
        t1_proceed.store(true);
      }
    } catch (const NJUDBException_ &e) {
      if (e.type_ == NJUDB_MVCC_WRITE_CONFLICT) {
        exceptions_caught.fetch_add(1);
      }
    }
  });
  
  std::thread t2([&]() {
    // Small delay to let T1 start
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
    
    auto txn2 = std::make_unique<Transaction>(101, IsolationLevel::REPEATABLE_READ,
                                               ConcurrencyProtocol::MVCC);
    txn2->SetTimestamp(1001);
    txn2->SetReadTimestamp(1001);
    
    // T2 tries to insert - should be caught by double-check in InsertVersion
    try {
      auto old_tuple = CreateTestRecord(20);
      auto *version = version_mgr.CreateVersion(txn2.get(), rid, std::move(old_tuple));
      if (version != nullptr) {
        successes.fetch_add(1);
      }
    } catch (const NJUDBException_ &e) {
      if (e.type_ == NJUDB_MVCC_WRITE_CONFLICT) {
        exceptions_caught.fetch_add(1);
      }
    }
  });
  
  t1.join();
  t2.join();
  
  // Exactly one should succeed
  EXPECT_EQ(successes.load(), 1) 
    << "Exactly one transaction should succeed";
  EXPECT_EQ(exceptions_caught.load(), 1)
    << "Exactly one transaction should be rejected";
}

//===--------------------------------------------------------------------===//
// Test 6: Exception Safety (UndoRecord Not Half-Created)
//===--------------------------------------------------------------------===//

TEST(MVCCWriteConflictTest, ExceptionSafety) {
  VersionManager version_mgr;
  RID rid(1, 0);
  
  // Transaction 1: Create uncommitted version
  auto txn1 = std::make_unique<Transaction>(100, IsolationLevel::REPEATABLE_READ,
                                             ConcurrencyProtocol::MVCC);
  txn1->SetTimestamp(1000);
  txn1->SetReadTimestamp(1000);
  
  auto old_tuple1 = CreateTestRecord(10);
  version_mgr.CreateVersion(txn1.get(), rid, std::move(old_tuple1));
  
  // Transaction 2: Try to create conflicting version
  auto txn2 = std::make_unique<Transaction>(101, IsolationLevel::REPEATABLE_READ,
                                             ConcurrencyProtocol::MVCC);
  txn2->SetTimestamp(1001);
  txn2->SetReadTimestamp(1001);
  
  auto old_tuple2 = CreateTestRecord(20);
  
  bool exception_thrown = false;
  try {
    version_mgr.CreateVersion(txn2.get(), rid, std::move(old_tuple2));
  } catch (const NJUDBException_ &e) {
    exception_thrown = true;
    // The key test: verify no half-created UndoRecord exists
    // Only T1's version should be in the chain
    auto chain = version_mgr.GetVersionChain(rid);
    ASSERT_NE(chain, nullptr);
    EXPECT_EQ(chain->GetVersionCount(), 1) 
      << "No half-created UndoRecord should exist after exception";
  }
  
  EXPECT_TRUE(exception_thrown) << "Expected write-write conflict exception";
}

} // namespace njudb

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
