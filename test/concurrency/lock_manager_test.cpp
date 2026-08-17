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
// Lock Manager Tests
//

#include "concurrency/lock_manager.h"
#include "concurrency/txn_manager.h"
#include "log/log_manager.h"
#include "storage/disk/disk_manager.h"
#include <gtest/gtest.h>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <thread>
#include <tuple>

namespace njudb {

class LockManagerTest : public ::testing::Test {
protected:
  void SetUp() override {
    // Lab05 must be testable before Lab06 logging is implemented.
    txn_mgr_ = new TxnManager(nullptr);
    
    // Initialize lock manager with DETECTION policy
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

static auto GetTableLockSetSize(Transaction *txn, LockMode lock_mode) -> size_t {
  switch (lock_mode) {
    case LockMode::SHARED:
      return txn->GetSharedTableLockSet()->size();
    case LockMode::EXCLUSIVE:
      return txn->GetExclusiveTableLockSet()->size();
    case LockMode::INTENTION_SHARED:
      return txn->GetIntentionSharedTableLockSet()->size();
    case LockMode::INTENTION_EXCLUSIVE:
      return txn->GetIntentionExclusiveTableLockSet()->size();
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      return txn->GetSharedIntentionExclusiveTableLockSet()->size();
  }
  return 0;
}

static auto GetPageLockSetSize(Transaction *txn, LockMode lock_mode) -> size_t {
  switch (lock_mode) {
    case LockMode::SHARED:
      return txn->GetSharedPageLockSet()->size();
    case LockMode::EXCLUSIVE:
      return txn->GetExclusivePageLockSet()->size();
    case LockMode::INTENTION_SHARED:
    case LockMode::INTENTION_EXCLUSIVE:
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      return 0;
  }
  return 0;
}

static auto GetTupleLockSetSize(Transaction *txn, LockMode lock_mode) -> size_t {
  switch (lock_mode) {
    case LockMode::SHARED:
      return txn->GetSharedTupleLockSet()->size();
    case LockMode::EXCLUSIVE:
      return txn->GetExclusiveTupleLockSet()->size();
    case LockMode::INTENTION_SHARED:
    case LockMode::INTENTION_EXCLUSIVE:
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      return 0;
  }
  return 0;
}

static auto LockModeTestName(const testing::TestParamInfo<LockMode> &info) -> std::string {
  switch (info.param) {
    case LockMode::SHARED:
      return "Shared";
    case LockMode::EXCLUSIVE:
      return "Exclusive";
    case LockMode::INTENTION_SHARED:
      return "IntentionShared";
    case LockMode::INTENTION_EXCLUSIVE:
      return "IntentionExclusive";
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      return "SharedIntentionExclusive";
  }
  return "Unknown";
}

static auto LockModeName(LockMode lock_mode) -> std::string {
  switch (lock_mode) {
    case LockMode::SHARED:
      return "Shared";
    case LockMode::EXCLUSIVE:
      return "Exclusive";
    case LockMode::INTENTION_SHARED:
      return "IntentionShared";
    case LockMode::INTENTION_EXCLUSIVE:
      return "IntentionExclusive";
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      return "SharedIntentionExclusive";
  }
  return "Unknown";
}

static auto IsolationLevelTestName(const testing::TestParamInfo<IsolationLevel> &info) -> std::string {
  switch (info.param) {
    case IsolationLevel::READ_UNCOMMITTED:
      return "ReadUncommitted";
    case IsolationLevel::READ_COMMITTED:
      return "ReadCommitted";
    case IsolationLevel::REPEATABLE_READ:
      return "RepeatableRead";
    case IsolationLevel::SERIALIZABLE:
      return "Serializable";
  }
  return "Unknown";
}

static auto AreLockModesCompatibleForTest(LockMode held, LockMode requested) -> bool {
  static const bool compatibility[5][5] = {
    // Held/requested order: S, X, IS, IX, SIX
    /* S   */ {true,  false, true,  false, false},
    /* X   */ {false, false, false, false, false},
    /* IS  */ {true,  false, true,  true,  true },
    /* IX  */ {false, false, true,  true,  false},
    /* SIX */ {false, false, true,  false, false}
  };

  return compatibility[static_cast<int>(held)][static_cast<int>(requested)];
}

class LockManagerIsolationLevelTest : public LockManagerTest,
                                      public ::testing::WithParamInterface<IsolationLevel> {};

class LockManagerTableLockModeTest : public LockManagerTest, public ::testing::WithParamInterface<LockMode> {};

TEST_P(LockManagerTableLockModeTest, BasicTableLock) {
  LockMode lock_mode = GetParam();

  txn_mgr_->Begin(1);
  auto txn = txn_mgr_->GetTransaction(1);

  EXPECT_TRUE(lock_mgr_->LockTable(txn, lock_mode, 0));
  EXPECT_EQ(GetTableLockSetSize(txn, lock_mode), 1);

  // Release lock
  EXPECT_TRUE(lock_mgr_->UnlockTable(txn, 0));
  EXPECT_EQ(GetTableLockSetSize(txn, lock_mode), 0);

  txn_mgr_->Commit(1);
}

INSTANTIATE_TEST_SUITE_P(
  AllTableLockModes,
  LockManagerTableLockModeTest,
  ::testing::Values(
    LockMode::SHARED,
    LockMode::EXCLUSIVE,
    LockMode::INTENTION_SHARED,
    LockMode::INTENTION_EXCLUSIVE,
    LockMode::SHARED_INTENTION_EXCLUSIVE),
  LockModeTestName);

class LockManagerPageLockModeTest : public LockManagerTest, public ::testing::WithParamInterface<LockMode> {};

TEST_P(LockManagerPageLockModeTest, PageLocking) {
  LockMode lock_mode = GetParam();

  txn_mgr_->Begin(1);
  auto txn = txn_mgr_->GetTransaction(1);

  EXPECT_TRUE(lock_mgr_->LockPage(txn, lock_mode, 1));
  EXPECT_EQ(GetPageLockSetSize(txn, lock_mode), 1);

  EXPECT_TRUE(lock_mgr_->UnlockPage(txn, 1));
  EXPECT_EQ(GetPageLockSetSize(txn, lock_mode), 0);

  txn_mgr_->Commit(1);
}

INSTANTIATE_TEST_SUITE_P(
  SharedAndExclusivePageLocks,
  LockManagerPageLockModeTest,
  ::testing::Values(LockMode::SHARED, LockMode::EXCLUSIVE),
  LockModeTestName);

class LockManagerTupleLockModeTest : public LockManagerTest, public ::testing::WithParamInterface<LockMode> {};

TEST_P(LockManagerTupleLockModeTest, TupleLocking) {
  LockMode lock_mode = GetParam();

  txn_mgr_->Begin(1);
  auto txn = txn_mgr_->GetTransaction(1);

  RID rid(1, 0);

  EXPECT_TRUE(lock_mgr_->LockTuple(txn, lock_mode, rid));
  EXPECT_EQ(GetTupleLockSetSize(txn, lock_mode), 1);

  EXPECT_TRUE(lock_mgr_->UnlockTuple(txn, rid));
  EXPECT_EQ(GetTupleLockSetSize(txn, lock_mode), 0);

  txn_mgr_->Commit(1);
}

INSTANTIATE_TEST_SUITE_P(
  SharedAndExclusiveTupleLocks,
  LockManagerTupleLockModeTest,
  ::testing::Values(LockMode::SHARED, LockMode::EXCLUSIVE),
  LockModeTestName);

using LockCompatibilityParam = std::tuple<LockMode, LockMode>;

class LockManagerCompatibilityTest
    : public LockManagerTest,
      public ::testing::WithParamInterface<LockCompatibilityParam> {};

static auto LockCompatibilityTestName(
    const testing::TestParamInfo<LockCompatibilityParam> &info) -> std::string {
  auto [held, requested] = info.param;
  return LockModeName(held) + "_Then_" + LockModeName(requested);
}

TEST_P(LockManagerCompatibilityTest, LockCompatibility) {
  auto [held_mode, requested_mode] = GetParam();
  bool compatible = AreLockModesCompatibleForTest(held_mode, requested_mode);

  txn_mgr_->Begin(1);
  txn_mgr_->Begin(2);

  auto txn1 = txn_mgr_->GetTransaction(1);
  auto txn2 = txn_mgr_->GetTransaction(2);

  EXPECT_TRUE(lock_mgr_->LockTable(txn1, held_mode, 0));
  EXPECT_EQ(GetTableLockSetSize(txn1, held_mode), 1);

  if (compatible) {
    EXPECT_TRUE(lock_mgr_->LockTable(txn2, requested_mode, 0));
    EXPECT_EQ(GetTableLockSetSize(txn2, requested_mode), 1);
  } else {
    bool txn2_acquired = false;
    std::thread txn2_thread([&]() {
      txn2_acquired = lock_mgr_->LockTable(txn2, requested_mode, 0);
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    EXPECT_FALSE(txn2_acquired);

    EXPECT_TRUE(lock_mgr_->UnlockTable(txn1, 0));
    txn2_thread.join();

    EXPECT_TRUE(txn2_acquired);
    EXPECT_EQ(GetTableLockSetSize(txn2, requested_mode), 1);
  }

  txn_mgr_->Commit(1);
  txn_mgr_->Commit(2);
}

INSTANTIATE_TEST_SUITE_P(
  AllLockModePairs,
  LockManagerCompatibilityTest,
  ::testing::Combine(
    ::testing::Values(
      LockMode::SHARED,
      LockMode::EXCLUSIVE,
      LockMode::INTENTION_SHARED,
      LockMode::INTENTION_EXCLUSIVE,
      LockMode::SHARED_INTENTION_EXCLUSIVE),
    ::testing::Values(
      LockMode::SHARED,
      LockMode::EXCLUSIVE,
      LockMode::INTENTION_SHARED,
      LockMode::INTENTION_EXCLUSIVE,
      LockMode::SHARED_INTENTION_EXCLUSIVE)),
  LockCompatibilityTestName);

using LockUpgradeParam = std::tuple<LockMode, LockMode>;

class LockManagerValidUpgradeTest
    : public LockManagerTest,
      public ::testing::WithParamInterface<LockUpgradeParam> {};

class LockManagerInvalidUpgradeTest
    : public LockManagerTest,
      public ::testing::WithParamInterface<LockUpgradeParam> {};

static auto LockUpgradeTestName(const testing::TestParamInfo<LockUpgradeParam> &info) -> std::string {
  auto [from, to] = info.param;
  return LockModeName(from) + "_To_" + LockModeName(to);
}

TEST_P(LockManagerValidUpgradeTest, LockUpgrade) {
  auto [old_mode, new_mode] = GetParam();

  txn_mgr_->Begin(1);
  auto txn = txn_mgr_->GetTransaction(1);

  EXPECT_TRUE(lock_mgr_->LockTable(txn, old_mode, 0));
  EXPECT_EQ(GetTableLockSetSize(txn, old_mode), 1);

  EXPECT_TRUE(lock_mgr_->LockTable(txn, new_mode, 0));
  EXPECT_EQ(GetTableLockSetSize(txn, old_mode), 0);
  EXPECT_EQ(GetTableLockSetSize(txn, new_mode), 1);
  EXPECT_EQ(txn->GetState(), TxnState::GROWING);

  txn_mgr_->Commit(1);
}

TEST_P(LockManagerInvalidUpgradeTest, InvalidLockUpgrade) {
  auto [old_mode, new_mode] = GetParam();

  txn_mgr_->Begin(1);
  txn_mgr_->Begin(2);
  auto txn = txn_mgr_->GetTransaction(1);
  auto other_txn = txn_mgr_->GetTransaction(2);

  EXPECT_TRUE(lock_mgr_->LockTable(txn, old_mode, 0));
  EXPECT_EQ(GetTableLockSetSize(txn, old_mode), 1);

  EXPECT_THROW(lock_mgr_->LockTable(txn, new_mode, 0), std::logic_error);
  EXPECT_EQ(txn->GetState(), TxnState::ABORTED);

  txn_mgr_->Abort(1);
  EXPECT_EQ(txn_mgr_->GetTransaction(1), nullptr);

  EXPECT_TRUE(lock_mgr_->LockTable(other_txn, LockMode::EXCLUSIVE, 0));
  EXPECT_EQ(GetTableLockSetSize(other_txn, LockMode::EXCLUSIVE), 1);
  txn_mgr_->Commit(2);
}

INSTANTIATE_TEST_SUITE_P(
  ValidTableLockUpgrades,
  LockManagerValidUpgradeTest,
  ::testing::Values(
    LockUpgradeParam{LockMode::INTENTION_SHARED, LockMode::SHARED},
    LockUpgradeParam{LockMode::INTENTION_SHARED, LockMode::EXCLUSIVE},
    LockUpgradeParam{LockMode::INTENTION_SHARED, LockMode::INTENTION_EXCLUSIVE},
    LockUpgradeParam{LockMode::INTENTION_SHARED, LockMode::SHARED_INTENTION_EXCLUSIVE},
    LockUpgradeParam{LockMode::SHARED, LockMode::EXCLUSIVE},
    LockUpgradeParam{LockMode::SHARED, LockMode::SHARED_INTENTION_EXCLUSIVE},
    LockUpgradeParam{LockMode::INTENTION_EXCLUSIVE, LockMode::EXCLUSIVE},
    LockUpgradeParam{LockMode::INTENTION_EXCLUSIVE, LockMode::SHARED_INTENTION_EXCLUSIVE},
    LockUpgradeParam{LockMode::SHARED_INTENTION_EXCLUSIVE, LockMode::EXCLUSIVE}),
  LockUpgradeTestName);

INSTANTIATE_TEST_SUITE_P(
  InvalidTableLockUpgrades,
  LockManagerInvalidUpgradeTest,
  ::testing::Values(
    LockUpgradeParam{LockMode::SHARED, LockMode::INTENTION_SHARED},
    LockUpgradeParam{LockMode::SHARED, LockMode::INTENTION_EXCLUSIVE},
    LockUpgradeParam{LockMode::INTENTION_EXCLUSIVE, LockMode::SHARED},
    LockUpgradeParam{LockMode::INTENTION_EXCLUSIVE, LockMode::INTENTION_SHARED},
    LockUpgradeParam{LockMode::SHARED_INTENTION_EXCLUSIVE, LockMode::SHARED},
    LockUpgradeParam{LockMode::SHARED_INTENTION_EXCLUSIVE, LockMode::INTENTION_SHARED},
    LockUpgradeParam{LockMode::SHARED_INTENTION_EXCLUSIVE, LockMode::INTENTION_EXCLUSIVE},
    LockUpgradeParam{LockMode::EXCLUSIVE, LockMode::SHARED},
    LockUpgradeParam{LockMode::EXCLUSIVE, LockMode::INTENTION_SHARED},
    LockUpgradeParam{LockMode::EXCLUSIVE, LockMode::INTENTION_EXCLUSIVE},
    LockUpgradeParam{LockMode::EXCLUSIVE, LockMode::SHARED_INTENTION_EXCLUSIVE}),
  LockUpgradeTestName);

TEST_F(LockManagerTest, HierarchicalLockingFollowsParentBeforeChildProtocol) {
  txn_mgr_->Begin(1);
  auto txn = txn_mgr_->GetTransaction(1);

  RID read_rid(10, 0);
  RID write_rid(20, 0);

  EXPECT_TRUE(lock_mgr_->LockTable(txn, LockMode::INTENTION_SHARED, 10));
  EXPECT_TRUE(lock_mgr_->LockPage(txn, LockMode::SHARED, 10));
  EXPECT_TRUE(lock_mgr_->LockTuple(txn, LockMode::SHARED, read_rid));

  EXPECT_TRUE(lock_mgr_->LockTable(txn, LockMode::INTENTION_EXCLUSIVE, 20));
  EXPECT_TRUE(lock_mgr_->LockPage(txn, LockMode::EXCLUSIVE, 20));
  EXPECT_TRUE(lock_mgr_->LockTuple(txn, LockMode::EXCLUSIVE, write_rid));

  EXPECT_EQ(txn->GetIntentionSharedTableLockSet()->size(), 1);
  EXPECT_EQ(txn->GetIntentionExclusiveTableLockSet()->size(), 1);
  EXPECT_EQ(txn->GetSharedPageLockSet()->size(), 1);
  EXPECT_EQ(txn->GetExclusivePageLockSet()->size(), 1);
  EXPECT_EQ(txn->GetSharedTupleLockSet()->size(), 1);
  EXPECT_EQ(txn->GetExclusiveTupleLockSet()->size(), 1);

  txn_mgr_->Commit(1);
}

TEST_F(LockManagerTest, UnlockAllReleasesHierarchicalLocksAndWakesTableWaiter) {
  txn_mgr_->Begin(1);
  txn_mgr_->Begin(2);

  auto txn1 = txn_mgr_->GetTransaction(1);
  auto txn2 = txn_mgr_->GetTransaction(2);

  RID rid(1, 0);

  EXPECT_TRUE(lock_mgr_->LockTable(txn1, LockMode::INTENTION_SHARED, 1));
  EXPECT_TRUE(lock_mgr_->LockPage(txn1, LockMode::SHARED, 1));
  EXPECT_TRUE(lock_mgr_->LockTuple(txn1, LockMode::SHARED, rid));
  EXPECT_TRUE(txn1->HasAnyLocks());

  bool txn2_acquired = false;
  std::thread txn2_thread([&]() {
    txn2_acquired = lock_mgr_->LockTable(txn2, LockMode::EXCLUSIVE, 1);
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  EXPECT_FALSE(txn2_acquired);

  lock_mgr_->UnlockAll(txn1);
  EXPECT_FALSE(txn1->HasAnyLocks());

  txn2_thread.join();
  EXPECT_TRUE(txn2_acquired);
  EXPECT_EQ(GetTableLockSetSize(txn2, LockMode::EXCLUSIVE), 1);

  txn_mgr_->Commit(1);
  txn_mgr_->Commit(2);
}

TEST_P(LockManagerTableLockModeTest, IdempotentTableLocking) {
  LockMode lock_mode = GetParam();

  txn_mgr_->Begin(1);
  auto txn = txn_mgr_->GetTransaction(1);

  EXPECT_TRUE(lock_mgr_->LockTable(txn, lock_mode, 0));
  EXPECT_EQ(GetTableLockSetSize(txn, lock_mode), 1);

  // Acquire same lock again - should be idempotent
  EXPECT_TRUE(lock_mgr_->LockTable(txn, lock_mode, 0));
  EXPECT_EQ(GetTableLockSetSize(txn, lock_mode), 1);

  txn_mgr_->Commit(1);
}

TEST_F(LockManagerTest, UnlockAll) {
  txn_mgr_->Begin(1);
  txn_mgr_->Begin(2);
  auto txn = txn_mgr_->GetTransaction(1);
  auto waiter = txn_mgr_->GetTransaction(2);

  // Acquire multiple locks
  EXPECT_TRUE(lock_mgr_->LockTable(txn, LockMode::SHARED, 0));
  EXPECT_TRUE(lock_mgr_->LockTable(txn, LockMode::EXCLUSIVE, 1));
  EXPECT_TRUE(lock_mgr_->LockTable(txn, LockMode::INTENTION_SHARED, 2));
  EXPECT_TRUE(lock_mgr_->LockTable(txn, LockMode::INTENTION_EXCLUSIVE, 3));
  EXPECT_TRUE(lock_mgr_->LockTable(txn, LockMode::SHARED_INTENTION_EXCLUSIVE, 4));

  EXPECT_TRUE(lock_mgr_->LockPage(txn, LockMode::SHARED, 10));
  EXPECT_TRUE(lock_mgr_->LockPage(txn, LockMode::EXCLUSIVE, 11));

  RID rid(1, 0);
  EXPECT_TRUE(lock_mgr_->LockTuple(txn, LockMode::SHARED, rid));

  EXPECT_TRUE(txn->HasAnyLocks());

  bool waiter_acquired = false;
  std::thread waiter_thread([&]() {
    waiter_acquired = lock_mgr_->LockTable(waiter, LockMode::EXCLUSIVE, 4);
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  EXPECT_FALSE(waiter_acquired);

  lock_mgr_->UnlockAll(txn);

  EXPECT_FALSE(txn->HasAnyLocks());

  waiter_thread.join();
  EXPECT_TRUE(waiter_acquired);
  EXPECT_EQ(GetTableLockSetSize(waiter, LockMode::EXCLUSIVE), 1);

  txn_mgr_->Commit(1);
  txn_mgr_->Commit(2);
}

// Wait-Die Prevention Tests
class LockManagerWaitDieTest : public ::testing::Test {
protected:
  void SetUp() override {
    txn_mgr_ = new TxnManager(nullptr);
    lock_mgr_ = new LockManager(DeadlockPolicy::WAIT_DIE, txn_mgr_);
    txn_mgr_->SetLockManager(lock_mgr_);
  }

  void TearDown() override {
    delete lock_mgr_;
    delete txn_mgr_;
  }

  TxnManager *txn_mgr_;
  LockManager *lock_mgr_;
};

TEST_F(LockManagerWaitDieTest, OlderWaitsForMultipleHolders) {
  txn_mgr_->Begin(1);  // Oldest requester
  txn_mgr_->Begin(2);  // Younger holder
  txn_mgr_->Begin(3);  // Youngest holder

  auto txn1 = txn_mgr_->GetTransaction(1);
  auto txn2 = txn_mgr_->GetTransaction(2);
  auto txn3 = txn_mgr_->GetTransaction(3);

  EXPECT_TRUE(lock_mgr_->LockTable(txn2, LockMode::SHARED, 0));
  EXPECT_TRUE(lock_mgr_->LockTable(txn3, LockMode::SHARED, 0));

  bool txn1_acquired = false;
  std::thread txn1_thread([&]() {
    txn1_acquired = lock_mgr_->LockTable(txn1, LockMode::EXCLUSIVE, 0);
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  EXPECT_FALSE(txn1_acquired);
  EXPECT_NE(txn1->GetState(), TxnState::ABORTED);

  EXPECT_TRUE(lock_mgr_->UnlockTable(txn2, 0));
  EXPECT_TRUE(lock_mgr_->UnlockTable(txn3, 0));

  txn1_thread.join();
  EXPECT_TRUE(txn1_acquired);
  EXPECT_EQ(GetTableLockSetSize(txn1, LockMode::EXCLUSIVE), 1);

  txn_mgr_->Commit(1);
  txn_mgr_->Commit(2);
  txn_mgr_->Commit(3);
}

TEST_F(LockManagerWaitDieTest, YoungerDiesIfAnyHolderIsOlder) {
  txn_mgr_->Begin(1);  // Oldest holder
  txn_mgr_->Begin(2);  // Middle requester
  txn_mgr_->Begin(3);  // Youngest holder

  auto txn1 = txn_mgr_->GetTransaction(1);
  auto txn2 = txn_mgr_->GetTransaction(2);
  auto txn3 = txn_mgr_->GetTransaction(3);

  EXPECT_TRUE(lock_mgr_->LockTable(txn3, LockMode::SHARED, 0));
  EXPECT_TRUE(lock_mgr_->LockTable(txn1, LockMode::SHARED, 0));

  EXPECT_FALSE(lock_mgr_->LockTable(txn2, LockMode::EXCLUSIVE, 0));
  EXPECT_EQ(txn2->GetState(), TxnState::ABORTED);
  EXPECT_EQ(GetTableLockSetSize(txn1, LockMode::SHARED), 1);
  EXPECT_EQ(GetTableLockSetSize(txn3, LockMode::SHARED), 1);

  txn_mgr_->Commit(1);
  txn_mgr_->Abort(2);
  txn_mgr_->Commit(3);
}

TEST_F(LockManagerWaitDieTest, YoungerDiesReleasesPreviouslyHeldLocks) {
  txn_mgr_->Begin(1);  // Older
  txn_mgr_->Begin(2);  // Younger

  auto txn1 = txn_mgr_->GetTransaction(1);
  auto txn2 = txn_mgr_->GetTransaction(2);

  EXPECT_TRUE(lock_mgr_->LockTable(txn2, LockMode::EXCLUSIVE, 1));
  EXPECT_TRUE(lock_mgr_->LockTable(txn1, LockMode::EXCLUSIVE, 0));

  EXPECT_FALSE(lock_mgr_->LockTable(txn2, LockMode::EXCLUSIVE, 0));
  EXPECT_EQ(txn2->GetState(), TxnState::ABORTED);

  txn_mgr_->Abort(2);
  EXPECT_EQ(txn_mgr_->GetTransaction(2), nullptr);

  txn_mgr_->Begin(3);
  auto txn3 = txn_mgr_->GetTransaction(3);

  EXPECT_TRUE(lock_mgr_->LockTable(txn3, LockMode::EXCLUSIVE, 1));
  EXPECT_EQ(GetTableLockSetSize(txn3, LockMode::EXCLUSIVE), 1);

  txn_mgr_->Commit(1);
  txn_mgr_->Commit(3);
}

// Wound-Wait Prevention Tests
class LockManagerWoundWaitTest : public ::testing::Test {
protected:
  void SetUp() override {
    txn_mgr_ = new TxnManager(nullptr);
    lock_mgr_ = new LockManager(DeadlockPolicy::WOUND_WAIT, txn_mgr_);
    txn_mgr_->SetLockManager(lock_mgr_);
  }

  void TearDown() override {
    delete lock_mgr_;
    delete txn_mgr_;
  }

  TxnManager *txn_mgr_;
  LockManager *lock_mgr_;
};

TEST_F(LockManagerWoundWaitTest, OlderWoundsYoungerAndReleasesHeldLocks) {
  txn_mgr_->Begin(1);  // Older requester
  txn_mgr_->Begin(2);  // Younger holder

  auto txn1 = txn_mgr_->GetTransaction(1);
  auto txn2 = txn_mgr_->GetTransaction(2);

  EXPECT_TRUE(lock_mgr_->LockTable(txn2, LockMode::EXCLUSIVE, 0));
  EXPECT_TRUE(lock_mgr_->LockTable(txn2, LockMode::EXCLUSIVE, 1));

  EXPECT_TRUE(lock_mgr_->LockTable(txn1, LockMode::EXCLUSIVE, 0));
  EXPECT_EQ(txn2->GetState(), TxnState::ABORTED);
  EXPECT_FALSE(txn2->HasAnyLocks());
  EXPECT_EQ(GetTableLockSetSize(txn1, LockMode::EXCLUSIVE), 1);

  txn_mgr_->Begin(3);
  auto txn3 = txn_mgr_->GetTransaction(3);

  EXPECT_TRUE(lock_mgr_->LockTable(txn3, LockMode::EXCLUSIVE, 1));
  EXPECT_EQ(GetTableLockSetSize(txn3, LockMode::EXCLUSIVE), 1);

  txn_mgr_->Commit(1);
  txn_mgr_->Abort(2);
  txn_mgr_->Commit(3);
}

TEST_F(LockManagerWoundWaitTest, YoungerWaitsForOlderHolder) {
  txn_mgr_->Begin(1);  // Older holder
  txn_mgr_->Begin(2);  // Younger requester

  auto txn1 = txn_mgr_->GetTransaction(1);
  auto txn2 = txn_mgr_->GetTransaction(2);

  EXPECT_TRUE(lock_mgr_->LockTable(txn1, LockMode::EXCLUSIVE, 0));

  std::atomic<bool> txn2_done{false};
  std::atomic<bool> txn2_result{false};

  std::thread txn2_thread([&]() {
    txn2_result = lock_mgr_->LockTable(txn2, LockMode::EXCLUSIVE, 0);
    txn2_done = true;
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  EXPECT_FALSE(txn2_done);
  EXPECT_NE(txn2->GetState(), TxnState::ABORTED);

  lock_mgr_->UnlockAll(txn1);

  for (int i = 0; i < 100 && !txn2_done; i++) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  ASSERT_TRUE(txn2_done);
  EXPECT_TRUE(txn2_result);
  EXPECT_EQ(GetTableLockSetSize(txn2, LockMode::EXCLUSIVE), 1);

  txn2_thread.join();
  txn_mgr_->Commit(1);
  txn_mgr_->Commit(2);
}

TEST_F(LockManagerWoundWaitTest, OlderWoundsAllConflictingYoungerHolders) {
  txn_mgr_->Begin(1);  // Older requester
  txn_mgr_->Begin(2);  // Younger holder
  txn_mgr_->Begin(3);  // Youngest holder

  auto txn1 = txn_mgr_->GetTransaction(1);
  auto txn2 = txn_mgr_->GetTransaction(2);
  auto txn3 = txn_mgr_->GetTransaction(3);

  EXPECT_TRUE(lock_mgr_->LockTable(txn2, LockMode::SHARED, 0));
  EXPECT_TRUE(lock_mgr_->LockTable(txn3, LockMode::SHARED, 0));
  EXPECT_TRUE(lock_mgr_->LockTable(txn2, LockMode::EXCLUSIVE, 1));
  EXPECT_TRUE(lock_mgr_->LockTable(txn3, LockMode::EXCLUSIVE, 2));

  EXPECT_TRUE(lock_mgr_->LockTable(txn1, LockMode::EXCLUSIVE, 0));
  EXPECT_EQ(txn2->GetState(), TxnState::ABORTED);
  EXPECT_EQ(txn3->GetState(), TxnState::ABORTED);
  EXPECT_FALSE(txn2->HasAnyLocks());
  EXPECT_FALSE(txn3->HasAnyLocks());
  EXPECT_EQ(GetTableLockSetSize(txn1, LockMode::EXCLUSIVE), 1);

  txn_mgr_->Begin(4);
  auto txn4 = txn_mgr_->GetTransaction(4);

  EXPECT_TRUE(lock_mgr_->LockTable(txn4, LockMode::EXCLUSIVE, 1));
  EXPECT_TRUE(lock_mgr_->LockTable(txn4, LockMode::EXCLUSIVE, 2));
  EXPECT_EQ(GetTableLockSetSize(txn4, LockMode::EXCLUSIVE), 2);

  txn_mgr_->Commit(1);
  txn_mgr_->Abort(2);
  txn_mgr_->Abort(3);
  txn_mgr_->Commit(4);
}

TEST_F(LockManagerTest, DeadlockDetectionAbortsVictimAndReleasesLocks) {
  txn_mgr_->Begin(1);
  txn_mgr_->Begin(2);

  auto txn1 = txn_mgr_->GetTransaction(1);
  auto txn2 = txn_mgr_->GetTransaction(2);

  EXPECT_TRUE(lock_mgr_->LockTable(txn1, LockMode::EXCLUSIVE, 1));
  EXPECT_TRUE(lock_mgr_->LockTable(txn2, LockMode::EXCLUSIVE, 2));

  std::atomic<bool> txn1_done{false};
  std::atomic<bool> txn2_done{false};
  std::atomic<bool> txn1_result{false};
  std::atomic<bool> txn2_result{false};

  std::thread txn1_thread([&]() {
    try {
      txn1_result = lock_mgr_->LockTable(txn1, LockMode::EXCLUSIVE, 2);
    } catch (const std::exception &) {
      txn1_result = false;
    }
    txn1_done = true;
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  EXPECT_FALSE(txn1_done);

  std::thread txn2_thread([&]() {
    try {
      txn2_result = lock_mgr_->LockTable(txn2, LockMode::EXCLUSIVE, 1);
    } catch (const std::exception &) {
      txn2_result = false;
    }
    txn2_done = true;
  });

  for (int i = 0; i < 100 && !txn1_done && !txn2_done; i++) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  if (!txn1_done && !txn2_done) {
    txn_mgr_->Abort(1);
    txn_mgr_->Abort(2);
    txn1_thread.join();
    txn2_thread.join();
    FAIL() << "Deadlock detection did not abort a victim before timeout";
  }

  bool txn1_aborted = txn1->GetState() == TxnState::ABORTED || (txn1_done && !txn1_result);
  bool txn2_aborted = txn2->GetState() == TxnState::ABORTED || (txn2_done && !txn2_result);

  EXPECT_NE(txn1_aborted, txn2_aborted);

  if (txn1_aborted) {
    txn_mgr_->Abort(1);
    EXPECT_EQ(txn_mgr_->GetTransaction(1), nullptr);

    for (int i = 0; i < 100 && !txn2_done; i++) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    ASSERT_TRUE(txn2_done);
    EXPECT_TRUE(txn2_result);
    EXPECT_EQ(GetTableLockSetSize(txn2, LockMode::EXCLUSIVE), 2);
    txn_mgr_->Commit(2);
  } else {
    txn_mgr_->Abort(2);
    EXPECT_EQ(txn_mgr_->GetTransaction(2), nullptr);

    for (int i = 0; i < 100 && !txn1_done; i++) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    ASSERT_TRUE(txn1_done);
    EXPECT_TRUE(txn1_result);
    EXPECT_EQ(GetTableLockSetSize(txn1, LockMode::EXCLUSIVE), 2);
    txn_mgr_->Commit(1);
  }

  txn1_thread.join();
  txn2_thread.join();
}

// 2PL Phase Enforcement Tests
TEST_P(LockManagerIsolationLevelTest, TwoPhaseLockingEnforcedForRepeatableReadAndSerializable) {
  txn_mgr_->Begin(1, GetParam());
  txn_mgr_->Begin(2);
  auto txn1 = txn_mgr_->GetTransaction(1);
  auto txn2 = txn_mgr_->GetTransaction(2);

  EXPECT_TRUE(lock_mgr_->LockTable(txn1, LockMode::SHARED, 0));
  EXPECT_TRUE(lock_mgr_->LockTable(txn1, LockMode::EXCLUSIVE, 1));
  EXPECT_EQ(txn1->GetState(), TxnState::GROWING);

  EXPECT_TRUE(lock_mgr_->UnlockTable(txn1, 0));
  EXPECT_EQ(txn1->GetState(), TxnState::SHIRNKING);

  EXPECT_THROW(lock_mgr_->LockTable(txn1, LockMode::SHARED, 2), std::logic_error);
  EXPECT_EQ(txn1->GetState(), TxnState::ABORTED);

  txn_mgr_->Abort(1);
  EXPECT_EQ(txn_mgr_->GetTransaction(1), nullptr);

  EXPECT_TRUE(lock_mgr_->LockTable(txn2, LockMode::EXCLUSIVE, 1));
  txn_mgr_->Commit(2);
}

INSTANTIATE_TEST_SUITE_P(RepeatableReadAndSerializable,
                         LockManagerIsolationLevelTest,
                         ::testing::Values(IsolationLevel::REPEATABLE_READ,
                                           IsolationLevel::SERIALIZABLE),
                         IsolationLevelTestName);

TEST_F(LockManagerTest, ReadCommittedDoesNotEnterShrinkingAfterReadUnlock) {
  txn_mgr_->Begin(1, IsolationLevel::READ_COMMITTED);
  txn_mgr_->Begin(2);
  auto txn1 = txn_mgr_->GetTransaction(1);
  auto txn2 = txn_mgr_->GetTransaction(2);

  EXPECT_TRUE(lock_mgr_->LockTable(txn1, LockMode::SHARED, 0));
  EXPECT_EQ(txn1->GetState(), TxnState::GROWING);

  EXPECT_TRUE(lock_mgr_->UnlockTable(txn1, 0));
  EXPECT_EQ(txn1->GetState(), TxnState::GROWING);

  EXPECT_TRUE(lock_mgr_->LockTable(txn2, LockMode::EXCLUSIVE, 0));
  txn_mgr_->Commit(2);

  EXPECT_TRUE(lock_mgr_->LockTable(txn1, LockMode::SHARED, 1));
  EXPECT_EQ(txn1->GetState(), TxnState::GROWING);

  txn_mgr_->Commit(1);
}

TEST_F(LockManagerTest, ReadCommittedCanAcquireWriteAfterReadUnlock) {
  txn_mgr_->Begin(1, IsolationLevel::READ_COMMITTED);
  auto txn = txn_mgr_->GetTransaction(1);

  EXPECT_TRUE(lock_mgr_->LockTable(txn, LockMode::SHARED, 0));
  EXPECT_EQ(txn->GetState(), TxnState::GROWING);

  EXPECT_TRUE(lock_mgr_->UnlockTable(txn, 0));
  EXPECT_EQ(txn->GetState(), TxnState::GROWING);

  EXPECT_TRUE(lock_mgr_->LockTable(txn, LockMode::EXCLUSIVE, 1));
  EXPECT_EQ(GetTableLockSetSize(txn, LockMode::EXCLUSIVE), 1);

  txn_mgr_->Commit(1);
}

} // namespace njudb

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
