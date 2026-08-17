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
// Created for Lab05 Phase 4.1 - Version Chain Infrastructure Tests
//

#include <gtest/gtest.h>
#include <atomic>
#include <thread>
#include "concurrency/undo_record.h"
#include "concurrency/version_chain.h"
#include "concurrency/version_table.h"
#include "common/record.h"

namespace njudb {

// Helper function to create a simple record for testing
// Since Record requires a schema and we don't want to complicate tests,
// we'll use nullptr for old_tuple in most tests (simulating INSERT operations)
static auto CreateTestRecord() -> RecordUptr {
  // Return nullptr to simplify tests - this simulates INSERT operations
  // where there's no old tuple data
  return nullptr;
}

static auto CreateIntRecord(const RecordSchema &schema, const RID &rid, int32_t value) -> RecordUptr {
  std::vector<ValueSptr> values{ValueFactory::CreateIntValue(value)};
  return std::make_unique<Record>(&schema, values, rid);
}

static auto AddCommittedVersion(VersionChain *chain,
    const RID &rid,
    txn_id_t txn_id,
    timestamp_t begin_ts,
    timestamp_t commit_ts) -> UndoRecord * {
  auto rec = CreateTestRecord();
  auto *undo = new UndoRecord(txn_id, rid, std::move(rec), begin_ts);
  undo->Commit(commit_ts);
  chain->InsertVersion(undo);
  return undo;
}

static void AddCommittedVersions(VersionChain *chain,
    const RID &rid,
    int count,
    txn_id_t txn_id_base,
    timestamp_t timestamp_base) {
  for (int i = 0; i < count; i++) {
    AddCommittedVersion(chain, rid, txn_id_base + i, timestamp_base + i * 100, timestamp_base + i * 100 + 50);
  }
}

static void AddCommittedVersions(const VersionChainHandle &chain,
    const RID &rid,
    int count,
    txn_id_t txn_id_base,
    timestamp_t timestamp_base) {
  AddCommittedVersions(chain.get(), rid, count, txn_id_base, timestamp_base);
}

//===--------------------------------------------------------------------===//
// UndoRecord Tests
//===--------------------------------------------------------------------===//

TEST(UndoRecordTest, BasicCreation) {
  RID rid(1, 0);
  std::vector<RTField> fields;
  RTField              value_field;
  value_field.field_.field_name_ = "value";
  value_field.field_.field_type_ = TYPE_INT;
  value_field.field_.field_size_ = sizeof(int32_t);
  fields.push_back(value_field);
  RecordSchema schema(fields);

  auto record = CreateIntRecord(schema, rid, 42);
  
  UndoRecord undo(100, rid, std::move(record), 1000);
  UndoRecord prev(99, RID(0, 0), CreateTestRecord(), 900);
  
  EXPECT_EQ(undo.GetTxnId(), 100);
  EXPECT_EQ(undo.GetRID(), rid);
  EXPECT_EQ(undo.GetBeginTimestamp(), 1000);
  EXPECT_EQ(undo.GetCommitTimestamp(), MAX_TS);
  EXPECT_FALSE(undo.IsDeleted());
  EXPECT_EQ(undo.GetPrev(), nullptr);

  ASSERT_NE(undo.GetTuple(), nullptr);
  EXPECT_EQ(undo.GetTuple()->GetRID(), rid);
  auto stored_value = undo.GetTuple()->GetValueAt(0);
  ASSERT_NE(stored_value, nullptr);
  ASSERT_FALSE(stored_value->IsNull());
  auto *int_value = dynamic_cast<const IntValue *>(stored_value.get());
  ASSERT_NE(int_value, nullptr);
  EXPECT_EQ(int_value->Get(), 42);

  undo.SetPrev(&prev);
  EXPECT_EQ(undo.GetPrev(), &prev);
}

TEST(UndoRecordTest, CommitState) {
  RID rid(1, 0);
  auto record = CreateTestRecord();
  
  UndoRecord undo(100, rid, std::move(record), 1000);
  
  undo.Commit(1500);
  EXPECT_EQ(undo.GetCommitTimestamp(), 1500);
}

TEST(UndoRecordTest, AbortedVersion) {
  RID rid(1, 0);
  auto record = CreateTestRecord();
  
  UndoRecord undo(100, rid, std::move(record), 1000);
  
  // Abort the version
  undo.Abort();
  EXPECT_EQ(undo.GetCommitTimestamp(), ABORTED_TS);
  
}

TEST(UndoRecordTest, DeleteMarker) {
  RID rid(1, 0);
  auto record = CreateTestRecord();
  
  UndoRecord undo(100, rid, std::move(record), 1000);
  undo.SetDeleted(true);
  undo.Commit(1500);
  
  EXPECT_TRUE(undo.IsDeleted());
}

//===--------------------------------------------------------------------===//
// VersionChain Tests
//===--------------------------------------------------------------------===//

TEST(VersionChainTest, ResolveUndoChainVisibility) {
  VersionChain chain;
  RID rid(1, 0);

  std::vector<RTField> fields;
  RTField value_field;
  value_field.field_.field_name_ = "value";
  value_field.field_.field_type_ = TYPE_INT;
  value_field.field_.field_size_ = sizeof(int32_t);
  fields.push_back(value_field);
  RecordSchema schema(fields);

  // A0 was replaced at ts=20, then A1 was replaced by the page value at ts=30.
  auto *a0 = new UndoRecord(100, rid, CreateIntRecord(schema, rid, 100), 10);
  a0->Commit(20);
  chain.InsertVersion(a0);

  auto *a1 = new UndoRecord(101, rid, CreateIntRecord(schema, rid, 200), 20);
  a1->Commit(30);
  chain.InsertVersion(a1);

  auto before_first_update = chain.ResolveVisibility(15, 200);
  EXPECT_EQ(before_first_update.state, VersionVisibilityState::READ_UNDO);
  ASSERT_NE(before_first_update.undo_tuple, nullptr);
  EXPECT_EQ(dynamic_cast<const IntValue *>(before_first_update.undo_tuple->GetValueAt(0).get())->Get(), 100);

  auto between_updates = chain.ResolveVisibility(25, 201);
  EXPECT_EQ(between_updates.state, VersionVisibilityState::READ_UNDO);
  ASSERT_NE(between_updates.undo_tuple, nullptr);
  EXPECT_EQ(dynamic_cast<const IntValue *>(between_updates.undo_tuple->GetValueAt(0).get())->Get(), 200);

  auto after_latest_update = chain.ResolveVisibility(35, 202);
  EXPECT_EQ(after_latest_update.state, VersionVisibilityState::READ_PAGE);
  EXPECT_EQ(after_latest_update.undo_tuple, nullptr);
}

TEST(VersionChainTest, ResolveUncommittedInsertVisibility) {
  VersionChain chain;
  RID rid(1, 0);
  auto *insert_marker = new UndoRecord(100, rid, nullptr, 10);
  chain.InsertVersion(insert_marker);

  auto writer_read = chain.ResolveVisibility(10, 100);
  EXPECT_EQ(writer_read.state, VersionVisibilityState::READ_PAGE);

  auto other_read = chain.ResolveVisibility(20, 200);
  EXPECT_EQ(other_read.state, VersionVisibilityState::NOT_VISIBLE);
  EXPECT_EQ(other_read.undo_tuple, nullptr);
}

TEST(VersionChainTest, CanInsertVersion) {
  VersionChain chain;
  RID rid(1, 0);

  EXPECT_TRUE(chain.CanInsertVersion());

  auto rec = CreateTestRecord();
  auto *undo = new UndoRecord(100, rid, std::move(rec), 1000);
  chain.InsertVersion(undo);

  EXPECT_FALSE(chain.CanInsertVersion());

  undo->Commit(1500);
  EXPECT_TRUE(chain.CanInsertVersion());
}

TEST(VersionChainTest, RemoveOnlyCurrentUncommittedHead) {
  VersionChain chain;
  RID rid(1, 0);

  auto *committed = new UndoRecord(100, rid, CreateTestRecord(), 1000);
  committed->Commit(1500);
  chain.InsertVersion(committed);

  auto *uncommitted = new UndoRecord(101, rid, CreateTestRecord(), 2000);
  chain.InsertVersion(uncommitted);

  EXPECT_TRUE(chain.RemoveUncommittedHead(uncommitted));
  EXPECT_EQ(chain.GetHead(), committed);
  EXPECT_EQ(chain.GetVersionCount(), 1);
  EXPECT_EQ(uncommitted->GetPrev(), nullptr);
  delete uncommitted;

  EXPECT_FALSE(chain.RemoveUncommittedHead(committed));
  EXPECT_EQ(chain.GetHead(), committed);
  EXPECT_EQ(chain.GetVersionCount(), 1);
}

TEST(VersionChainTest, PrepareExistingWriteReusesOwnerAndRejectsOtherTransaction) {
  VersionChain chain;
  RID rid(1, 0);
  auto *uncommitted = new UndoRecord(100, rid, CreateTestRecord(), 1000);
  chain.InsertVersion(uncommitted);

  auto reused = chain.PrepareExistingWrite(100, true);
  EXPECT_EQ(reused.undo_record, uncommitted);
  EXPECT_FALSE(reused.conflict);
  EXPECT_FALSE(reused.previous_deleted);
  EXPECT_TRUE(uncommitted->IsDeleted());

  auto conflict = chain.PrepareExistingWrite(101, false);
  EXPECT_EQ(conflict.undo_record, nullptr);
  EXPECT_TRUE(conflict.conflict);
  EXPECT_TRUE(uncommitted->IsDeleted());

  chain.RestoreExistingWrite(uncommitted, reused.previous_deleted);
  EXPECT_FALSE(uncommitted->IsDeleted());
}

TEST(VersionChainTest, DeleteAtWatermarkCanBeReclaimedBeforePruning) {
  VersionChain chain;
  RID rid(1, 0);
  auto *deleted = new UndoRecord(100, rid, CreateTestRecord(), 10);
  deleted->SetDeleted(true);
  deleted->Commit(20);
  chain.InsertVersion(deleted);

  EXPECT_TRUE(chain.CanReclaimDelete(deleted, 20));
}

TEST(VersionChainTest, PruneVersions) {
  VersionChain chain;
  RID rid(1, 0);
  RTField field;
  field.field_.field_name_ = "value";
  field.field_.field_type_ = TYPE_INT;
  field.field_.field_size_ = sizeof(int32_t);
  RecordSchema schema({field});

  auto *v1 = new UndoRecord(100, rid, CreateIntRecord(schema, rid, 10), 10);
  v1->Commit(20);
  chain.InsertVersion(v1);
  auto *v2 = new UndoRecord(101, rid, CreateIntRecord(schema, rid, 20), 20);
  v2->Commit(30);
  chain.InsertVersion(v2);
  auto *v3 = new UndoRecord(102, rid, CreateIntRecord(schema, rid, 30), 30);
  v3->Commit(40);
  chain.InsertVersion(v3);

  auto before = chain.ResolveVisibility(35, 200);
  ASSERT_EQ(before.state, VersionVisibilityState::READ_UNDO);
  ASSERT_NE(before.undo_tuple, nullptr);
  EXPECT_EQ(dynamic_cast<const IntValue *>(before.undo_tuple->GetValueAt(0).get())->Get(), 30);

  EXPECT_EQ(chain.PruneVersions(35), 2);
  EXPECT_EQ(chain.GetVersionCount(), 1);

  auto after = chain.ResolveVisibility(35, 200);
  ASSERT_EQ(after.state, VersionVisibilityState::READ_UNDO);
  ASSERT_NE(after.undo_tuple, nullptr);
  EXPECT_EQ(dynamic_cast<const IntValue *>(after.undo_tuple->GetValueAt(0).get())->Get(), 30);
}

TEST(VersionChainTest, PruneVersionsNoOp) {
  VersionChain empty_chain;
  EXPECT_EQ(empty_chain.PruneVersions(4000), 0);
  EXPECT_EQ(empty_chain.GetVersionCount(), 0);

  VersionChain chain;
  RID rid(1, 0);
  std::vector<UndoRecord *> versions;

  for (int i = 0; i < 3; i++) {
    auto rec = CreateTestRecord();
    auto *undo = new UndoRecord(100 + i, rid, std::move(rec), 4000 + i * 1000);
    undo->Commit(4500 + i * 1000);
    chain.InsertVersion(undo);
    versions.push_back(undo);
  }

  EXPECT_EQ(chain.PruneVersions(4000), 0);
  EXPECT_EQ(chain.GetVersionCount(), 3);
  EXPECT_EQ(chain.GetHead(), versions[2]);
  EXPECT_EQ(versions[2]->GetPrev(), versions[1]);
  EXPECT_EQ(versions[1]->GetPrev(), versions[0]);
  EXPECT_EQ(versions[0]->GetPrev(), nullptr);
}

TEST(VersionChainTest, PruneRemovesAbortedRecordsWithoutChangingVisibility) {
  VersionChain chain;
  RID rid(1, 0);
  RTField field;
  field.field_.field_name_ = "value";
  field.field_.field_type_ = TYPE_INT;
  field.field_.field_size_ = sizeof(int32_t);
  RecordSchema schema({field});

  auto *committed = new UndoRecord(100, rid, CreateIntRecord(schema, rid, 10), 10);
  committed->Commit(50);
  chain.InsertVersion(committed);
  auto *aborted = new UndoRecord(101, rid, CreateIntRecord(schema, rid, 20), 50);
  aborted->Abort();
  chain.InsertVersion(aborted);
  auto *newest = new UndoRecord(102, rid, CreateIntRecord(schema, rid, 30), 50);
  newest->Commit(100);
  chain.InsertVersion(newest);

  auto before = chain.ResolveVisibility(75, 200);
  ASSERT_EQ(before.state, VersionVisibilityState::READ_UNDO);
  ASSERT_NE(before.undo_tuple, nullptr);
  EXPECT_EQ(dynamic_cast<const IntValue *>(before.undo_tuple->GetValueAt(0).get())->Get(), 30);

  EXPECT_EQ(chain.PruneVersions(75), 2);
  EXPECT_EQ(chain.GetVersionCount(), 1);

  auto after = chain.ResolveVisibility(75, 200);
  ASSERT_EQ(after.state, VersionVisibilityState::READ_UNDO);
  ASSERT_NE(after.undo_tuple, nullptr);
  EXPECT_EQ(dynamic_cast<const IntValue *>(after.undo_tuple->GetValueAt(0).get())->Get(), 30);
}

TEST(VersionChainTest, AbortedHeadReadsPhysicallyRestoredPageForNewSnapshot) {
  VersionChain chain;
  RID rid(1, 0);
  RTField field;
  field.field_.field_name_ = "value";
  field.field_.field_type_ = TYPE_INT;
  field.field_.field_size_ = sizeof(int32_t);
  RecordSchema schema({field});

  auto *aborted = new UndoRecord(101, rid, CreateIntRecord(schema, rid, 10), 20);
  aborted->Abort();
  chain.InsertVersion(aborted);

  auto visibility = chain.ResolveVisibility(30, 200);
  EXPECT_EQ(visibility.state, VersionVisibilityState::READ_PAGE);
  EXPECT_EQ(visibility.undo_tuple, nullptr);
}

TEST(VersionChainTest, NeedsGC) {
  VersionChain chain;
  RID rid(1, 0);

  auto add_committed_version = [&](int i) {
    auto rec = CreateTestRecord();
    auto *undo = new UndoRecord(100 + i, rid, std::move(rec), 1000 + i * 100);
    undo->Commit(1500 + i * 100);
    chain.InsertVersion(undo);
    return undo;
  };
  
  // Initially doesn't need GC
  EXPECT_FALSE(chain.NeedsGC());
  
  // GC threshold is 10: exactly 10 versions should not need GC.
  for (int i = 0; i < 10; i++) {
    add_committed_version(i);
  }
  EXPECT_FALSE(chain.NeedsGC());

  // 11 versions exceed the threshold.
  add_committed_version(10);
  EXPECT_TRUE(chain.NeedsGC());

  // After pruning below the threshold, NeedsGC should become false again.
  chain.PruneVersions(2300);
  EXPECT_FALSE(chain.NeedsGC());
}

//===--------------------------------------------------------------------===//
// VersionTable Tests
//===--------------------------------------------------------------------===//

TEST(VersionTableTest, GetOrCreateVersionChain) {
  VersionTable version_table;
  
  RID rid1(1, 0);
  RID rid2(1, 1);
  
  // Get version chains (should create them)
  auto chain1 = version_table.GetVersionChain(rid1);
  auto chain2 = version_table.GetVersionChain(rid2);
  
  EXPECT_NE(chain1, nullptr);
  EXPECT_NE(chain2, nullptr);
  EXPECT_NE(chain1, chain2);
  
  // Getting same RID again should return same chain
  AddCommittedVersions(chain1, rid1, 1, 100, 1000);
  auto chain1_again = version_table.GetVersionChain(rid1);
  EXPECT_EQ(chain1, chain1_again);
  EXPECT_EQ(chain1_again->GetVersionCount(), 1);
}

TEST(VersionTableTest, HasVersionChain) {
  VersionTable version_table;
  
  RID rid1(1, 0);
  RID rid2(1, 1);
  
  // Initially no chains
  EXPECT_FALSE(version_table.HasVersionChain(rid1));
  EXPECT_FALSE(version_table.HasVersionChain(rid2));
  
  // Create chain for rid1
  version_table.GetVersionChain(rid1);
  
  EXPECT_TRUE(version_table.HasVersionChain(rid1));
  EXPECT_FALSE(version_table.HasVersionChain(rid2));
}

TEST(VersionTableTest, RemoveVersionChain) {
  VersionTable version_table;
  
  RID rid(1, 0);
  // RID missing_rid(1, 1);

  // Removing a non-existing RID should be safe.
  // version_table.RemoveVersionChain(missing_rid);
  // EXPECT_FALSE(version_table.HasVersionChain(missing_rid));
  
  // Create chain
  auto chain = version_table.GetVersionChain(rid);
  AddCommittedVersions(chain, rid, 1, 100, 1000);
  EXPECT_TRUE(version_table.HasVersionChain(rid));
  EXPECT_EQ(chain->GetVersionCount(), 1);
  
  // Remove chain
  version_table.RemoveVersionChain(rid);
  EXPECT_FALSE(version_table.HasVersionChain(rid));
  EXPECT_EQ(chain->GetVersionCount(), 1);

  auto new_chain = version_table.GetVersionChain(rid);
  EXPECT_NE(new_chain, nullptr);
  EXPECT_NE(new_chain, chain);
  EXPECT_TRUE(version_table.HasVersionChain(rid));
  EXPECT_EQ(new_chain->GetVersionCount(), 0);
}

TEST(VersionTableTest, SameRIDInDifferentTablesHasIndependentChains) {
  VersionTable version_table;
  RID rid(1, 0);
  auto table_one = version_table.GetOrCreateVersionChain({1, rid});
  auto table_two = version_table.GetOrCreateVersionChain({2, rid});

  EXPECT_NE(table_one, table_two);
  AddCommittedVersions(table_one, rid, 1, 100, 1000);
  EXPECT_EQ(table_one->GetVersionCount(), 1);
  EXPECT_EQ(table_two->GetVersionCount(), 0);
  EXPECT_EQ(version_table.GetAllKeys().size(), 2);
}

TEST(VersionTableTest, RemovedChainHandleRemainsSafeDuringConcurrentRecreate) {
  VersionTable version_table;
  VersionKey key{1, RID(1, 0)};
  auto original = version_table.GetOrCreateVersionChain(key);
  AddCommittedVersions(original, key.rid, 1, 100, 1000);

  std::atomic<bool> start{false};
  std::thread reader([&] {
    while (!start.load()) {
    }
    for (int i = 0; i < 1000; i++) {
      EXPECT_EQ(original->GetVersionCount(), 1);
    }
  });
  std::thread replacer([&] {
    start.store(true);
    for (int i = 0; i < 1000; i++) {
      version_table.RemoveVersionChain(key);
      auto replacement = version_table.GetOrCreateVersionChain(key);
      EXPECT_NE(replacement, original);
    }
  });

  reader.join();
  replacer.join();
  EXPECT_EQ(original->GetVersionCount(), 1);
}

TEST(VersionTableTest, GetAllRIDs) {
  VersionTable version_table;
  
  RID rid1(1, 0);
  RID rid2(1, 1);
  RID rid3(2, 0);

  EXPECT_TRUE(version_table.GetAllRIDs().empty());
  
  version_table.GetVersionChain(rid1);
  version_table.GetVersionChain(rid1);
  auto rids = version_table.GetAllRIDs();
  EXPECT_EQ(rids.size(), 1);
  EXPECT_TRUE(std::find(rids.begin(), rids.end(), rid1) != rids.end());
  
  // Create chains
  version_table.GetVersionChain(rid2);
  version_table.GetVersionChain(rid3);
  
  // Get all RIDs
  rids = version_table.GetAllRIDs();
  EXPECT_EQ(rids.size(), 3);
  
  // Check all RIDs are present
  EXPECT_TRUE(std::find(rids.begin(), rids.end(), rid1) != rids.end());
  EXPECT_TRUE(std::find(rids.begin(), rids.end(), rid2) != rids.end());
  EXPECT_TRUE(std::find(rids.begin(), rids.end(), rid3) != rids.end());

  version_table.RemoveVersionChain(rid2);
  rids = version_table.GetAllRIDs();
  EXPECT_EQ(rids.size(), 2);
  EXPECT_TRUE(std::find(rids.begin(), rids.end(), rid1) != rids.end());
  EXPECT_TRUE(std::find(rids.begin(), rids.end(), rid2) == rids.end());
  EXPECT_TRUE(std::find(rids.begin(), rids.end(), rid3) != rids.end());
}

TEST(VersionTableTest, GetStats) {
  VersionTable version_table;

  auto stats = version_table.GetStats();
  EXPECT_EQ(stats.total_chains, 0);
  EXPECT_EQ(stats.total_versions, 0);
  EXPECT_EQ(stats.chains_needing_gc, 0);
  
  RID rid1(1, 0);
  RID rid2(1, 1);
  
  // Create chains and add versions
  auto chain1 = version_table.GetVersionChain(rid1);
  auto chain2 = version_table.GetVersionChain(rid2);
  
  AddCommittedVersions(chain1, rid1, 5, 100, 1000);
  AddCommittedVersions(chain2, rid2, 3, 200, 2000);
  
  // Get stats
  stats = version_table.GetStats();
  EXPECT_EQ(stats.total_chains, 2);
  EXPECT_EQ(stats.total_versions, 8);  // 5 + 3
  EXPECT_EQ(stats.chains_needing_gc, 0);  // None exceed threshold of 10

  version_table.RemoveVersionChain(rid2);
  stats = version_table.GetStats();
  EXPECT_EQ(stats.total_chains, 1);
  EXPECT_EQ(stats.total_versions, 5);
  EXPECT_EQ(stats.chains_needing_gc, 0);

  chain1->PruneVersions(1300);
  stats = version_table.GetStats();
  EXPECT_EQ(stats.total_chains, 1);
  EXPECT_EQ(stats.total_versions, 2);
  EXPECT_EQ(stats.chains_needing_gc, 0);
}

TEST(VersionTableTest, StatsWithGCNeeded) {
  VersionTable version_table;
  
  RID rid1(1, 0);
  RID rid2(1, 1);
  RID rid3(2, 0);
  auto chain1 = version_table.GetVersionChain(rid1);
  auto chain2 = version_table.GetVersionChain(rid2);
  auto chain3 = version_table.GetVersionChain(rid3);

  AddCommittedVersions(chain1, rid1, 11, 100, 1000);
  AddCommittedVersions(chain2, rid2, 10, 200, 3000);
  AddCommittedVersions(chain3, rid3, 1, 300, 5000);
  
  auto stats = version_table.GetStats();
  EXPECT_EQ(stats.total_chains, 3);
  EXPECT_EQ(stats.total_versions, 22);
  EXPECT_EQ(stats.chains_needing_gc, 1);

  chain1->PruneVersions(1800);
  stats = version_table.GetStats();
  EXPECT_EQ(stats.total_chains, 3);
  EXPECT_EQ(stats.total_versions, 14);
  EXPECT_EQ(stats.chains_needing_gc, 0);
}

//===--------------------------------------------------------------------===//
// Integration Tests
//===--------------------------------------------------------------------===//

TEST(VersionChainIntegrationTest, UndoChainWorkflow) {
  RID rid(1, 0);
  VersionChain chain;
  RTField field;
  field.field_.field_name_ = "value";
  field.field_.field_type_ = TYPE_INT;
  field.field_.field_size_ = sizeof(int32_t);
  RecordSchema schema({field});

  auto *oldest = new UndoRecord(100, rid, nullptr, 5);
  oldest->Commit(10);
  chain.InsertVersion(oldest);
  auto *first_update = new UndoRecord(101, rid, CreateIntRecord(schema, rid, 10), 10);
  first_update->Commit(20);
  chain.InsertVersion(first_update);
  auto *second_update = new UndoRecord(102, rid, CreateIntRecord(schema, rid, 20), 20);
  second_update->Commit(30);
  chain.InsertVersion(second_update);

  EXPECT_EQ(chain.ResolveVisibility(5, 200).state, VersionVisibilityState::NOT_VISIBLE);
  auto at_15 = chain.ResolveVisibility(15, 201);
  ASSERT_EQ(at_15.state, VersionVisibilityState::READ_UNDO);
  EXPECT_EQ(dynamic_cast<const IntValue *>(at_15.undo_tuple->GetValueAt(0).get())->Get(), 10);
  auto at_25 = chain.ResolveVisibility(25, 202);
  ASSERT_EQ(at_25.state, VersionVisibilityState::READ_UNDO);
  EXPECT_EQ(dynamic_cast<const IntValue *>(at_25.undo_tuple->GetValueAt(0).get())->Get(), 20);
  EXPECT_EQ(chain.ResolveVisibility(35, 203).state, VersionVisibilityState::READ_PAGE);

  EXPECT_EQ(chain.PruneVersions(25), 2);
  auto after_prune = chain.ResolveVisibility(25, 204);
  ASSERT_EQ(after_prune.state, VersionVisibilityState::READ_UNDO);
  EXPECT_EQ(dynamic_cast<const IntValue *>(after_prune.undo_tuple->GetValueAt(0).get())->Get(), 20);
}

}  // namespace njudb

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
