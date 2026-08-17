#include <atomic>
#include <filesystem>
#include <thread>

#include <gtest/gtest.h>

#include "concurrency/timestamp_oracle.h"
#include "concurrency/txn_manager.h"
#include "concurrency/version_manager.h"
#include "concurrency/garbage_collector.h"
#include "concurrency/mvcc_write_context.h"
#include "execution/mvcc_write_coordinator.h"
#include "storage/buffer/buffer_pool_manager.h"
#include "storage/disk/disk_manager.h"
#include "system/handle/database_handle.h"
#include "system/index/index_manager.h"
#include "system/table/table_manager.h"

namespace njudb {

class MVCCAtomicWriteTest : public ::testing::Test
{
protected:
  void SetUp() override
  {
    test_dir_ = "./mvcc_atomic_write_" + std::to_string(std::rand());
    std::filesystem::remove_all(test_dir_);
    std::filesystem::create_directories(test_dir_);

    DiskManager::CreateFile(FILE_NAME(test_dir_, test_dir_, DB_SUFFIX));
    auto db_fd = disk_manager_.OpenFile(FILE_NAME(test_dir_, test_dir_, DB_SUFFIX));
    size_t zero = 0;
    disk_manager_.WriteFile(db_fd, reinterpret_cast<const char *>(&zero), sizeof(size_t), 0, SEEK_CUR);
    disk_manager_.WriteFile(db_fd, reinterpret_cast<const char *>(&zero), sizeof(size_t), 0, SEEK_CUR);
    disk_manager_.CloseFile(db_fd);

    db_ = std::make_unique<DatabaseHandle>(
        test_dir_, &disk_manager_, &table_manager_, &index_manager_, ConcurrencyProtocol::MVCC);
    db_->SetVersionManager(&version_manager_);
    db_->Acquire();

    RTField id;
    id.field_.field_name_ = "id";
    id.field_.field_type_ = TYPE_INT;
    id.field_.field_size_ = sizeof(int);

    RTField alt;
    alt.field_.field_name_ = "alt";
    alt.field_.field_type_ = TYPE_INT;
    alt.field_.field_size_ = sizeof(int);

    RTField value;
    value.field_.field_name_ = "value";
    value.field_.field_type_ = TYPE_INT;
    value.field_.field_size_ = sizeof(int);

    RecordSchema schema({id, alt, value});
    db_->CreateTable("items", schema, NARY_MODEL);
    table_ = db_->GetTable("items");

    RecordSchema id_key({table_->GetSchema().GetFieldAt(0)});
    db_->CreateIndex("items_id", "items", id_key, IndexType::BPTREE);
    id_index_ = db_->GetIndexes(table_->GetTableId()).back();

    RecordSchema alt_key({table_->GetSchema().GetFieldAt(1)});
    db_->CreateIndex("items_alt", "items", alt_key, IndexType::BPTREE);
    auto indexes = db_->GetIndexes(table_->GetTableId());
    alt_index_ = indexes.back();
    indexes_ = {id_index_, alt_index_};
  }

  void TearDown() override
  {
    if (db_ != nullptr) {
      db_->Release();
      db_.reset();
    }
    std::filesystem::remove_all(test_dir_);
  }

  auto MakeRecord(int id, int alt, int value, RID rid = INVALID_RID) -> RecordUptr
  {
    std::vector<ValueSptr> values{
        ValueFactory::CreateIntValue(id),
        ValueFactory::CreateIntValue(alt),
        ValueFactory::CreateIntValue(value),
    };
    return std::make_unique<Record>(&table_->GetSchema(), values, rid);
  }

  auto MakeRecordWithNullAlt(int id, int value) -> RecordUptr
  {
    std::vector<ValueSptr> values{
        ValueFactory::CreateIntValue(id),
        ValueFactory::CreateNullValue(TYPE_INT),
        ValueFactory::CreateIntValue(value),
    };
    return std::make_unique<Record>(&table_->GetSchema(), values, INVALID_RID);
  }

  auto InsertInitial(int id, int alt, int value) -> RecordUptr
  {
    auto record = MakeRecord(id, alt, value);
    record->SetRID(table_->InsertRecord(*record));
    for (auto *index : indexes_) {
      index->InsertRecord(*record);
    }
    return record;
  }

  auto Begin(txn_id_t txn_id, ConcurrencyProtocol protocol = ConcurrencyProtocol::MVCC) -> Transaction *
  {
    txn_manager_.Begin(txn_id, IsolationLevel::REPEATABLE_READ, protocol);
    return txn_manager_.GetTransaction(txn_id);
  }

  void ExpectNoBookkeeping(Transaction *txn)
  {
    EXPECT_TRUE(txn->GetUndoRecords().empty());
    EXPECT_TRUE(txn->GetWriteSet().empty());
    EXPECT_TRUE(txn->GetIndexWriteSet().empty());
  }

  std::string test_dir_;
  DiskManager disk_manager_;
  BufferPoolManager buffer_pool_manager_{&disk_manager_, nullptr};
  TableManager table_manager_{&disk_manager_, &buffer_pool_manager_};
  IndexManager index_manager_{&disk_manager_, &buffer_pool_manager_};
  TimestampOracle timestamp_oracle_;
  TxnManager txn_manager_{nullptr, nullptr, &timestamp_oracle_};
  VersionManager version_manager_;
  std::unique_ptr<DatabaseHandle> db_;
  TableHandle *table_{nullptr};
  IndexHandle *id_index_{nullptr};
  IndexHandle *alt_index_{nullptr};
  std::list<IndexHandle *> indexes_;
};

TEST_F(MVCCAtomicWriteTest, SuccessfulInsertUpdateDeleteRegisterOneCompleteWrite)
{
  auto *insert_txn = Begin(1);
  auto inserted = MakeRecord(1, 11, 100);
  auto rid = MVCCWriteCoordinator::Insert(table_, indexes_, *inserted, insert_txn, &version_manager_);
  EXPECT_EQ(insert_txn->GetUndoRecords().size(), 1);
  EXPECT_EQ(insert_txn->GetWriteSet().size(), 1);
  EXPECT_EQ(insert_txn->GetIndexWriteSet().size(), 2);
  EXPECT_TRUE(id_index_->CheckRecordExists(*inserted));
  txn_manager_.Commit(1);

  auto *update_txn = Begin(2);
  auto updated = MakeRecord(2, 22, 200, rid);
  MVCCWriteCoordinator::Update(table_, indexes_, *inserted, *updated, update_txn, &version_manager_);
  EXPECT_EQ(update_txn->GetUndoRecords().size(), 1);
  EXPECT_EQ(update_txn->GetWriteSet().size(), 1);
  EXPECT_EQ(update_txn->GetIndexWriteSet().size(), 2);
  EXPECT_TRUE(*table_->GetRecord(rid, nullptr) == *updated);
  EXPECT_FALSE(id_index_->CheckRecordExists(*inserted));
  EXPECT_TRUE(id_index_->CheckRecordExists(*updated));
  txn_manager_.Commit(2);

  auto *delete_txn = Begin(3);
  MVCCWriteCoordinator::Delete(table_, indexes_, *updated, delete_txn, &version_manager_);
  EXPECT_EQ(delete_txn->GetUndoRecords().size(), 1);
  EXPECT_EQ(delete_txn->GetWriteSet().size(), 1);
  EXPECT_EQ(delete_txn->GetIndexWriteSet().size(), 0);
  EXPECT_THROW(table_->GetRecord(rid, delete_txn), NJUDBException_);
  EXPECT_TRUE(*table_->GetRecord(rid, nullptr) == *updated);
  EXPECT_TRUE(id_index_->CheckRecordExists(*updated));
  txn_manager_.Commit(3);
}

TEST_F(MVCCAtomicWriteTest, UpdateConflictIsRejectedBeforePageAndIndexMutation)
{
  auto original = InsertInitial(10, 110, 1000);
  auto first = MakeRecord(11, 111, 1001, original->GetRID());
  auto second = MakeRecord(12, 112, 1002, original->GetRID());

  auto *winner = Begin(10);
  MVCCWriteCoordinator::Update(table_, indexes_, *original, *first, winner, &version_manager_);

  auto *loser = Begin(11);
  EXPECT_THROW(
      MVCCWriteCoordinator::Update(table_, indexes_, *original, *second, loser, &version_manager_),
      NJUDBException_);

  EXPECT_TRUE(*table_->GetRecord(original->GetRID(), nullptr) == *first);
  EXPECT_TRUE(id_index_->CheckRecordExists(*first));
  EXPECT_FALSE(id_index_->CheckRecordExists(*second));
  ExpectNoBookkeeping(loser);
  txn_manager_.Abort(10, db_.get());
  txn_manager_.Abort(11);
}

TEST_F(MVCCAtomicWriteTest, DeleteConflictIsRejectedBeforePhysicalDelete)
{
  auto original = InsertInitial(20, 120, 2000);
  auto first = MakeRecord(21, 121, 2001, original->GetRID());

  auto *winner = Begin(20);
  MVCCWriteCoordinator::Update(table_, indexes_, *original, *first, winner, &version_manager_);

  auto *loser = Begin(21);
  EXPECT_THROW(
      MVCCWriteCoordinator::Delete(table_, indexes_, *first, loser, &version_manager_),
      NJUDBException_);

  EXPECT_TRUE(*table_->GetRecord(original->GetRID(), nullptr) == *first);
  EXPECT_TRUE(id_index_->CheckRecordExists(*first));
  ExpectNoBookkeeping(loser);
  txn_manager_.Abort(20, db_.get());
  txn_manager_.Abort(21);
}

TEST_F(MVCCAtomicWriteTest, InsertIndexFailureRestoresPageEarlierIndexAndBookkeeping)
{
  auto *txn = Begin(30);
  auto invalid = MakeRecordWithNullAlt(30, 3000);

  EXPECT_THROW(
      MVCCWriteCoordinator::Insert(table_, indexes_, *invalid, txn, &version_manager_),
      NJUDBException_);

  EXPECT_THROW(table_->GetRecord(invalid->GetRID(), nullptr), NJUDBException_);
  EXPECT_FALSE(id_index_->CheckRecordExists(*invalid));
  ExpectNoBookkeeping(txn);
  auto chain = version_manager_.GetVersionChain(table_->GetTableId(), invalid->GetRID());
  ASSERT_NE(chain, nullptr);
  EXPECT_EQ(chain->GetVersionCount(), 0);

  auto valid = MakeRecord(30, 130, 3000);
  EXPECT_NO_THROW(MVCCWriteCoordinator::Insert(table_, indexes_, *valid, txn, &version_manager_));
  EXPECT_EQ(txn->GetUndoRecords().size(), 1);
  EXPECT_TRUE(id_index_->CheckRecordExists(*valid));
  txn_manager_.Commit(30);
}

TEST_F(MVCCAtomicWriteTest, LaterIndexUpdateFailureRestoresPageAndEarlierIndex)
{
  auto original = InsertInitial(40, 140, 4000);
  auto blocker = InsertInitial(41, 141, 4100);
  auto invalid = MakeRecord(42, 141, 4200, original->GetRID());
  auto *txn = Begin(40);

  EXPECT_THROW(
      MVCCWriteCoordinator::Update(table_, indexes_, *original, *invalid, txn, &version_manager_),
      NJUDBException_);

  EXPECT_TRUE(*table_->GetRecord(original->GetRID(), nullptr) == *original);
  EXPECT_TRUE(id_index_->CheckRecordExists(*original));
  EXPECT_FALSE(id_index_->CheckRecordExists(*invalid));
  EXPECT_TRUE(alt_index_->CheckRecordExists(*blocker));
  ExpectNoBookkeeping(txn);
  auto chain = version_manager_.GetVersionChain(table_->GetTableId(), original->GetRID());
  ASSERT_NE(chain, nullptr);
  EXPECT_EQ(chain->GetVersionCount(), 0);
  txn_manager_.Abort(40);
}

TEST_F(MVCCAtomicWriteTest, PageMutationFailureRemovesReservedVersion)
{
  auto original = MakeRecord(45, 145, 4500);
  original->SetRID(table_->InsertRecord(*original));
  table_->DeleteRecord(original->GetRID());
  auto updated = MakeRecord(46, 146, 4600, original->GetRID());
  auto *txn = Begin(45);

  EXPECT_THROW(
      MVCCWriteCoordinator::Update(table_, {}, *original, *updated, txn, &version_manager_),
      NJUDBException_);

  ExpectNoBookkeeping(txn);
  auto chain = version_manager_.GetVersionChain(table_->GetTableId(), original->GetRID());
  ASSERT_NE(chain, nullptr);
  EXPECT_EQ(chain->GetVersionCount(), 0);
  txn_manager_.Abort(45);
}

TEST_F(MVCCAtomicWriteTest, FailedWritePreservesEarlierSuccessfulBookkeeping)
{
  auto *txn = Begin(46);
  auto valid = MakeRecord(46, 146, 4600);
  MVCCWriteCoordinator::Insert(table_, indexes_, *valid, txn, &version_manager_);
  auto invalid = MakeRecordWithNullAlt(47, 4700);

  EXPECT_THROW(
      MVCCWriteCoordinator::Insert(table_, indexes_, *invalid, txn, &version_manager_),
      NJUDBException_);

  EXPECT_EQ(txn->GetUndoRecords().size(), 1);
  EXPECT_EQ(txn->GetWriteSet().size(), 1);
  EXPECT_EQ(txn->GetIndexWriteSet().size(), 2);
  EXPECT_TRUE(*table_->GetRecord(valid->GetRID(), nullptr) == *valid);
  EXPECT_TRUE(id_index_->CheckRecordExists(*valid));
  EXPECT_THROW(table_->GetRecord(invalid->GetRID(), nullptr), NJUDBException_);
  txn_manager_.Commit(46);
}

TEST_F(MVCCAtomicWriteTest, SameTransactionRepeatedUpdateReusesFirstUndoRecord)
{
  auto original = InsertInitial(47, 147, 4700);
  auto first = MakeRecord(48, 148, 4800, original->GetRID());
  auto second = MakeRecord(49, 149, 4900, original->GetRID());
  auto *writer = Begin(47);
  auto *reader = Begin(48);

  MVCCWriteCoordinator::Update(table_, indexes_, *original, *first, writer, &version_manager_);
  auto *first_undo = writer->GetUndoRecords().front();
  MVCCWriteCoordinator::Update(table_, indexes_, *first, *second, writer, &version_manager_);

  ASSERT_EQ(writer->GetUndoRecords().size(), 1);
  EXPECT_EQ(writer->GetUndoRecords().front(), first_undo);
  EXPECT_EQ(version_manager_.GetVersionChain(table_->GetTableId(), original->GetRID())->GetVersionCount(), 1);
  ASSERT_NE(first_undo->GetTuple(), nullptr);
  EXPECT_TRUE(*first_undo->GetTuple() == *original);
  EXPECT_TRUE(*table_->GetRecord(original->GetRID(), writer) == *second);
  EXPECT_TRUE(*table_->GetRecord(original->GetRID(), reader) == *original);
  EXPECT_EQ(writer->GetWriteSet().size(), 2);
  EXPECT_EQ(writer->GetIndexWriteSet().size(), 4);

  txn_manager_.Commit(47);
  txn_manager_.Commit(48);

  auto *new_reader = Begin(52);
  EXPECT_TRUE(*table_->GetRecord(original->GetRID(), new_reader) == *second);
  EXPECT_FALSE(id_index_->CheckRecordExists(*original));
  EXPECT_FALSE(id_index_->CheckRecordExists(*first));
  EXPECT_TRUE(id_index_->CheckRecordExists(*second));
  txn_manager_.Commit(52);
}

TEST_F(MVCCAtomicWriteTest, FailedRepeatedWritePreservesFirstWriteAndReusableUndo)
{
  auto original = InsertInitial(50, 150, 5000);
  auto blocker = InsertInitial(51, 151, 5100);
  auto first = MakeRecord(52, 152, 5200, original->GetRID());
  auto invalid = MakeRecord(53, 151, 5300, original->GetRID());
  auto *txn = Begin(49);

  MVCCWriteCoordinator::Update(table_, indexes_, *original, *first, txn, &version_manager_);
  auto *undo = txn->GetUndoRecords().front();
  EXPECT_THROW(
      MVCCWriteCoordinator::Update(table_, indexes_, *first, *invalid, txn, &version_manager_),
      NJUDBException_);

  ASSERT_EQ(txn->GetUndoRecords().size(), 1);
  EXPECT_EQ(txn->GetUndoRecords().front(), undo);
  EXPECT_FALSE(undo->IsDeleted());
  EXPECT_TRUE(*table_->GetRecord(original->GetRID(), nullptr) == *first);
  EXPECT_TRUE(id_index_->CheckRecordExists(*first));
  EXPECT_FALSE(id_index_->CheckRecordExists(*invalid));
  EXPECT_TRUE(alt_index_->CheckRecordExists(*blocker));
  EXPECT_EQ(txn->GetWriteSet().size(), 1);
  EXPECT_EQ(txn->GetIndexWriteSet().size(), 2);
  txn_manager_.Abort(49, db_.get());
}

TEST_F(MVCCAtomicWriteTest, RepeatedUpdateAbortRestoresOriginalTableAndIndexes)
{
  auto original = InsertInitial(60, 160, 6000);
  auto first = MakeRecord(61, 161, 6100, original->GetRID());
  auto second = MakeRecord(62, 162, 6200, original->GetRID());
  auto *txn = Begin(60);

  MVCCWriteCoordinator::Update(table_, indexes_, *original, *first, txn, &version_manager_);
  MVCCWriteCoordinator::Update(table_, indexes_, *first, *second, txn, &version_manager_);
  auto *undo = txn->GetUndoRecords().front();
  txn_manager_.Abort(60, db_.get());

  EXPECT_EQ(undo->GetCommitTimestamp(), ABORTED_TS);
  EXPECT_TRUE(*table_->GetRecord(original->GetRID(), nullptr) == *original);
  EXPECT_TRUE(id_index_->CheckRecordExists(*original));
  EXPECT_FALSE(id_index_->CheckRecordExists(*first));
  EXPECT_FALSE(id_index_->CheckRecordExists(*second));
}

TEST_F(MVCCAtomicWriteTest, UpdateThenDeleteAbortRestoresOriginalTableAndIndexes)
{
  auto original = InsertInitial(70, 170, 7000);
  auto updated = MakeRecord(71, 171, 7100, original->GetRID());
  auto *txn = Begin(70);

  MVCCWriteCoordinator::Update(table_, indexes_, *original, *updated, txn, &version_manager_);
  MVCCWriteCoordinator::Delete(table_, indexes_, *updated, txn, &version_manager_);
  ASSERT_EQ(txn->GetUndoRecords().size(), 1);
  auto *undo = txn->GetUndoRecords().front();
  EXPECT_TRUE(undo->IsDeleted());
  txn_manager_.Abort(70, db_.get());

  EXPECT_EQ(undo->GetCommitTimestamp(), ABORTED_TS);
  EXPECT_TRUE(*table_->GetRecord(original->GetRID(), nullptr) == *original);
  EXPECT_TRUE(id_index_->CheckRecordExists(*original));
  EXPECT_FALSE(id_index_->CheckRecordExists(*updated));
}

TEST_F(MVCCAtomicWriteTest, InsertThenUpdateAbortRemovesTupleAndIndexes)
{
  auto inserted = MakeRecord(80, 180, 8000);
  auto *txn = Begin(80);
  auto rid = MVCCWriteCoordinator::Insert(table_, indexes_, *inserted, txn, &version_manager_);
  auto updated = MakeRecord(81, 181, 8100, rid);
  MVCCWriteCoordinator::Update(table_, indexes_, *inserted, *updated, txn, &version_manager_);

  ASSERT_EQ(txn->GetUndoRecords().size(), 1);
  EXPECT_EQ(txn->GetUndoRecords().front()->GetTuple(), nullptr);
  txn_manager_.Abort(80, db_.get());

  EXPECT_THROW(table_->GetRecord(rid, nullptr), NJUDBException_);
  EXPECT_FALSE(id_index_->CheckRecordExists(*inserted));
  EXPECT_FALSE(id_index_->CheckRecordExists(*updated));
}

TEST_F(MVCCAtomicWriteTest, OtherTransactionStillConflictsAfterRepeatedWriterUpdates)
{
  auto original = InsertInitial(90, 190, 9000);
  auto first = MakeRecord(91, 191, 9100, original->GetRID());
  auto second = MakeRecord(92, 192, 9200, original->GetRID());
  auto other = MakeRecord(93, 193, 9300, original->GetRID());
  auto *writer = Begin(90);
  auto *competitor = Begin(91);

  MVCCWriteCoordinator::Update(table_, indexes_, *original, *first, writer, &version_manager_);
  MVCCWriteCoordinator::Update(table_, indexes_, *first, *second, writer, &version_manager_);
  EXPECT_THROW(
      MVCCWriteCoordinator::Update(table_, indexes_, *original, *other, competitor, &version_manager_),
      NJUDBException_);
  ExpectNoBookkeeping(competitor);

  txn_manager_.Abort(90, db_.get());
  txn_manager_.Abort(91, db_.get());
}

TEST_F(MVCCAtomicWriteTest, LogicalDeleteKeepsRIDUntilOldSnapshotEndsAndGCReclaims)
{
  auto original = InsertInitial(100, 200, 10000);
  const auto deleted_rid = original->GetRID();
  auto *old_reader = Begin(100);
  auto *deleter = Begin(101);

  MVCCWriteCoordinator::Delete(table_, indexes_, *original, deleter, &version_manager_);
  EXPECT_THROW(table_->GetRecord(deleted_rid, deleter), NJUDBException_);
  EXPECT_TRUE(*table_->GetRecord(deleted_rid, old_reader) == *original);
  EXPECT_TRUE(*table_->GetRecord(deleted_rid, nullptr) == *original);
  EXPECT_TRUE(id_index_->CheckRecordExists(*original));
  txn_manager_.Commit(101);

  auto *new_reader = Begin(102);
  EXPECT_THROW(table_->GetRecord(deleted_rid, new_reader), NJUDBException_);
  txn_manager_.Commit(102);

  GarbageCollector gc(&version_manager_, &timestamp_oracle_);
  gc.CollectGarbage();
  EXPECT_EQ(gc.GetTotalReclaimedTuples(), 0);
  EXPECT_TRUE(*table_->GetRecord(deleted_rid, old_reader) == *original);
  EXPECT_TRUE(*table_->GetRecord(deleted_rid, nullptr) == *original);
  EXPECT_NE(version_manager_.GetVersionChain(table_->GetTableId(), deleted_rid), nullptr);

  auto *blocked_insert_txn = Begin(103);
  auto blocked_insert = MakeRecord(100, 200, 10100);
  EXPECT_THROW(
      MVCCWriteCoordinator::Insert(table_, indexes_, *blocked_insert, blocked_insert_txn, &version_manager_),
      NJUDBException_);
  ExpectNoBookkeeping(blocked_insert_txn);
  txn_manager_.Abort(103, db_.get());

  txn_manager_.Commit(100);
  gc.CollectGarbage();
  EXPECT_EQ(gc.GetTotalReclaimedTuples(), 1);
  EXPECT_THROW(table_->GetRecord(deleted_rid, nullptr), NJUDBException_);
  EXPECT_EQ(version_manager_.GetVersionChain(table_->GetTableId(), deleted_rid), nullptr);
  EXPECT_FALSE(id_index_->CheckRecordExists(*original));

  auto *reinsert_txn = Begin(104);
  auto reinserted = MakeRecord(100, 200, 10200);
  auto reused = MVCCWriteCoordinator::Insert(table_, indexes_, *reinserted, reinsert_txn, &version_manager_);
  EXPECT_EQ(reused, deleted_rid);
  EXPECT_TRUE(id_index_->CheckRecordExists(*reinserted));
  txn_manager_.Commit(104);
}

TEST_F(MVCCAtomicWriteTest, AbortedLogicalDeleteIsNeverReclaimed)
{
  auto original = InsertInitial(110, 210, 11000);
  auto *deleter = Begin(110);
  MVCCWriteCoordinator::Delete(table_, indexes_, *original, deleter, &version_manager_);
  txn_manager_.Abort(110, db_.get());

  GarbageCollector gc(&version_manager_, &timestamp_oracle_);
  gc.CollectGarbage();

  EXPECT_EQ(gc.GetTotalReclaimedTuples(), 0);
  EXPECT_TRUE(*table_->GetRecord(original->GetRID(), nullptr) == *original);
  auto *reader = Begin(111);
  EXPECT_TRUE(*table_->GetRecord(original->GetRID(), reader) == *original);
  EXPECT_TRUE(id_index_->CheckRecordExists(*original));
  txn_manager_.Commit(111);
}

TEST_F(MVCCAtomicWriteTest, ConcurrentWritersProduceExactlyOneCompleteWrite)
{
  auto original = InsertInitial(50, 150, 5000);
  auto first = MakeRecord(51, 151, 5100, original->GetRID());
  auto second = MakeRecord(52, 152, 5200, original->GetRID());
  auto *txn1 = Begin(50);
  auto *txn2 = Begin(51);
  std::atomic<int> ready{0};
  std::atomic<int> success{0};

  auto write = [&](Transaction *txn, const Record &next) {
    ready.fetch_add(1);
    while (ready.load() != 2) {
      std::this_thread::yield();
    }
    try {
      MVCCWriteCoordinator::Update(table_, indexes_, *original, next, txn, &version_manager_);
      success.fetch_add(1);
    } catch (const NJUDBException_ &) {
    }
  };

  std::thread t1(write, txn1, std::cref(*first));
  std::thread t2(write, txn2, std::cref(*second));
  t1.join();
  t2.join();

  EXPECT_EQ(success.load(), 1);
  EXPECT_EQ(txn1->GetUndoRecords().size() + txn2->GetUndoRecords().size(), 1);
  EXPECT_EQ(txn1->GetWriteSet().size() + txn2->GetWriteSet().size(), 1);
  EXPECT_EQ(txn1->GetIndexWriteSet().size() + txn2->GetIndexWriteSet().size(), 2);
  auto page = table_->GetRecord(original->GetRID(), nullptr);
  EXPECT_TRUE(*page == *first || *page == *second);
  EXPECT_EQ(id_index_->CheckRecordExists(*first) + id_index_->CheckRecordExists(*second), 1);

  txn_manager_.Abort(50, db_.get());
  txn_manager_.Abort(51, db_.get());
}

TEST_F(MVCCAtomicWriteTest, CoordinatorRejectsTwoPhaseLockingTransactionWithoutMutation)
{
  auto *txn = Begin(60, ConcurrencyProtocol::TWO_PHASE_LOCKING);
  auto record = MakeRecord(60, 160, 6000);

  EXPECT_THROW(
      MVCCWriteCoordinator::Insert(table_, indexes_, *record, txn, &version_manager_),
      NJUDBException_);
  EXPECT_EQ(table_->GetFirstRID(), INVALID_RID);
  ExpectNoBookkeeping(txn);
  txn_manager_.Abort(60);
}

TEST_F(MVCCAtomicWriteTest, DirectMVCCPageWritesAreRejectedWithoutMutation)
{
  auto *txn = Begin(70);
  auto record = MakeRecord(70, 170, 7000);

  EXPECT_THROW(table_->InsertRecord(*record, txn), NJUDBException_);
  EXPECT_EQ(table_->GetFirstRID(), INVALID_RID);
  ExpectNoBookkeeping(txn);
  txn_manager_.Abort(70);
}

TEST_F(MVCCAtomicWriteTest, InsertVersionIsReservedBeforeTupleBecomesVisible)
{
  auto *writer = Begin(80);
  auto *reader = Begin(81);
  auto record = MakeRecord(80, 180, 8000);
  std::atomic<bool> version_reserved{false};
  std::atomic<bool> reader_checked{false};
  RID rid = INVALID_RID;

  std::thread insert_thread([&] {
    MVCCWriteContext::Scope write_scope(writer);
    rid = table_->InsertRecordBeforePageWrite(*record, writer, [&](const RID &allocated_rid) {
      rid = allocated_rid;
      version_manager_.PrepareVersion(writer, table_->GetTableId(), allocated_rid, nullptr, false);
      version_reserved.store(true);
      while (!reader_checked.load()) {
        std::this_thread::yield();
      }
    });
  });

  while (!version_reserved.load()) {
    std::this_thread::yield();
  }
  auto chain = version_manager_.GetVersionChain(table_->GetTableId(), rid);
  EXPECT_NE(chain, nullptr);
  if (chain != nullptr) {
    EXPECT_EQ(chain->GetVersionCount(), 1);
  }
  EXPECT_THROW(table_->GetRecord(rid, reader), NJUDBException_);
  reader_checked.store(true);
  insert_thread.join();

  EXPECT_NE(rid, INVALID_RID);
  EXPECT_TRUE(*table_->GetRecord(rid, writer) == *record);
  txn_manager_.Abort(80, db_.get());
  txn_manager_.Abort(81);
}

}  // namespace njudb
