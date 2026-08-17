/*------------------------------------------------------------------------------
 - Copyright (c) 2024. Websoft research group, Nanjing University.
 -
 - This program is free software: you can redistribute it and/or modify
 - it under the terms of the GNU General Public License as published by
 - the Free Software Foundation, either version 3 of the License, or
 - (at your option) any later version.
 -----------------------------------------------------------------------------*/

#include <filesystem>

#include <gtest/gtest.h>

#include "concurrency/lock_manager.h"
#include "concurrency/txn_manager.h"
#include "storage/buffer/buffer_pool_manager.h"
#include "storage/disk/disk_manager.h"
#include "system/handle/database_handle.h"
#include "system/index/index_manager.h"
#include "system/table/table_manager.h"

namespace njudb {

class TwoPLIndexAbortTest : public ::testing::Test
{
protected:
  void SetUp() override
  {
    test_dir_ = "./two_pl_index_abort_" + std::to_string(std::rand());
    std::filesystem::remove_all(test_dir_);
    std::filesystem::create_directories(test_dir_);

    DiskManager::CreateFile(FILE_NAME(test_dir_, test_dir_, DB_SUFFIX));
    auto db_fd = disk_manager_.OpenFile(FILE_NAME(test_dir_, test_dir_, DB_SUFFIX));
    size_t zero = 0;
    disk_manager_.WriteFile(db_fd, reinterpret_cast<const char *>(&zero), sizeof(size_t), 0, SEEK_CUR);
    disk_manager_.WriteFile(db_fd, reinterpret_cast<const char *>(&zero), sizeof(size_t), 0, SEEK_CUR);
    disk_manager_.CloseFile(db_fd);

    db_ = std::make_unique<DatabaseHandle>(test_dir_, &disk_manager_, &table_manager_, &index_manager_);
    db_->SetLockManager(&lock_manager_);
    db_->Acquire();

    std::vector<RTField> fields;
    RTField id;
    id.field_.field_name_ = "id";
    id.field_.field_type_ = TYPE_INT;
    id.field_.field_size_ = sizeof(int);
    fields.push_back(id);

    RTField value;
    value.field_.field_name_ = "value";
    value.field_.field_type_ = TYPE_INT;
    value.field_.field_size_ = sizeof(int);
    fields.push_back(value);

    RecordSchema schema(std::move(fields));
    db_->CreateTable("items", schema, NARY_MODEL);
    table_ = db_->GetTable("items");

    std::vector<RTField> key_fields{table_->GetSchema().GetFieldAt(0)};
    RecordSchema key_schema(std::move(key_fields));
    db_->CreateIndex("items_pk", "items", key_schema, IndexType::BPTREE);
    index_ = db_->GetIndexes(table_->GetTableId()).front();
  }

  void TearDown() override
  {
    if (db_ != nullptr) {
      db_->Release();
      db_.reset();
    }
    std::filesystem::remove_all(test_dir_);
  }

  auto MakeRecord(int id, int value, RID rid = INVALID_RID) -> RecordUptr
  {
    std::vector<ValueSptr> values{
        ValueFactory::CreateIntValue(id),
        ValueFactory::CreateIntValue(value),
    };
    return std::make_unique<Record>(&table_->GetSchema(), values, rid);
  }

  auto BeginTxn(txn_id_t txn_id) -> Transaction *
  {
    txn_manager_.Begin(txn_id, IsolationLevel::SERIALIZABLE, ConcurrencyProtocol::TWO_PHASE_LOCKING);
    auto *txn = txn_manager_.GetTransaction(txn_id);
    EXPECT_TRUE(lock_manager_.LockTable(txn, LockMode::INTENTION_EXCLUSIVE, table_->GetTableId()));
    return txn;
  }

  std::string test_dir_;
  DiskManager disk_manager_;
  BufferPoolManager buffer_pool_manager_{&disk_manager_, nullptr};
  TableManager table_manager_{&disk_manager_, &buffer_pool_manager_};
  IndexManager index_manager_{&disk_manager_, &buffer_pool_manager_};
  TxnManager txn_manager_{nullptr};
  LockManager lock_manager_{DeadlockPolicy::DETECTION, &txn_manager_};
  std::unique_ptr<DatabaseHandle> db_;
  TableHandle *table_{nullptr};
  IndexHandle *index_{nullptr};
};

TEST_F(TwoPLIndexAbortTest, InsertAbortRestoresTableAndIndex)
{
  auto *txn = BeginTxn(1);
  auto record = MakeRecord(10, 100);
  auto rid = table_->InsertRecord(*record, txn);
  record->SetRID(rid);
  index_->InsertRecord(*record);
  txn->AppendIndexWriteRecord(IndexWriteRecord(IndexWriteType::INSERT, index_->GetIndexId(), nullptr, record->Clone()));

  txn_manager_.Abort(1, db_.get());

  EXPECT_THROW(table_->GetRecord(rid), NJUDBException_);
  EXPECT_FALSE(index_->CheckRecordExists(*record));
}

TEST_F(TwoPLIndexAbortTest, UpdateAbortRestoresTableAndIndex)
{
  auto old_record = MakeRecord(20, 200);
  auto rid = table_->InsertRecord(*old_record);
  old_record->SetRID(rid);
  index_->InsertRecord(*old_record);

  auto *txn = BeginTxn(2);
  auto new_record = MakeRecord(21, 201, rid);
  table_->UpdateRecord(rid, *new_record, txn);
  index_->UpdateRecord(*old_record, *new_record);
  txn->AppendIndexWriteRecord(
      IndexWriteRecord(IndexWriteType::UPDATE, index_->GetIndexId(), old_record->Clone(), new_record->Clone()));

  txn_manager_.Abort(2, db_.get());

  auto restored = table_->GetRecord(rid);
  EXPECT_TRUE(*restored == *old_record);
  EXPECT_TRUE(index_->CheckRecordExists(*old_record));
  EXPECT_FALSE(index_->CheckRecordExists(*new_record));
}

TEST_F(TwoPLIndexAbortTest, DeleteAbortRestoresTableAndIndex)
{
  auto old_record = MakeRecord(30, 300);
  auto rid = table_->InsertRecord(*old_record);
  old_record->SetRID(rid);
  index_->InsertRecord(*old_record);

  auto *txn = BeginTxn(3);
  table_->DeleteRecord(rid, txn);
  index_->DeleteRecord(*old_record);
  txn->AppendIndexWriteRecord(
      IndexWriteRecord(IndexWriteType::DELETE, index_->GetIndexId(), old_record->Clone(), nullptr));

  txn_manager_.Abort(3, db_.get());

  auto restored = table_->GetRecord(rid);
  EXPECT_TRUE(*restored == *old_record);
  EXPECT_TRUE(index_->CheckRecordExists(*old_record));
}

}  // namespace njudb
