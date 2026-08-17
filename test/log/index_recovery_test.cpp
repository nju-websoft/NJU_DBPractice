/*------------------------------------------------------------------------------
 - Copyright (c) 2024. Websoft research group, Nanjing University.
 -
 - Lab06 Phase 5: derived-index recovery tests.
 -----------------------------------------------------------------------------*/

#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>

#include "storage/buffer/buffer_pool_manager.h"
#include "storage/disk/disk_manager.h"
#include "system/handle/database_handle.h"
#include "system/index/index_manager.h"
#include "system/table/table_manager.h"

using namespace njudb;

class IndexRecoveryTest : public ::testing::Test
{
protected:
  void SetUp() override
  {
    db_name_ = "./index_recovery_" + std::to_string(std::rand());
    std::filesystem::remove_all(db_name_);
    std::filesystem::create_directories(db_name_);

    DiskManager::CreateFile(FILE_NAME(db_name_, db_name_, DB_SUFFIX));
    auto db_fd = disk_manager_.OpenFile(FILE_NAME(db_name_, db_name_, DB_SUFFIX));
    size_t zero = 0;
    disk_manager_.WriteFile(db_fd, reinterpret_cast<const char *>(&zero), sizeof(size_t), 0, SEEK_CUR);
    disk_manager_.WriteFile(db_fd, reinterpret_cast<const char *>(&zero), sizeof(size_t), 0, SEEK_CUR);
    disk_manager_.CloseFile(db_fd);

    db_ = std::make_unique<DatabaseHandle>(db_name_, &disk_manager_, &table_manager_, &index_manager_);
    db_->Acquire();

    RTField id;
    id.field_.field_name_ = "id";
    id.field_.field_type_ = TYPE_INT;
    id.field_.field_size_ = sizeof(int);

    RTField alt;
    alt.field_.field_name_ = "alt";
    alt.field_.field_type_ = TYPE_INT;
    alt.field_.field_size_ = sizeof(int);

    RecordSchema schema({id, alt});
    db_->CreateTable("items", schema, NARY_MODEL);
    table_ = db_->GetTable("items");

    RecordSchema id_key({table_->GetSchema().GetFieldAt(0)});
    db_->CreateIndex("id_idx", "items", id_key, IndexType::BPTREE);
    RecordSchema alt_key({table_->GetSchema().GetFieldAt(1)});
    db_->CreateIndex("alt_idx", "items", alt_key, IndexType::HASH);
  }

  void TearDown() override
  {
    if (db_ != nullptr) {
      db_->Close();
      db_.reset();
    }
    std::filesystem::remove_all(db_name_);
  }

  auto MakeRecord(int id, int alt, RID rid = INVALID_RID) -> RecordUptr
  {
    std::vector<ValueSptr> values{ValueFactory::CreateIntValue(id), ValueFactory::CreateIntValue(alt)};
    return std::make_unique<Record>(&table_->GetSchema(), values, rid);
  }

  auto InsertConsistent(int id, int alt) -> RecordUptr
  {
    auto record = MakeRecord(id, alt);
    record->SetRID(table_->InsertRecord(*record));
    for (auto *index : db_->GetIndexes("items")) {
      index->InsertRecord(*record);
    }
    return record;
  }

  auto Index(const std::string &name) -> IndexHandle *
  {
    for (auto *index : db_->GetIndexes("items")) {
      if (index->GetIndexName() == name) {
        return index;
      }
    }
    return nullptr;
  }

  void ExpectPresentInBoth(const Record &record)
  {
    ASSERT_NE(Index("id_idx"), nullptr);
    ASSERT_NE(Index("alt_idx"), nullptr);
    EXPECT_TRUE(Index("id_idx")->CheckRecordExists(record));
    EXPECT_TRUE(Index("alt_idx")->CheckRecordExists(record));
  }

  void ExpectAbsentFromBoth(const Record &record)
  {
    ASSERT_NE(Index("id_idx"), nullptr);
    ASSERT_NE(Index("alt_idx"), nullptr);
    EXPECT_FALSE(Index("id_idx")->CheckRecordExists(record));
    EXPECT_FALSE(Index("alt_idx")->CheckRecordExists(record));
  }

  std::string db_name_;
  DiskManager disk_manager_;
  BufferPoolManager buffer_pool_manager_{&disk_manager_, nullptr};
  TableManager table_manager_{&disk_manager_, &buffer_pool_manager_};
  IndexManager index_manager_{&disk_manager_, &buffer_pool_manager_};
  std::unique_ptr<DatabaseHandle> db_;
  TableHandle *table_{nullptr};
};

TEST_F(IndexRecoveryTest, RebuildCorrectsInsertUpdateDeleteAndMultipleIndexes)
{
  auto old_updated = InsertConsistent(1, 10);
  auto old_deleted = InsertConsistent(2, 20);

  auto new_updated = MakeRecord(11, 110, old_updated->GetRID());
  table_->UpdateRecord(old_updated->GetRID(), *new_updated);
  table_->DeleteRecord(old_deleted->GetRID());
  auto inserted = MakeRecord(3, 30);
  inserted->SetRID(table_->InsertRecord(*inserted));

  db_->RebuildIndexes();

  EXPECT_FALSE(db_->IsIndexRebuildRequired());
  ExpectAbsentFromBoth(*old_updated);
  ExpectAbsentFromBoth(*old_deleted);
  ExpectPresentInBoth(*new_updated);
  ExpectPresentInBoth(*inserted);
  EXPECT_FALSE(std::filesystem::exists(FILE_NAME(db_name_, "items_id_idx.__rebuild_tmp", IDX_SUFFIX)));
  EXPECT_FALSE(std::filesystem::exists(FILE_NAME(db_name_, "items_alt_idx.__rebuild_tmp", IDX_SUFFIX)));
}

TEST_F(IndexRecoveryTest, PersistentMarkerAllowsRebuildAfterRestart)
{
  auto stale = MakeRecord(7, 70);
  stale->SetRID(table_->InsertRecord(*stale));
  const auto stale_rid = stale->GetRID();
  {
    std::ofstream marker(std::filesystem::path(db_name_) / ".index_rebuild_required");
    marker << "required";
  }

  db_->Close();
  db_->Open();
  table_ = db_->GetTable("items");
  stale = MakeRecord(7, 70, stale_rid);
  ASSERT_TRUE(db_->IsIndexRebuildRequired());

  db_->RebuildIndexes();

  EXPECT_FALSE(db_->IsIndexRebuildRequired());
  ExpectPresentInBoth(*stale);
}

TEST_F(IndexRecoveryTest, DuplicateFinalTableStateKeepsMarkerAndCanBeRetried)
{
  auto original = InsertConsistent(5, 50);
  auto duplicate = MakeRecord(5, 60);
  duplicate->SetRID(table_->InsertRecord(*duplicate));

  EXPECT_THROW(db_->RebuildIndexes(), NJUDBException_);
  EXPECT_TRUE(db_->IsIndexRebuildRequired());
  ASSERT_NE(Index("id_idx"), nullptr);
  EXPECT_TRUE(Index("id_idx")->CheckRecordExists(*original));

  table_->DeleteRecord(duplicate->GetRID());
  EXPECT_NO_THROW(db_->RebuildIndexes());
  EXPECT_FALSE(db_->IsIndexRebuildRequired());
  ExpectPresentInBoth(*original);
}

TEST_F(IndexRecoveryTest, EmptyTableIndexesCanBeRebuilt)
{
  EXPECT_NO_THROW(db_->RebuildIndexes());
  EXPECT_FALSE(db_->IsIndexRebuildRequired());
  ASSERT_NE(Index("id_idx"), nullptr);
  ASSERT_NE(Index("alt_idx"), nullptr);
  EXPECT_TRUE(Index("id_idx")->IsEmpty());
  EXPECT_TRUE(Index("alt_idx")->IsEmpty());
}

TEST_F(IndexRecoveryTest, SameNamedIndexesInDifferentDatabasesAreIndependent)
{
  auto first = MakeRecord(1, 10);
  first->SetRID(table_->InsertRecord(*first));

  const auto other_db_name = db_name_ + "_other";
  std::filesystem::create_directories(other_db_name);
  DiskManager::CreateFile(FILE_NAME(other_db_name, other_db_name, DB_SUFFIX));
  auto db_fd = disk_manager_.OpenFile(FILE_NAME(other_db_name, other_db_name, DB_SUFFIX));
  size_t zero = 0;
  disk_manager_.WriteFile(db_fd, reinterpret_cast<const char *>(&zero), sizeof(size_t), 0, SEEK_CUR);
  disk_manager_.WriteFile(db_fd, reinterpret_cast<const char *>(&zero), sizeof(size_t), 0, SEEK_CUR);
  disk_manager_.CloseFile(db_fd);

  DatabaseHandle other_db(other_db_name, &disk_manager_, &table_manager_, &index_manager_);
  other_db.Open();
  RTField id;
  id.field_.field_name_ = "id";
  id.field_.field_type_ = TYPE_INT;
  id.field_.field_size_ = sizeof(int);
  RTField alt;
  alt.field_.field_name_ = "alt";
  alt.field_.field_type_ = TYPE_INT;
  alt.field_.field_size_ = sizeof(int);
  RecordSchema schema({id, alt});
  other_db.CreateTable("items", schema, NARY_MODEL);
  auto *other_table = other_db.GetTable("items");
  RecordSchema key({other_table->GetSchema().GetFieldAt(0)});
  other_db.CreateIndex("id_idx", "items", key, IndexType::BPTREE);

  std::vector<ValueSptr> values{ValueFactory::CreateIntValue(2), ValueFactory::CreateIntValue(20)};
  auto second = std::make_unique<Record>(&other_table->GetSchema(), values, INVALID_RID);
  second->SetRID(other_table->InsertRecord(*second));

  db_->RebuildIndexes();
  other_db.RebuildIndexes();

  EXPECT_TRUE(Index("id_idx")->CheckRecordExists(*first));
  auto *other_index = other_db.GetIndexes("items").front();
  EXPECT_TRUE(other_index->CheckRecordExists(*second));
  std::vector<ValueSptr> first_values{ValueFactory::CreateIntValue(1), ValueFactory::CreateIntValue(10)};
  Record other_first(&other_table->GetSchema(), first_values, first->GetRID());
  EXPECT_FALSE(other_index->CheckRecordExists(other_first));

  other_db.Close();
  std::filesystem::remove_all(other_db_name);
}
