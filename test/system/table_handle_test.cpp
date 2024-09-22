//
// Created by ziqi on 2024/8/19.
//

#include "../config.h"
#include "common/types.h"
#include "storage/storage.h"
#include "system/handle/table_handle.h"
#include "system/table/table_manager.h"

#include <cassert>
#include <unordered_map>
#include <vector>
#include <unordered_set>
#include <shared_mutex>

#include "gtest/gtest.h"
using namespace wsdb;

auto GenTableSchema(int n) -> RecordSchemaUptr
{
  std::vector<RTField> fields;
  for (int i = 0; i < n % 4; ++i) {
    RTField f;
    f.field_.field_name_ = fmt::format("i_{}", i);
    f.field_.field_type_ = TYPE_INT;
    f.field_.field_size_ = 4;
    fields.push_back(f);
  }
  for (int i = 0; i < n % 4; ++i) {
    RTField f;
    f.field_.field_name_ = fmt::format("s_{}", i);
    f.field_.field_type_ = TYPE_STRING;
    f.field_.field_size_ = rand() % 20;
    fields.push_back(f);
  }
  for (int i = 0; i < n % 4; ++i) {
    RTField f;
    f.field_.field_name_ = fmt::format("f_{}", i);
    f.field_.field_type_ = TYPE_FLOAT;
    f.field_.field_size_ = 4;
    fields.push_back(f);
  }
  return std::make_unique<RecordSchema>(fields);
}

auto GenRecordUnderSchema(const RecordSchema &schema) -> RecordUptr
{
  std::vector<ValueSptr> values;
  for (const auto &f : schema.GetFields()) {
    auto data = new char[f.field_.field_size_];
    for (int i = 0; i < static_cast<int>(f.field_.field_size_); ++i) {
      data[i] = rand() % 256;
    }
    values.emplace_back(ValueFactory::CreateValue(f.field_.field_type_, data, f.field_.field_size_));
    delete[] data;
  }
  return std::make_unique<Record>(&schema, values, INVALID_RID);
}

TEST(TableHandle, Simple)
{
  auto        disk_manager        = std::make_unique<DiskManager>();
  auto        buffer_pool_manager = std::make_unique<BufferPoolManager>(disk_manager.get(), nullptr);
  auto        table_manager       = std::make_unique<TableManager>(disk_manager.get(), buffer_pool_manager.get());
  std::string table_name          = "table_handle_simple";
  if (!std::filesystem::exists(TEST_DIR))
    std::filesystem::create_directory(TEST_DIR);
  if (std::filesystem::exists(FILE_NAME(TEST_DIR, table_name, TAB_SUFFIX)))
    std::filesystem::remove(FILE_NAME(TEST_DIR, table_name, TAB_SUFFIX));
  auto tbl_schema = GenTableSchema(10);
  table_manager->CreateTable(TEST_DIR, table_name, *tbl_schema, NARY_MODEL);
  auto tbl   = table_manager->OpenTable(TEST_DIR, table_name, NARY_MODEL);
  tbl_schema = nullptr;
  ASSERT_EQ(tbl->GetTableName(), table_name);
  ASSERT_EQ(tbl->GetSchema().GetFieldCount(), tbl->GetSchema().GetFieldCount());
  ASSERT_EQ(tbl->GetSchema().GetRecordLength(), tbl->GetSchema().GetRecordLength());
  // insert record
  for (int i = 0; i < 100; ++i) {
    auto record  = GenRecordUnderSchema(tbl->GetSchema());
    auto rid     = tbl->InsertRecord(*record);
    auto record2 = tbl->GetRecord(rid);
    ASSERT_TRUE(*record == *record2);
  }
  // update record
  for (int i = 0; i < 100; ++i) {
    auto record  = GenRecordUnderSchema(tbl->GetSchema());
    auto rid     = tbl->InsertRecord(*record);
    auto record2 = tbl->GetRecord(rid);
    ASSERT_TRUE(*record == *record2);
    auto record3 = GenRecordUnderSchema(tbl->GetSchema());
    tbl->UpdateRecord(rid, *record3);
    auto record4 = tbl->GetRecord(rid);
    ASSERT_TRUE(*record3 == *record4);
  }
  // delete record
  for (int i = 0; i < 100; ++i) {
    auto record  = GenRecordUnderSchema(tbl->GetSchema());
    auto rid     = tbl->InsertRecord(*record);
    auto record2 = tbl->GetRecord(rid);
    ASSERT_TRUE(*record == *record2);
    tbl->DeleteRecord(rid);
    ASSERT_THROW(tbl->GetRecord(rid), WSDBException_);
  }
  table_manager->CloseTable(TEST_DIR, *tbl);
  table_manager->DropTable(TEST_DIR, table_name);
}

TEST(TableHandle, MultiThread)
{
  auto        disk_manager        = std::make_unique<DiskManager>();
  auto        buffer_pool_manager = std::make_unique<BufferPoolManager>(disk_manager.get(), nullptr);
  auto        table_manager       = std::make_unique<TableManager>(disk_manager.get(), buffer_pool_manager.get());
  std::string table_name          = "table_handle_multi_thread";
  if (!std::filesystem::exists(TEST_DIR))
    std::filesystem::create_directory(TEST_DIR);
  if (std::filesystem::exists(FILE_NAME(TEST_DIR, table_name, TAB_SUFFIX)))
    std::filesystem::remove(FILE_NAME(TEST_DIR, table_name, TAB_SUFFIX));
  auto tbl_schema = GenTableSchema(10);
  table_manager->CreateTable(TEST_DIR, table_name, *tbl_schema, NARY_MODEL);
  auto tbl   = table_manager->OpenTable(TEST_DIR, table_name, NARY_MODEL);
  tbl_schema = nullptr;
  ASSERT_EQ(tbl->GetTableName(), table_name);
  ASSERT_EQ(tbl->GetSchema().GetFieldCount(), tbl->GetSchema().GetFieldCount());
  ASSERT_EQ(tbl->GetSchema().GetRecordLength(), tbl->GetSchema().GetRecordLength());
  std::vector<std::thread> threads;
  std::atomic<int>         cnt(0);
  std::shared_mutex        rw_lock;
  std::vector<RID>         rids;
  for (int i = 0; i < 10; ++i) {
    threads.emplace_back([&]() {
      for (int i = 0; i < 1000; ++i) {
        auto record = GenRecordUnderSchema(tbl->GetSchema());
        rw_lock.lock();
        auto rid = tbl->InsertRecord(*record);
        rids.push_back(rid);
        auto record2 = tbl->GetRecord(rid);
        rw_lock.unlock();
        ASSERT_TRUE(*record == *record2);
        cnt++;
      }
    });
  }
  for (auto &t : threads) {
    t.join();
  }
  // randomly delete and update records in rids
  threads.clear();
  for (int t = 0; t < 10; ++t) {
    if (t % 2 == 0) {
      threads.emplace_back([&]() {
        for (int i = 0; i < 1000; ++i) {
          rw_lock.lock();
          if (rids.empty()) {
            rw_lock.unlock();
            continue;
          }
          auto rid = rids[rand() % rids.size()];
          rids.erase(std::remove(rids.begin(), rids.end(), rid), rids.end());
          tbl->DeleteRecord(rid);
          ASSERT_THROW(tbl->GetRecord(rid), WSDBException_);
          rw_lock.unlock();
          cnt--;
        }
      });
    } else {
      threads.emplace_back([&]() {
        for (int i = 0; i < 1000; ++i) {
          rw_lock.lock();
          if (rids.empty()) {
            rw_lock.unlock();
            continue;
          }
          auto rid    = rids[rand() % rids.size()];
          auto record = GenRecordUnderSchema(tbl->GetSchema());
          tbl->UpdateRecord(rid, *record);
          auto record2 = tbl->GetRecord(rid);
          rw_lock.unlock();
          ASSERT_TRUE(*record == *record2);
        }
      });
    }
  }
  for (auto &t : threads) {
    t.join();
  }
  ASSERT_EQ(cnt, rids.size());
  table_manager->CloseTable(TEST_DIR, *tbl);
  table_manager->DropTable(TEST_DIR, table_name);
}

TEST(TableHandle, PAX_MultiThread)
{
  auto        disk_manager        = std::make_unique<DiskManager>();
  auto        buffer_pool_manager = std::make_unique<BufferPoolManager>(disk_manager.get(), nullptr);
  auto        table_manager       = std::make_unique<TableManager>(disk_manager.get(), buffer_pool_manager.get());
  std::string table_name          = "table_handle_pax_multi_thread";
  if (!std::filesystem::exists(TEST_DIR))
    std::filesystem::create_directory(TEST_DIR);
  if (std::filesystem::exists(FILE_NAME(TEST_DIR, table_name, TAB_SUFFIX)))
    std::filesystem::remove(FILE_NAME(TEST_DIR, table_name, TAB_SUFFIX));
  auto tbl_schema = GenTableSchema(10);
  table_manager->CreateTable(TEST_DIR, table_name, *tbl_schema, PAX_MODEL);
  auto tbl   = table_manager->OpenTable(TEST_DIR, table_name, PAX_MODEL);
  tbl_schema = nullptr;
  ASSERT_EQ(tbl->GetTableName(), table_name);
  ASSERT_EQ(tbl->GetSchema().GetFieldCount(), tbl->GetSchema().GetFieldCount());
  ASSERT_EQ(tbl->GetSchema().GetRecordLength(), tbl->GetSchema().GetRecordLength());
  std::vector<std::thread> threads;
  std::atomic<int>         cnt(0);
  std::shared_mutex        rw_lock;
  std::vector<RID>         rids;
  for (int i = 0; i < 10; ++i) {
    threads.emplace_back([&]() {
      for (int i = 0; i < 1000; ++i) {
        auto record = GenRecordUnderSchema(tbl->GetSchema());
        rw_lock.lock();
        auto rid = tbl->InsertRecord(*record);
        rids.push_back(rid);
        auto record2 = tbl->GetRecord(rid);
        rw_lock.unlock();
        ASSERT_TRUE(*record == *record2);
        cnt++;
      }
    });
  }
  for (auto &t : threads) {
    t.join();
  }
  // check read chunk
  std::unordered_map<page_id_t, ChunkUptr> chunks;
  std::vector<RTField> fields(tbl->GetSchema().GetFields().begin(), tbl->GetSchema().GetFields().begin() + 3);
  auto                 chunk_schema = std::make_unique<RecordSchema>(fields);
  for (auto &rid : rids) {
    if (chunks.find(rid.PageID()) != chunks.end()) {
      continue;
    }
    auto chunk = tbl->GetChunk(rid.PageID(), chunk_schema.get());
    ASSERT_TRUE(chunk != nullptr);
    ASSERT_EQ(chunk->GetColCount(), chunk_schema->GetFieldCount());
    chunks[rid.PageID()] = std::move(chunk);
  }
  // check chunk data using record scan
  std::unordered_map<page_id_t, std::vector<ArrayValueSptr>> chunk_data;
  for (auto rid = tbl->GetFirstRID(); rid != INVALID_RID; rid = tbl->GetNextRID(rid)) {
    auto record = tbl->GetRecord(rid);
    if (chunk_data.find(rid.PageID()) == chunk_data.end()) {
      chunk_data[rid.PageID()] = std::vector<ArrayValueSptr>(3);
      for (int i = 0; i < 3; ++i) {
        chunk_data[rid.PageID()][i] = ValueFactory::CreateArrayValue();
      }
    }
    for (int i = 0; i < 3; ++i) {
      chunk_data[rid.PageID()][i]->Append(record->GetValueAt(i));
    }
  }
  for (const auto &[pid, chunk] : chunks) {
    ASSERT_TRUE(chunk_data.find(pid) != chunk_data.end());
    for (int i = 0; i < 3; ++i) {
      ASSERT_TRUE(*chunk->GetCol(i) == *chunk_data[pid][i]);
    }
  }

  // randomly delete and update records in rids
  threads.clear();
  for (int t = 0; t < 10; ++t) {
    if (t % 2 == 0) {
      threads.emplace_back([&]() {
        for (int i = 0; i < 1000; ++i) {
          rw_lock.lock();
          if (rids.empty()) {
            rw_lock.unlock();
            continue;
          }
          auto rid = rids[rand() % rids.size()];
          rids.erase(std::remove(rids.begin(), rids.end(), rid), rids.end());
          tbl->DeleteRecord(rid);
          ASSERT_THROW(tbl->GetRecord(rid), WSDBException_);
          rw_lock.unlock();
          cnt--;
        }
      });
    } else {
      threads.emplace_back([&]() {
        for (int i = 0; i < 1000; ++i) {
          rw_lock.lock();
          if (rids.empty()) {
            rw_lock.unlock();
            continue;
          }
          auto rid    = rids[rand() % rids.size()];
          auto record = GenRecordUnderSchema(tbl->GetSchema());
          tbl->UpdateRecord(rid, *record);
          auto record2 = tbl->GetRecord(rid);
          rw_lock.unlock();
          ASSERT_TRUE(*record == *record2);
        }
      });
    }
  }
  for (auto &t : threads) {
    t.join();
  }
  ASSERT_EQ(cnt, rids.size());
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
