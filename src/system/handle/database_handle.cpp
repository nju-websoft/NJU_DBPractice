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
// Created by ziqi on 2024/7/28.
//

#include "database_handle.h"

namespace njudb {
DatabaseHandle::DatabaseHandle(
    std::string db_name, DiskManager *disk_manager, TableManager *tbl_mgr, IndexManager *idx_mgr)
    : ref_cnt_(0), db_name_(std::move(db_name)), disk_manager_(disk_manager), tbl_mgr_(tbl_mgr), idx_mgr_(idx_mgr)
{}

void DatabaseHandle::Open()
{
  /**
   * open all tables and indexes in the database
   * .db file example:
   * | table_num | table_name_1_len | table_name_1 | storage_model_1 | ... | table_name_n_len | table_name_n |
   * storage_model_n | |index_num | index_name_1_len | index_name_1 | index_type_1 | ... | index_name_n_len |
   * index_name_n | index_type_n |
   */
  // open db_name_.db
  auto db_fd = disk_manager_->OpenFile(FILE_NAME(db_name_, db_name_, DB_SUFFIX));
  // read table names and storage model
  // read table number
  size_t table_num = 0;
  disk_manager_->ReadFile(db_fd, reinterpret_cast<char *>(&table_num), sizeof(size_t), 0, SEEK_CUR);
  for (size_t i = 0; i < table_num; ++i) {
    // read table name length
    size_t table_name_len = 0;
    disk_manager_->ReadFile(db_fd, reinterpret_cast<char *>(&table_name_len), sizeof(size_t), 0, SEEK_CUR);
    // read table name
    char *table_name_mem           = new char[table_name_len + 1];
    table_name_mem[table_name_len] = '\0';
    disk_manager_->ReadFile(db_fd, table_name_mem, table_name_len, 0, SEEK_CUR);
    std::string table_name(table_name_mem);
    delete[] table_name_mem;
    // read storage model
    StorageModel storage_model;
    disk_manager_->ReadFile(db_fd, reinterpret_cast<char *>(&storage_model), sizeof(StorageModel), 0, SEEK_CUR);
    // create table handle via table manager
    auto tbl_hdl                   = tbl_mgr_->OpenTable(db_name_, table_name, storage_model);
    tables_[tbl_hdl->GetTableId()] = std::move(tbl_hdl);
  }
  // read index number
  size_t index_num = 0;
  disk_manager_->ReadFile(db_fd, reinterpret_cast<char *>(&index_num), sizeof(size_t), 0, SEEK_CUR);
  for (size_t i = 0; i < index_num; ++i) {
    // read table name, index name, and index type
    std::string table_name;
    std::string index_name;
    // read index name length
    size_t table_name_len = 0;
    disk_manager_->ReadFile(db_fd, reinterpret_cast<char *>(&table_name_len), sizeof(size_t), 0, SEEK_CUR);
    // read table name
    char *table_name_mem           = new char[table_name_len + 1];
    table_name_mem[table_name_len] = '\0';
    disk_manager_->ReadFile(db_fd, table_name_mem, table_name_len, 0, SEEK_CUR);
    table_name            = std::string(table_name_mem);
    size_t index_name_len = 0;
    disk_manager_->ReadFile(db_fd, reinterpret_cast<char *>(&index_name_len), sizeof(size_t), 0, SEEK_CUR);
    // read index name
    char *index_name_mem           = new char[index_name_len + 1];
    index_name_mem[index_name_len] = '\0';
    disk_manager_->ReadFile(db_fd, index_name_mem, index_name_len, 0, SEEK_CUR);
    index_name = std::string(index_name_mem);
    delete[] table_name_mem;
    delete[] index_name_mem;
    // read index type
    IndexType index_type;
    disk_manager_->ReadFile(db_fd, reinterpret_cast<char *>(&index_type), sizeof(IndexType), 0, SEEK_CUR);
    // create index handle
    auto idx_hdl  = idx_mgr_->OpenIndex(db_name_, index_name, table_name, index_type);
    auto iid      = idx_hdl->GetIndexId();
    indexes_[iid] = std::move(idx_hdl);
    // update tab_idx_map_
    auto table_id = indexes_[iid]->GetTableId();
    if (tab_idx_map_.find(table_id) == tab_idx_map_.end())
      tab_idx_map_[table_id] = std::list<idx_id_t>();
    tab_idx_map_[table_id].push_back(iid);
  }
  disk_manager_->CloseFile(db_fd);
}

void DatabaseHandle::Close()
{
  if (ref_cnt_ == 0 || --ref_cnt_ > 0) {
    return;
  }
  FlushMeta();
  // close all tables and indexes in the database
  // close tables
  for (auto &table : tables_) {
    NJUDB_LOG("close table");
    tbl_mgr_->CloseTable(db_name_, *table.second);
  }
  // close indexes
  for (auto &index : indexes_) {
    idx_mgr_->CloseIndex(*index.second);
  }
  // clear
  tables_.clear();
  indexes_.clear();
  tab_idx_map_.clear();
}

void DatabaseHandle::FlushMeta()
{
  /**
   * flush all tables and indexes in the database
   * .db file example:
   * | table_num | table_name_1_len | table_name_1 | storage_model_1 | ... | table_name_n_len | table_name_n |
   * storage_model_n | |index_num | index_name_1_len | index_name_1 | index_type_1 | ... | index_name_n_len |
   * index_name_n | index_type_n |
   */

  // open db_name_.db
  auto db_fd = disk_manager_->OpenFile(FILE_NAME(db_name_, db_name_, DB_SUFFIX));
  // write table names and storage model
  // write table number
  size_t table_num = tables_.size();
  disk_manager_->WriteFile(db_fd, reinterpret_cast<const char *>(&table_num), sizeof(size_t), SEEK_SET);
  for (auto &table : tables_) {
    // write table name length
    size_t table_name_len = table.second->GetTableName().size();
    disk_manager_->WriteFile(db_fd, reinterpret_cast<const char *>(&table_name_len), sizeof(size_t), SEEK_CUR);
    // write table name
    disk_manager_->WriteFile(db_fd, table.second->GetTableName().c_str(), table_name_len, SEEK_CUR);
    // write storage model
    StorageModel storage_model = table.second->GetStorageModel();
    disk_manager_->WriteFile(db_fd, reinterpret_cast<const char *>(&storage_model), sizeof(StorageModel), SEEK_CUR);
  }
  // write index number
  size_t index_num = indexes_.size();
  disk_manager_->WriteFile(db_fd, reinterpret_cast<const char *>(&index_num), sizeof(size_t), SEEK_CUR);
  for (auto &index : indexes_) {
    // write table name length
    NJUDB_ASSERT(tab_idx_map_.find(index.second->GetTableId()) != tab_idx_map_.end(),
        fmt::format("Index {} does not belong to any table", index.second->GetIndexName()));
    NJUDB_ASSERT(tables_.find(index.second->GetTableId()) != tables_.end(),
        fmt::format("Table {} does not exist for index {}", index.second->GetTableId(), index.second->GetIndexName()));
    auto   table_name     = tables_[index.second->GetTableId()]->GetTableName();
    size_t table_name_len = table_name.size();
    disk_manager_->WriteFile(db_fd, reinterpret_cast<const char *>(&table_name_len), sizeof(size_t), SEEK_CUR);
    // write table name
    disk_manager_->WriteFile(db_fd, table_name.c_str(), table_name_len, SEEK_CUR);
    // write index name length
    size_t index_name_len = index.second->GetIndexName().size();
    disk_manager_->WriteFile(db_fd, reinterpret_cast<const char *>(&index_name_len), sizeof(size_t), SEEK_CUR);
    // write index name
    disk_manager_->WriteFile(db_fd, index.second->GetIndexName().c_str(), index_name_len, SEEK_CUR);
    // write index type
    IndexType index_type = index.second->GetIndexType();
    disk_manager_->WriteFile(db_fd, reinterpret_cast<const char *>(&index_type), sizeof(IndexType), SEEK_CUR);
  }
  disk_manager_->CloseFile(db_fd);
}

void DatabaseHandle::CreateTable(
    const std::string &tab_name, const RecordSchema &rec_schema, StorageModel storage_model)
{
  tbl_mgr_->CreateTable(db_name_, tab_name, rec_schema, storage_model);
  auto tbl_hdl                   = tbl_mgr_->OpenTable(db_name_, tab_name, storage_model);
  tables_[tbl_hdl->GetTableId()] = std::move(tbl_hdl);

  FlushMeta();
}

void DatabaseHandle::DropTable(const std::string &tab_name)
{
  auto tid   = tbl_mgr_->GetTableId(db_name_, tab_name);
  auto table = tables_[tid].get();
  tbl_mgr_->CloseTable(db_name_, *table);
  TableManager::DropTable(db_name_, tab_name);
  tables_.erase(tid);
  for (auto &idx_id : tab_idx_map_[tid]) {
    auto index = indexes_[idx_id].get();
    idx_mgr_->CloseIndex(*index);
    idx_mgr_->DropIndex(db_name_, index->GetIndexName(), tab_name);
    indexes_.erase(idx_id);
  }
  tab_idx_map_.erase(tid);
  FlushMeta();
}

void DatabaseHandle::CreateIndex(
    const std::string &idx_name, const std::string &tab_name, const RecordSchema &key_schema, IndexType idx_type)
{
  auto table_id = tbl_mgr_->GetTableId(db_name_, tab_name);

  idx_mgr_->CreateIndex(db_name_, idx_name, tab_name, key_schema, idx_type);
  auto idx_hdl = idx_mgr_->OpenIndex(db_name_, idx_name, tab_name, idx_type);
  // now insert records of the table into the index
  auto table = tables_[table_id].get();
  NJUDB_ASSERT(table != nullptr, fmt::format("Table {} does not exist", tab_name));
  NJUDB_ASSERT(table->GetTableId() == table_id,
      fmt::format("Table ID mismatch: expected {}, got {}", table_id, table->GetTableId()));
  NJUDB_ASSERT(table->GetTableName() == tab_name,
      fmt::format("Table name mismatch: expected {}, got {}", tab_name, table->GetTableName()));
  // insert all records into the index
  auto tab_hdl = tables_[table_id].get();
  try {
    for (auto rid = tab_hdl->GetFirstRID(); rid != INVALID_RID; rid = tab_hdl->GetNextRID(rid)) {
      auto rec = tab_hdl->GetRecord(rid);
      idx_hdl->InsertRecord(*rec);
    }
    // catch NJUDB_INDEX_FAIL
  } catch (const NJUDBException_ &e) {
    if (e.type_ == NJUDB_INDEX_FAIL) {
      // Handle index failure (e.g., log it, clean up, etc.) close the index file and remove it
      idx_mgr_->CloseIndex(*idx_hdl);
      idx_mgr_->DropIndex(db_name_, idx_name, tab_name);
      NJUDB_THROW(NJUDB_INDEX_FAIL,
          fmt::format("Failed to create index {} on table {}: {}", idx_name, tab_name, e.short_what()));
    } else {
      throw;
    }
  }
  auto index_id = idx_hdl->GetIndexId();
  indexes_[index_id] = std::move(idx_hdl);
  tab_idx_map_[table_id].push_back(index_id);

  FlushMeta();
}

void DatabaseHandle::DropIndex(const std::string &idx_name, const std::string &tab_name)
{
  auto table_id = tbl_mgr_->GetTableId(db_name_, tab_name);
  if (table_id == INVALID_TABLE_ID) {
    NJUDB_THROW(NJUDB_TABLE_MISS, fmt::format("Table {} does not exist", tab_name));
  }
  auto idx_id = idx_mgr_->GetIndexId(db_name_, idx_name, tab_name);
  if (idx_id == INVALID_IDX_ID) {
    NJUDB_THROW(NJUDB_INDEX_MISS, fmt::format("Index {} does not exist on table {}", idx_name, tab_name));
  }
  auto index = indexes_[idx_id].get();
  NJUDB_ASSERT(index->GetTableId() == table_id, fmt::format("Index {} does not belong to table {}", idx_name, tab_name));
  NJUDB_ASSERT(index->GetIndexName() == idx_name,
      fmt::format("Index name mismatch: expected {}, got {}", idx_name, index->GetIndexName()));
  idx_mgr_->CloseIndex(*index);
  idx_mgr_->DropIndex(db_name_, idx_name, tab_name);
  indexes_.erase(idx_id);
  tab_idx_map_[table_id].remove(idx_id);

  FlushMeta();
}

auto DatabaseHandle::GetTable(const std::string &tab_name) -> TableHandle *
{
  auto tid = tbl_mgr_->GetTableId(db_name_, tab_name);
  if (tid == INVALID_TABLE_ID)
    return nullptr;
  return tables_[tid].get();
}

auto DatabaseHandle::GetTable(table_id_t tid) -> TableHandle *
{
  NJUDB_ASSERT(tid != INVALID_TABLE_ID, std::to_string(tid));
  return tables_[tid].get();
}

auto DatabaseHandle::GetIndexNum(table_id_t tid) -> size_t
{
  NJUDB_ASSERT(tid != INVALID_TABLE_ID, std::to_string(tid));
  return tab_idx_map_[tid].size();
}

auto DatabaseHandle::GetIndex(idx_id_t iid) -> IndexHandle *
{
  NJUDB_ASSERT(indexes_.find(iid) != indexes_.end(), std::to_string(iid));
  return indexes_[iid].get();
}

auto DatabaseHandle::GetIndexes(table_id_t tid) -> std::list<IndexHandle *>
{
  NJUDB_ASSERT(tid != INVALID_TABLE_ID, std::to_string(tid));
  std::list<IndexHandle *> indexes;
  for (auto &idx_id : tab_idx_map_[tid]) {
    indexes.push_back(indexes_[idx_id].get());
  }
  return indexes;
}

auto DatabaseHandle::GetIndexes(const std::string &tab_name) -> std::list<IndexHandle *>
{
  auto tid = tbl_mgr_->GetTableId(db_name_, tab_name);
  if (tid == INVALID_TABLE_ID)
    return {};
  return GetIndexes(tid);
}

}  // namespace njudb
