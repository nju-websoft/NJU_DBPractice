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

namespace wsdb {
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
    // read index name length
    size_t index_name_len = 0;
    disk_manager_->ReadFile(db_fd, reinterpret_cast<char *>(&index_name_len), sizeof(size_t), 0, SEEK_CUR);
    // read index name
    char *index_name_mem           = new char[index_name_len + 1];
    index_name_mem[index_name_len] = '\0';
    disk_manager_->ReadFile(db_fd, index_name_mem, index_name_len, 0, SEEK_CUR);
    std::string index_name(index_name_mem);
    delete[] index_name_mem;
    // read index type
    IndexType index_type;
    disk_manager_->ReadFile(db_fd, reinterpret_cast<char *>(&index_type), sizeof(IndexType), 0, SEEK_CUR);
    // create index handle
    // TODO: remove try catch below if IndexManager and indexes are implemented
    try {
      auto idx_hdl                    = idx_mgr_->OpenIndex(db_name_, index_name, index_type);
      indexes_[idx_hdl->GetIndexId()] = std::move(idx_hdl);
      // update tab_idx_map_
      auto table_id = indexes_[idx_hdl->GetIndexId()]->GetTableId();
      if (tab_idx_map_.find(table_id) == tab_idx_map_.end())
        tab_idx_map_[table_id] = std::list<idx_id_t>();
      tab_idx_map_[table_id].push_back(idx_hdl->GetIndexId());
    } catch (WSDBException_ &e) {
      if (e.type_ != WSDB_NOT_IMPLEMENTED)
        throw;
    }
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
    WSDB_LOG("close table");
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
    idx_mgr_->DropIndex(db_name_, index->GetIndexName());
    indexes_.erase(idx_id);
  }
  tab_idx_map_.erase(tid);
  FlushMeta();
}

void DatabaseHandle::CreateIndex(const std::string &tab_name, const RecordSchema &key_schema, IndexType idx_type)
{
  WSDB_THROW(WSDB_NOT_IMPLEMENTED, "");
}

void DatabaseHandle::DropIndex(const std::string &idx_name)
{
  WSDB_THROW(WSDB_NOT_IMPLEMENTED, "");
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
  WSDB_ASSERT(tid != INVALID_TABLE_ID, std::to_string(tid));
  return tables_[tid].get();
}

auto DatabaseHandle::GetIndexNum(table_id_t tid) -> size_t
{
  WSDB_ASSERT(tid != INVALID_TABLE_ID, std::to_string(tid));
  return tab_idx_map_[tid].size();
}

auto DatabaseHandle::GetIndex(idx_id_t iid) -> IndexHandle *
{
  WSDB_ASSERT(indexes_.find(iid) != indexes_.end(), std::to_string(iid));
  return indexes_[iid].get();
}

auto DatabaseHandle::GetIndexes(table_id_t tid) -> std::list<IndexHandle *>
{
  WSDB_ASSERT(tid != INVALID_TABLE_ID, std::to_string(tid));
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

}  // namespace wsdb
