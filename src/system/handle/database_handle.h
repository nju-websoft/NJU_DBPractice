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

#ifndef NJUDB_DATABASE_HANDLE_H
#define NJUDB_DATABASE_HANDLE_H

#include <thread>
#include "storage/disk/disk_manager.h"
#include "storage/buffer/buffer_pool_manager.h"
#include "system/table/table_manager.h"
#include "system/index/index_manager.h"

namespace njudb {
class DatabaseHandle
{
public:
  DatabaseHandle() = delete;

  DatabaseHandle(std::string db_name, DiskManager *disk_manager, TableManager *tbl_mgr, IndexManager *idx_mgr);

  void Open();

  void Close();

  void FlushMeta();

  void CreateTable(const std::string &tab_name, const RecordSchema &rec_schema, StorageModel storage_model);

  void DropTable(const std::string &tab_name);

  void CreateIndex(const std::string &idx_name, const std::string &tab_name, const RecordSchema &key_schema, IndexType idx_type);

  void DropIndex(const std::string &idx_name, const std::string &tab_name);

  [[nodiscard]] auto GetName() const -> std::string { return db_name_; }

  auto GetTable(const std::string &tab_name) -> TableHandle *;

  auto GetTable(table_id_t tid) -> TableHandle *;

  auto GetIndexNum(table_id_t tid) -> size_t;

  auto GetIndex(idx_id_t iid) -> IndexHandle *;

  auto GetIndexes(table_id_t tid) -> std::list<IndexHandle *>;

  auto GetIndexes(const std::string &tab_name) -> std::list<IndexHandle *>;

  auto GetAllTables() -> std::unordered_map<table_id_t, std::unique_ptr<TableHandle>> & { return tables_; }

  ~DatabaseHandle() = default;

public:
  // used to determine when to close db
  std::atomic<int> ref_cnt_;

private:
  std::string db_name_;

  DiskManager *disk_manager_;

  TableManager *tbl_mgr_;
  IndexManager *idx_mgr_;

  std::unordered_map<table_id_t, std::unique_ptr<TableHandle>> tables_;
  std::unordered_map<idx_id_t, std::unique_ptr<IndexHandle>>   indexes_;
  std::unordered_map<table_id_t, std::list<idx_id_t>>          tab_idx_map_;
};
}  // namespace njudb

#endif  // NJUDB_DATABASE_HANDLE_H
