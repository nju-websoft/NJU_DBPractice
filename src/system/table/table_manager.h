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

#ifndef NJUDB_TABLE_MANAGER_H
#define NJUDB_TABLE_MANAGER_H

#include "storage/disk/disk_manager.h"
#include "system/handle/table_handle.h"

namespace njudb {

class TableManager
{
public:
  TableManager() = delete;
  TableManager(DiskManager *disk_manager, BufferPoolManager *buffer_pool_manager)
      : disk_manager_(disk_manager), buffer_pool_manager_(buffer_pool_manager)
  {}
  ~TableManager() = default;

  void CreateTable(const std::string &db_name, const std::string &table_name, const RecordSchema &schema, StorageModel storage_model);

  static void DropTable(const std::string &db_name, const std::string &table_name);

  TableHandleUptr OpenTable(const std::string &db_name, const std::string &table_name, StorageModel storage_model);

  void CloseTable(const std::string &db_name, const TableHandle &table_handle);

  auto GetTableId(const std::string &db_name, const std::string &table_name) -> table_id_t;

private:
  void WriteTableHeader(table_id_t tid, const TableHeader &header, const RecordSchema &schema);

private:
  DiskManager       *disk_manager_;
  BufferPoolManager *buffer_pool_manager_;
};

}  // namespace njudb

#endif  // NJUDB_TABLE_MANAGER_H
