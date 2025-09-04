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

#ifndef NJUDB_INDEX_MANAGER_H
#define NJUDB_INDEX_MANAGER_H
#include "system/handle/index_handle.h"
#include "common/record.h"
namespace njudb {
class IndexManager
{
public:
  IndexManager() = delete;

  IndexManager(DiskManager *disk_manager, BufferPoolManager *buffer_pool_manager)
      : disk_manager_(disk_manager), buffer_pool_manager_(buffer_pool_manager)
  {}

  ~IndexManager() = default;

  void CreateIndex(
      const std::string &db_name, const std::string &index_name, const std::string &table_name, const RecordSchema &schema, IndexType index_type);

  void DropIndex(const std::string &db_name, const std::string &index_name, const std::string &table_name);

  auto OpenIndex(const std::string &db_name, const std::string &index_name, const std::string table_name, IndexType index_type) -> IndexHandleUptr;

  void CloseIndex(const IndexHandle &index_handle);

  auto GetIndexId(const std::string &db_name, const std::string &index_name, const std::string &table_name) -> idx_id_t;

  // Additional utility methods
  auto IndexExists(const std::string &db_name, const std::string &index_name, const std::string &table_name) -> bool;
  
  // Metadata management
  auto ListIndexes(const std::string &db_name) -> std::vector<std::string>;

  // Bulk operations
  void RebuildIndex(const std::string &db_name, const std::string &index_name);

private:
  DiskManager       *disk_manager_;
  BufferPoolManager *buffer_pool_manager_;
};
}  // namespace njudb

#endif  // NJUDB_INDEX_MANAGER_H
