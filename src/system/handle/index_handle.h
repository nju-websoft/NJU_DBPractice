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

#ifndef WSDB_INDEX_HANDLE_H
#define WSDB_INDEX_HANDLE_H
#include "storage/index/index.h"

namespace wsdb {
class IndexHandle
{
public:
  IndexHandle(DiskManager *disk_manager, BufferPoolManager *buffer_pool_manager, table_id_t tid, idx_id_t iid,
      IndexType index_type);

  ~IndexHandle();

  /**
   * insert the given record into the index, rid is recorded in rec
   * @param rec
   */
  void InsertRecord(const Record &rec);

  /**
   * delete the record from the index
   * @param rec
   */
  void DeleteRecord(const Record &rec);

  /**
   * update the old record to the new record in the index, rid is recorded in new_rec
   * @param old_rec
   * @param new_rec
   */
  void UpdateRecord(const Record &old_rec, const Record &new_rec);

  [[nodiscard]] auto GetTableId() const -> table_id_t { return table_id_; }

  [[nodiscard]] auto GetIndexId() const -> idx_id_t { return index_id_; }

  [[nodiscard]] auto GetIndexType() const -> IndexType { return index_->GetIndexType(); }

  auto GetIndex() const -> Index * { return index_; }

  auto GetIndexName() const -> const std::string
  {
    auto file_name = disk_manager_->GetFileName(index_id_);
    return file_name.substr(0, file_name.size() - IDX_SUFFIX.size());
  }

  auto GetKeySchema() const -> const RecordSchema & { return *key_schema_; }

private:
  DiskManager       *disk_manager_;
  BufferPoolManager *buffer_pool_manager_;
  table_id_t         table_id_;
  idx_id_t           index_id_;
  Index             *index_;
  RecordSchemaUptr   key_schema_;
};

DEFINE_UNIQUE_PTR(IndexHandle);

}  // namespace wsdb

#endif  // WSDB_INDEX_HANDLE_H
