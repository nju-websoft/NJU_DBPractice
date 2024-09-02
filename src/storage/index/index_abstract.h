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
// Created by ziqi on 2024/7/18.
//

#ifndef WSDB_INDEX_ABSTRACT_H
#define WSDB_INDEX_ABSTRACT_H

#include "storage/buffer/buffer_pool_manager.h"
#include "storage/disk/disk_manager.h"
#include "system/handle/record_handle.h"

namespace wsdb {

enum class IndexType
{
  NONE,
  BPTREE,
  HASH,
};

class Index
{
public:
  Index() = delete;

  Index(DiskManager *disk_manager, BufferPoolManager *buffer_pool_manager, IndexType index_type, idx_id_t index_id,
      RecordSchema *key_schema)
      : disk_manager_(disk_manager),
        buffer_pool_manager_(buffer_pool_manager),
        index_type_(index_type),
        index_id_(index_id),
        key_schema_(key_schema)
  {}

  virtual ~Index() = default;

  virtual void Insert(const Record &key, const RID &rid) = 0;

  virtual void Delete(const Record &key, const RID &rid) = 0;

  [[nodiscard]] auto GetIndexType() const -> IndexType { return index_type_; }

private:
  DiskManager       *disk_manager_;
  BufferPoolManager *buffer_pool_manager_;
  IndexType          index_type_;
  idx_id_t           index_id_;
  RecordSchema      *key_schema_;
};

}  // namespace wsdb

#endif  // WSDB_INDEX_ABSTRACT_H
