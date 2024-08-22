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

#include "index_handle.h"

namespace wsdb {
IndexHandle::IndexHandle(DiskManager *disk_manager, BufferPoolManager *buffer_pool_manager, table_id_t tid,
    idx_id_t iid, IndexType index_type)
    : disk_manager_(disk_manager),
      buffer_pool_manager_(buffer_pool_manager),
      table_id_(tid),
      index_id_(iid),
      index_(nullptr)
{
  // FIXME: index information should be parsed here, not just pass the index id into the index
  // TODO(ziqi): parse index information from FILE_HEADER_PAGE here, including index key schema, index specific
  // information, etc.
  switch (index_type) {
    case IndexType::BPTREE: {
      // bpt_param = ParseBPTreeIndex();
      index_ = new BPTreeIndex(disk_manager, buffer_pool_manager, iid, key_schema_.get());
      break;
    }
    case IndexType::HASH: {
      // hash_param = ParseHashIndex();
      index_ = new HashIndex(disk_manager, buffer_pool_manager, iid, key_schema_.get());
      break;
    }
    default:
      throw WSDBException(
          WSDB_INVALID_ARGUMENT, Q(IndexHandle), Q(IndexHandle), fmt::format("{}", static_cast<int>(index_type)));
  }
}

void IndexHandle::InsertEntry(const Record &rec) {}

void IndexHandle::DeleteEntry(const Record &rec) {}

void IndexHandle::UpdateEntry(const Record &old_rec, const Record &new_rec) {}

IndexHandle::~IndexHandle() { delete index_; }
}  // namespace wsdb