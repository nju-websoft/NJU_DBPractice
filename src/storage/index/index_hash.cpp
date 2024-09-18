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

#include "index_hash.h"

namespace wsdb {

// FIXME: HashIndex initialization should include more information, such as bucket size, etc.
HashIndex::HashIndex(
    DiskManager *disk_manager, BufferPoolManager *buffer_pool_manager, idx_id_t index_id, RecordSchema *key_schema)
    : Index(disk_manager, buffer_pool_manager, IndexType::HASH, index_id, key_schema)
{
  WSDB_THROW(WSDB_NOT_IMPLEMENTED, "");
}
void HashIndex::Insert(const Record &key, const RID &rid) {}
void HashIndex::Delete(const Record &key, const RID &rid) {}
}  // namespace wsdb