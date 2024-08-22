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

#ifndef WSDB_INDEX_HASH_H
#define WSDB_INDEX_HASH_H

#include "index_abstract.h"

namespace wsdb {

class HashIndex : public Index
{
public:
  HashIndex(DiskManager *disk_manager, BufferPoolManager *buffer_pool_manager, idx_id_t index_id, RecordSchema *key_schema);

  void Insert(const Record &key, const RID &rid) override;

  void Delete(const Record &key, const RID &rid) override;
};

}  // namespace wsdb

#endif  // WSDB_INDEX_HASH_H
