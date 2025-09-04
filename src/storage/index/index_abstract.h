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

#ifndef NJUDB_INDEX_ABSTRACT_H
#define NJUDB_INDEX_ABSTRACT_H

#include "storage/buffer/buffer_pool_manager.h"
#include "storage/disk/disk_manager.h"
#include "common/record.h"

namespace njudb {

class Index
{
public:
  Index() = delete;

  Index(DiskManager *disk_manager, BufferPoolManager *buffer_pool_manager, IndexType index_type, idx_id_t index_id,
      const RecordSchema *key_schema)
      : disk_manager_(disk_manager),
        buffer_pool_manager_(buffer_pool_manager),
        index_type_(index_type),
        index_id_(index_id),
        key_schema_(key_schema)
  {}

  virtual ~Index() = default;

  virtual void Insert(const Record &key, const RID &rid) = 0;

  virtual auto Delete(const Record &key) -> bool = 0;

  // Search operations
  virtual auto Search(const Record &key) -> std::vector<RID> = 0;

  virtual auto SearchRange(const Record &low_key, const Record &high_key) -> std::vector<RID> = 0;

  // Iterator interface for range scans
  class IIterator
  {
  public:
    virtual ~IIterator()            = default;
    virtual auto IsValid() -> bool  = 0;
    virtual void Next()             = 0;
    virtual auto GetKey() -> Record = 0;
    virtual auto GetRID() -> RID    = 0;
  };

  virtual auto Begin() -> std::unique_ptr<IIterator>                  = 0;
  virtual auto Begin(const Record &key) -> std::unique_ptr<IIterator> = 0;
  virtual auto End() -> std::unique_ptr<IIterator>                    = 0;

  // Maintenance operations
  virtual void Clear()           = 0;
  virtual auto IsEmpty() -> bool = 0;
  virtual auto Size() -> size_t  = 0;

  // Index statistics and metadata
  virtual auto GetHeight() -> int = 0;
  virtual auto GetKeySchema() -> const RecordSchema * { return key_schema_; }
  virtual auto GetIndexId() -> idx_id_t { return index_id_; }

  [[nodiscard]] auto GetIndexType() const -> IndexType { return index_type_; }

protected:
  DiskManager       *disk_manager_;
  BufferPoolManager *buffer_pool_manager_;
  IndexType          index_type_;
  idx_id_t           index_id_;
  const RecordSchema      *key_schema_;
};

}  // namespace njudb

#endif  // NJUDB_INDEX_ABSTRACT_H
