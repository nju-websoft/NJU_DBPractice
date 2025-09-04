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

#ifndef NJUDB_INDEX_HANDLE_H
#define NJUDB_INDEX_HANDLE_H
#include "storage/index/index.h"

namespace njudb {
class IndexHandle
{
public:
  IndexHandle(DiskManager *disk_manager, BufferPoolManager *buffer_pool_manager, table_id_t tid, idx_id_t iid,
      IndexType index_type, RecordSchemaUptr key_schema, std::string index_name);

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

  [[nodiscard]] auto GetIndexId() const -> idx_id_t { return index_->GetIndexId(); }

  [[nodiscard]] auto GetIndexType() const -> IndexType { return index_->GetIndexType(); }

  auto GetIndex() const -> Index* { return index_.get(); }

  auto GetIndexName() const -> const std::string & { return index_name_; }

  auto GetKeySchema() const -> const RecordSchema & { return *index_->GetKeySchema(); }

  // Additional search operations
  /**
   * @brief Search for records matching the given key.
   *
   * @param key key has the same schema as the index key schema
   * @return std::vector<RID>
   */
  auto Search(const Record &key) -> std::vector<RID> { return index_->Search(key); }

  /**
   * @brief Search for records within a range defined by low_key and high_key.
   *
   * @param low_key The lower bound key (inclusive).
   * @param high_key The upper bound key (inclusive).
   * @return std::vector<RID> A vector of RIDs that match the search criteria.
   */
  auto SearchRange(const Record &low_key, const Record &high_key) -> std::vector<RID>;

  auto CheckRecordExists(const Record &record) -> bool;

  // Iterator operations
  auto Begin() -> std::unique_ptr<Index::IIterator> { return index_->Begin(); }

  auto Begin(const Record &key) -> std::unique_ptr<Index::IIterator> { return index_->Begin(key); }

  auto End() -> std::unique_ptr<Index::IIterator> { return index_->End(); }

  // Index statistics
  auto GetHeight() -> int { return index_->GetHeight(); }

  auto Size() -> size_t { return index_->Size(); }

  auto IsEmpty() -> bool { return index_->IsEmpty(); }

  // Maintenance operations
  void Clear() { index_->Clear(); }

  auto PrintIndexStats() -> std::string;

private:
  table_id_t             table_id_;
  std::unique_ptr<Index> index_;
  std::string            index_name_;
  RecordSchemaUptr       key_schema_holder_;
};

DEFINE_UNIQUE_PTR(IndexHandle);

}  // namespace njudb

#endif  // NJUDB_INDEX_HANDLE_H
