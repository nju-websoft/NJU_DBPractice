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
#include "../../../common/error.h"

namespace njudb {
IndexHandle::IndexHandle(DiskManager *disk_manager, BufferPoolManager *buffer_pool_manager, table_id_t tid,
    idx_id_t iid, IndexType index_type, RecordSchemaUptr key_schema, std::string index_name)
    : table_id_(tid),
      index_(nullptr),
      index_name_(std::move(index_name)),
      key_schema_holder_(std::move(key_schema))
{
  switch (index_type) {
    case IndexType::BPTREE: {
      index_ = std::make_unique<BPTreeIndex>(disk_manager, buffer_pool_manager, iid, key_schema_holder_.get());
      break;
    }
    case IndexType::HASH: {
      index_ = std::make_unique<HashIndex>(disk_manager, buffer_pool_manager, iid, key_schema_holder_.get());
      break;
    }
    default: NJUDB_FATAL(fmt::format("{}", static_cast<int>(index_type)));
  }
}

void IndexHandle::InsertRecord(const Record &rec)
{
  NJUDB_STUDENT_TODO(l4, t2);
}

void IndexHandle::DeleteRecord(const Record &rec)
{
  NJUDB_STUDENT_TODO(l4, t2);
}

void IndexHandle::UpdateRecord(const Record &old_rec, const Record &new_rec)
{
  NJUDB_STUDENT_TODO(l4, t2);
}

auto IndexHandle::SearchRange(const Record &low_key, const Record &high_key) -> std::vector<RID>
{
  NJUDB_STUDENT_TODO(l4, t2);
  return {};
}

auto IndexHandle::CheckRecordExists(const Record &record) -> bool
{
  NJUDB_STUDENT_TODO(l4, t2);
  return false;
}

auto IndexHandle::PrintIndexStats() -> std::string
{
  if (index_ == nullptr) {
    return "Index not initialized";
  }

  std::string stats;
  stats += fmt::format("Index Type: {}\n", static_cast<int>(index_->GetIndexType()));
  stats += fmt::format("Index ID: {}\n", index_->GetIndexId());
  stats += fmt::format("Size: {}\n", index_->Size());
  stats += fmt::format("Height: {}\n", index_->GetHeight());
  stats += fmt::format("Empty: {}\n", index_->IsEmpty() ? "Yes" : "No");

  return stats;
}

IndexHandle::~IndexHandle() = default;
}  // namespace njudb