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

#include "index_manager.h"
#include "common/config.h"
#include "common/types.h"
#include <filesystem>

namespace njudb {
void IndexManager::CreateIndex(const std::string &db_name, const std::string &index_name, const std::string &table_name,
    const RecordSchema &schema, IndexType index_type)
{
  // Generate index name based on table and columns
  auto index_file_name = table_name + "_" + index_name;

  // Create the index file
  auto full_path = FILE_NAME(db_name, index_file_name, IDX_SUFFIX);
  disk_manager_->CreateFile(full_path);

  // Open the index file
  auto index_fd        = disk_manager_->OpenFile(full_path);
  auto schema_raw_size = schema.SerializeSize();

  size_t index_header_size;
  if (index_type == IndexType::BPTREE) {
    index_header_size = BPTreeIndex::GetIndexHeaderSize();
  } else {
    index_header_size = HashIndex::GetIndexHeaderSize();
  }
  // Write record schema
  {
    std::vector<char> schema_data(schema_raw_size);
    schema.Serialize(schema_data.data());
    disk_manager_->WriteFile(index_fd, schema_data.data(), schema_raw_size, SEEK_SET, index_header_size);
  }

  // Write index name (null-terminated)
  size_t offset = index_header_size + schema_raw_size;
  {
    auto              index_name_size = index_name.size() + 1;  // +1 for null terminator
    std::vector<char> index_name_data(index_name_size);
    std::memcpy(index_name_data.data(), index_name.c_str(), index_name.size());
    index_name_data[index_name.size()] = '\0';  // Explicit null termination
    disk_manager_->WriteFile(index_fd, index_name_data.data(), index_name_size, SEEK_SET, offset);
    offset += index_name_size;
  }

  // Write index type
  {
    auto index_type_data = static_cast<int>(index_type);
    disk_manager_->WriteFile(index_fd, reinterpret_cast<const char *>(&index_type_data), sizeof(int), SEEK_SET, offset);
  }
  if (index_type == IndexType::BPTREE) {
    auto index = std::make_unique<BPTreeIndex>(disk_manager_, buffer_pool_manager_, index_fd, &schema);
    (void)index;
  } else {
    auto index = std::make_unique<HashIndex>(disk_manager_, buffer_pool_manager_, index_fd, &schema);
    (void)index;
  }

  // Close the index file
  buffer_pool_manager_->FlushAllPages(index_fd);
  disk_manager_->CloseFile(index_fd);
}

void IndexManager::DropIndex(const std::string &db_name, const std::string &index_name, const std::string &table_name)
{

  // Drop the index file
  auto index_file_name = table_name + "_" + index_name;
  auto full_path       = FILE_NAME(db_name, index_file_name, IDX_SUFFIX);
  disk_manager_->DestroyFile(full_path);
  // meta data will be removed by db handler
}

IndexHandleUptr IndexManager::OpenIndex(
    const std::string &db_name, const std::string &index_name, const std::string table_name, IndexType index_type)
{
  // Check if index exists
  auto index_file_name = table_name + "_" + index_name;

  // Open the index file
  auto full_path = FILE_NAME(db_name, index_file_name, IDX_SUFFIX);
  auto index_fd  = disk_manager_->OpenFile(full_path);
  auto table_fd  = disk_manager_->GetFileId(FILE_NAME(db_name, table_name, TAB_SUFFIX));

  // Read the index header to get schema
  char file_header_data[PAGE_SIZE];
  disk_manager_->ReadPage(index_fd, FILE_HEADER_PAGE_ID, file_header_data);
  auto   schema = std::make_unique<RecordSchema>();
  size_t cursor;
  if (index_type == IndexType::BPTREE) {
    cursor = BPTreeIndex::GetIndexHeaderSize();
  } else if (index_type == IndexType::HASH) {
    cursor = HashIndex::GetIndexHeaderSize();
  } else {
    NJUDB_FATAL("Unknown index type");
  }
  cursor += schema->Deserialize(file_header_data + cursor);
  schema->SetTableId(table_fd);
  // Read index name
  std::string index_name_dummy(file_header_data + cursor);
  cursor += index_name_dummy.size() + 1;  // +1 for null terminator
  // Read index type
  int index_type_data;
  std::memcpy(&index_type_data, file_header_data + cursor, sizeof(int));
  cursor += sizeof(int);
  IndexType index_type_dummy = static_cast<IndexType>(index_type_data);

  // check if the index name and type match
  NJUDB_ASSERT(index_type_dummy == index_type,
      fmt::format("Expected index type {}, got {}", static_cast<int>(index_type), index_type_data));
  NJUDB_ASSERT(
      index_name_dummy == index_name, fmt::format("Expected index name {}, got {}", index_name, index_name_dummy));

  return std::make_unique<IndexHandle>(
      disk_manager_, buffer_pool_manager_, table_fd, index_fd, index_type, std::move(schema), index_name);
}

void IndexManager::CloseIndex(const IndexHandle &index_handle)
{
  // Flush all pages in the index file
  buffer_pool_manager_->FlushAllPages(index_handle.GetIndexId());
  // delete all pages in the index file
  buffer_pool_manager_->DeleteAllPages(index_handle.GetIndexId());
  // Close the underlying index file
  disk_manager_->CloseFile(index_handle.GetIndexId());
}

auto IndexManager::GetIndexId(const std::string &db_name, const std::string &index_name, const std::string &table_name)
    -> idx_id_t
{
  auto index_file_name = table_name + "_" + index_name;
  auto full_path       = FILE_NAME(db_name, index_file_name, IDX_SUFFIX);
  return disk_manager_->GetFileId(full_path);
}

auto IndexManager::IndexExists(const std::string &db_name, const std::string &index_name, const std::string &table_name)
    -> bool
{
  auto index_file_name = table_name + "_" + index_name;
  auto full_path       = FILE_NAME(db_name, index_file_name, IDX_SUFFIX);
  return disk_manager_->FileExists(full_path);
}

auto IndexManager::ListIndexes(const std::string &db_name) -> std::vector<std::string>
{
  std::vector<std::string> indexes;

  // This is a simplified implementation
  // In practice, this should read from database metadata or scan directory

  try {
    for (const auto &entry : std::filesystem::directory_iterator(db_name)) {
      if (entry.is_regular_file() && entry.path().extension() == IDX_SUFFIX) {
        auto filename = entry.path().stem().string();
        indexes.push_back(filename);
      }
    }
  } catch (const std::filesystem::filesystem_error &) {
    // Directory doesn't exist or can't be accessed
  }

  return indexes;
}

void IndexManager::RebuildIndex(const std::string &db_name, const std::string &index_name)
{
  // This is a placeholder for index rebuild functionality
  // In practice, this would:
  // 1. Read all records from the table
  // 2. Clear the existing index
  // 3. Re-insert all records into the index

  NJUDB_THROW(NJUDB_NOT_IMPLEMENTED, "RebuildIndex not yet implemented");
}

}  // namespace njudb
