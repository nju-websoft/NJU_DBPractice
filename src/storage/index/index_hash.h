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

#ifndef NJUDB_INDEX_HASH_H
#define NJUDB_INDEX_HASH_H

#include "index_abstract.h"
#include "common/config.h"
#include <vector>
#include <unordered_map>

#define HASH_KEY_PAGE 1

namespace njudb {

// Hash index header page structure (page 0)
struct HashHeaderPage
{
  size_t    bucket_count_;
  size_t    total_entries_;
  page_id_t next_page_id_;  // For allocating new pages
};

// Hash bucket directory page structure (page 1)
struct HashBucketDirectory
{
  page_id_t bucket_page_ids_[0];  // Directory of bucket page IDs
};

// Hash bucket page structure - stores actual key-value pairs
struct HashBucketPage
{
  page_id_t next_page_id_;  // For overflow chaining, INVALID_PAGE_ID if no overflow
  size_t    entry_count_;
  char      data_[0];  // Flexible array for storing serialized key-value pairs

  // Calculate maximum entries that can fit in a page
  static auto GetMaxEntries(size_t key_size) -> size_t;

  // Serialize/deserialize entries
  void WriteEntry(size_t offset, const Record &key, const RID &rid);
  void ReadEntry(size_t offset, Record &key, RID &rid, size_t key_size) const;
};

class HashIndex : public Index
{
public:
  HashIndex(DiskManager *disk_manager, BufferPoolManager *buffer_pool_manager, idx_id_t index_id,
      const RecordSchema *key_schema);

  // Core operations
  void Insert(const Record &key, const RID &rid) override;
  auto Delete(const Record &key) -> bool override;

  // Search operations
  auto Search(const Record &key) -> std::vector<RID> override;
  auto SearchRange(const Record &low_key, const Record &high_key) -> std::vector<RID> override;

  // Iterator interface
  class HashIterator : public IIterator
  {
    friend class HashIndex;

  public:
    HashIterator(HashIndex *index, bool is_end = false);
    auto IsValid() -> bool override;
    void Next() override;
    auto GetKey() -> Record override;
    auto GetRID() -> RID override;

  private:
    HashIndex *index_;
    size_t     current_bucket_;
    size_t     current_entry_;
    page_id_t  current_page_id_;
    bool       is_end_;
    void       FindNextValidEntry();
  };

  auto Begin() -> std::unique_ptr<IIterator> override;
  auto Begin(const Record &key) -> std::unique_ptr<IIterator> override;
  auto End() -> std::unique_ptr<IIterator> override;

  // Maintenance operations
  void Clear() override;
  auto IsEmpty() -> bool override;
  auto Size() -> size_t override;

  // Index statistics
  auto GetHeight() -> int override;

  static auto GetIndexHeaderSize() -> size_t { return sizeof(HashHeaderPage); }

private:
  // Hash index specific fields
  size_t bucket_count_;
  size_t total_entries_;
  size_t key_size_;  // Size of each key in bytes

  // Hash function
  auto Hash(const Record &key) -> size_t;

  // Page management
  auto GetHashHeaderPage() -> HashHeaderPage *;
  auto GetBucketDirectory() -> HashBucketDirectory *;
  auto GetBucketPage(page_id_t page_id) -> HashBucketPage *;
  auto AllocateBucketPage() -> page_id_t;
  void InitializeHashIndex();

  // Helper methods for bucket operations
  void InsertIntoBucket(size_t bucket_index, const Record &key, const RID &rid);
  auto DeleteAllFromBucket(size_t bucket_index, const Record &key) -> size_t;
  auto SearchInBucket(size_t bucket_index, const Record &key) -> std::vector<RID>;

  static constexpr size_t DEFAULT_BUCKET_COUNT = 500;
  static constexpr double MAX_LOAD_FACTOR      = 0.75;
};

}  // namespace njudb

#endif  // NJUDB_INDEX_HASH_H
