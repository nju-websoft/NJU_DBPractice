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
#include "../buffer/page_guard.h"
#include <algorithm>
#include <functional>
#include <cstring>
#include <stdexcept>

namespace njudb {

// HashBucketPage implementation
auto HashBucketPage::GetMaxEntries(size_t key_size) -> size_t { NJUDB_STUDENT_TODO(l4, f1); }

void HashBucketPage::WriteEntry(size_t offset, const Record &key, const RID &rid) { NJUDB_STUDENT_TODO(l4, f1); }

void HashBucketPage::ReadEntry(size_t offset, Record &key, RID &rid, size_t key_size) const
{
  NJUDB_STUDENT_TODO(l4, f1);
}

// HashIndex implementation
HashIndex::HashIndex(DiskManager *disk_manager, BufferPoolManager *buffer_pool_manager, idx_id_t index_id,
    const RecordSchema *key_schema)
    : Index(disk_manager, buffer_pool_manager, IndexType::HASH, index_id, key_schema),
      bucket_count_(DEFAULT_BUCKET_COUNT),
      total_entries_(0),
      key_size_(key_schema->GetRecordLength())  // Only key data, no null map
{
  InitializeHashIndex();
}

void HashIndex::InitializeHashIndex()
{
  // Create header page for hash index
  auto header_guard = buffer_pool_manager_->FetchPageWrite(index_id_, FILE_HEADER_PAGE_ID);
  auto header_page  = reinterpret_cast<HashHeaderPage *>(header_guard.GetMutableData());

  if (header_page->bucket_count_ != 0) {
    // It is an index that has already been initialized
    bucket_count_  = header_page->bucket_count_;
    total_entries_ = header_page->total_entries_;
    return;
  }

  // Initialize header page
  header_page->bucket_count_  = bucket_count_;
  header_page->total_entries_ = 0;
  header_page->next_page_id_  = 2;  // Start allocating from page 2 (skip header and directory)

  // Initialize bucket directory page
  auto directory_guard = buffer_pool_manager_->FetchPageWrite(index_id_, HASH_KEY_PAGE);
  auto directory_page  = reinterpret_cast<HashBucketDirectory *>(directory_guard.GetMutableData());

  // Calculate how many bucket page IDs can fit in the directory page
  size_t available_space = PAGE_SIZE - sizeof(HashBucketDirectory) - PAGE_HEADER_SIZE;
  size_t max_bucket_refs = available_space / sizeof(page_id_t);

  if (bucket_count_ > max_bucket_refs) {
    // Too many buckets for single directory page - should handle this case
    // For now, limit to available space
    bucket_count_              = max_bucket_refs;
    header_page->bucket_count_ = bucket_count_;
  }

  // Initialize bucket page IDs - initially all invalid
  for (size_t i = 0; i < bucket_count_; ++i) {
    directory_page->bucket_page_ids_[i] = INVALID_PAGE_ID;
  }

  // header_guard and directory_guard will automatically unpin the pages as dirty when they go out of scope
}

auto HashIndex::Hash(const Record &key) -> size_t
{
  // Use the Record's built-in hash function
  return key.Hash() % bucket_count_;
}

auto HashIndex::GetHashHeaderPage() -> HashHeaderPage *
{
  auto page = buffer_pool_manager_->FetchPage(index_id_, FILE_HEADER_PAGE_ID);
  return reinterpret_cast<HashHeaderPage *>(page->GetData());
}

auto HashIndex::GetBucketDirectory() -> HashBucketDirectory *
{
  auto page = buffer_pool_manager_->FetchPage(index_id_, HASH_KEY_PAGE);
  return reinterpret_cast<HashBucketDirectory *>(page->GetData());
}

auto HashIndex::GetBucketPage(page_id_t page_id) -> HashBucketPage *
{
  auto page = buffer_pool_manager_->FetchPage(index_id_, page_id);
  return reinterpret_cast<HashBucketPage *>(PageContentPtr(page->GetData()));
}

auto HashIndex::AllocateBucketPage() -> page_id_t
{
  // Allocate a new bucket page
  page_id_t new_page_id = INVALID_PAGE_ID;

  NJUDB_STUDENT_TODO(l4, f1);

  return new_page_id;
}

void HashIndex::Insert(const Record &key, const RID &rid) { NJUDB_STUDENT_TODO(l4, f1); }

void HashIndex::InsertIntoBucket(size_t bucket_index, const Record &key, const RID &rid) { NJUDB_STUDENT_TODO(l4, f1); }

auto HashIndex::Delete(const Record &key) -> bool { NJUDB_STUDENT_TODO(l4, f1); }

auto HashIndex::DeleteAllFromBucket(size_t bucket_index, const Record &key) -> size_t { NJUDB_STUDENT_TODO(l4, f1); }

auto HashIndex::Search(const Record &key) -> std::vector<RID> { NJUDB_STUDENT_TODO(l4, f1); }

auto HashIndex::SearchInBucket(size_t bucket_index, const Record &key) -> std::vector<RID>
{
  NJUDB_STUDENT_TODO(l4, f1);
  return {};
}

auto HashIndex::SearchRange(const Record &low_key, const Record &high_key) -> std::vector<RID>
{
  // Hash indexes don't support efficient range queries
  // We need to scan all buckets and filter results
  NJUDB_STUDENT_TODO(l4, f1);
  return {};
}

// HashIterator implementation
HashIndex::HashIterator::HashIterator(HashIndex *index, bool is_end)
    : index_(index), current_bucket_(0), current_entry_(0), current_page_id_(INVALID_PAGE_ID), is_end_(is_end)
{
  if (!is_end_) {
    FindNextValidEntry();
  }
}

auto HashIndex::HashIterator::IsValid() -> bool { return !is_end_ && current_bucket_ < index_->bucket_count_; }

void HashIndex::HashIterator::Next() { NJUDB_STUDENT_TODO(l4, f1); }

void HashIndex::HashIterator::FindNextValidEntry() { NJUDB_STUDENT_TODO(l4, f1); }

auto HashIndex::HashIterator::GetKey() -> Record { NJUDB_STUDENT_TODO(l4, f1); }

auto HashIndex::HashIterator::GetRID() -> RID { NJUDB_STUDENT_TODO(l4, f1); }

auto HashIndex::Begin() -> std::unique_ptr<IIterator> { NJUDB_STUDENT_TODO(l4, f1); }

auto HashIndex::Begin(const Record &key) -> std::unique_ptr<IIterator>
{
  // For hash index, Begin(key) is similar to Begin() since there's no ordering
  // We could optionally start from the bucket containing the key
  NJUDB_STUDENT_TODO(l4, f1);
}

auto HashIndex::End() -> std::unique_ptr<IIterator> { return std::make_unique<HashIterator>(this, true); }

void HashIndex::Clear() { NJUDB_STUDENT_TODO(l4, f1); }

auto HashIndex::IsEmpty() -> bool { return total_entries_ == 0; }

auto HashIndex::Size() -> size_t { return total_entries_; }

auto HashIndex::GetHeight() -> int
{
  // Hash indexes have a constant height of 2 (header + bucket pages)
  return 2;
}

}  // namespace njudb