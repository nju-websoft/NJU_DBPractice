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

#include "index_bptree.h"
#include "../../../common/error.h"
#include "../buffer/page_guard.h"
#include <algorithm>
#include <cstring>
#include <vector>
#include <string>

#define TEST_BPTREE

namespace njudb {

static void DebugPrintLeaf(const BPTreeLeafPage *leaf_node, const RecordSchema *key_schema)
{
  return;
  // print all keys in the leaf node for debugging
  page_id_t leaf_page_id = leaf_node->GetPageId();
  printf("Leaf %d: ", leaf_page_id);
  for (int i = 0; i < leaf_node->GetSize(); i++) {
    Record current_key(key_schema, nullptr, leaf_node->KeyAt(i), INVALID_RID);
    printf("key[%d]: %s; ", i, current_key.GetValueAt(0)->ToString().c_str());
  }
  printf("\n");
}

static void DebugPrintInternal(const BPTreeInternalPage *internal_node, const RecordSchema *key_schema)
{
  return;
  // print all keys in the internal node for debugging
  page_id_t internal_page_id = internal_node->GetPageId();
  printf("Internal %d: ", internal_page_id);
  for (int i = 0; i < internal_node->GetSize(); i++) {
    Record current_key(key_schema, nullptr, internal_node->KeyAt(i), INVALID_RID);
    printf("key[%d]: %s, value: %d; ", i, current_key.GetValueAt(0)->ToString().c_str(), internal_node->ValueAt(i));
  }
  printf("\n");
}

// BPTreePage implementation
void BPTreePage::Init(idx_id_t index_id, page_id_t page_id, page_id_t parent_id, BPTreeNodeType node_type, int max_size)
{
  NJUDB_STUDENT_TODO(l4, t1);
}

auto BPTreePage::IsLeaf() const -> bool
{
  NJUDB_STUDENT_TODO(l4, t1);
  return false;
}

auto BPTreePage::IsRoot() const -> bool
{
  NJUDB_STUDENT_TODO(l4, t1);
  return false;
}

auto BPTreePage::GetSize() const -> int
{
  NJUDB_STUDENT_TODO(l4, t1);
  return 0;
}

auto BPTreePage::GetMaxSize() const -> int
{
  NJUDB_STUDENT_TODO(l4, t1);
  return 0;
}

void BPTreePage::SetSize(int size) { NJUDB_STUDENT_TODO(l4, t1); }

auto BPTreePage::GetPageId() const -> page_id_t
{
  NJUDB_STUDENT_TODO(l4, t1);
  return INVALID_PAGE_ID;
}

auto BPTreePage::GetParentPageId() const -> page_id_t
{
  NJUDB_STUDENT_TODO(l4, t1);
  return INVALID_PAGE_ID;
}

void BPTreePage::SetParentPageId(page_id_t parent_page_id) { NJUDB_STUDENT_TODO(l4, t1); }

auto BPTreePage::IsSafe(bool is_insert) const -> bool
{
  NJUDB_STUDENT_TODO(l4, t1);
  return false;
}

// BPTreeLeafPage implementation
void BPTreeLeafPage::Init(idx_id_t index_id, page_id_t page_id, page_id_t parent_id, int key_size, int max_size)
{
  NJUDB_STUDENT_TODO(l4, t1);
}

auto BPTreeLeafPage::GetNextPageId() const -> page_id_t
{
  NJUDB_STUDENT_TODO(l4, t1);
  return INVALID_PAGE_ID;
}

void BPTreeLeafPage::SetNextPageId(page_id_t next_page_id) { NJUDB_STUDENT_TODO(l4, t1); }

auto BPTreeLeafPage::KeyAt(int index) const -> const char *
{
  NJUDB_STUDENT_TODO(l4, t1);
  return nullptr;
}

auto BPTreeLeafPage::ValueAt(int index) const -> RID
{
  NJUDB_STUDENT_TODO(l4, t1);
  return INVALID_RID;
}

void BPTreeLeafPage::SetKeyAt(int index, const char *key) { NJUDB_STUDENT_TODO(l4, t1); }

void BPTreeLeafPage::SetValueAt(int index, const RID &value) { NJUDB_STUDENT_TODO(l4, t1); }

auto BPTreeLeafPage::KeyIndex(const Record &key, const RecordSchema *schema) const -> int
{
  NJUDB_STUDENT_TODO(l4, t1);
  return size_;
}

auto BPTreeLeafPage::LowerBound(const Record &key, const RecordSchema *schema) const -> int
{
  // Find the first position where key <= keys[pos]
  // This is useful for >= queries
  NJUDB_STUDENT_TODO(l4, t1);
  return size_;
}

auto BPTreeLeafPage::UpperBound(const Record &key, const RecordSchema *schema) const -> int
{
  // Find the first position where key < keys[pos]
  // This is useful for < queries
  NJUDB_STUDENT_TODO(l4, t1);
  return size_;
}

auto BPTreeLeafPage::Lookup(const Record &key, const RecordSchema *schema) const -> std::vector<RID>
{
  NJUDB_STUDENT_TODO(l4, t1);
  return {};
}

auto BPTreeLeafPage::Insert(const Record &key, const RID &value, const RecordSchema *schema) -> int
{
  NJUDB_STUDENT_TODO(l4, t1);
  return size_;
}

void BPTreeLeafPage::MoveHalfTo(BPTreeLeafPage *recipient) { NJUDB_STUDENT_TODO(l4, t1); }

void BPTreeLeafPage::CopyNFrom(const char *keys, const RID *values, int size) { NJUDB_STUDENT_TODO(l4, t1); }

auto BPTreeLeafPage::RemoveRecord(const Record &key, const RecordSchema *schema) -> int
{
  NJUDB_STUDENT_TODO(l4, t1);
  return -1;  // Key-RID pair not found
}

void BPTreeLeafPage::MoveAllTo(BPTreeLeafPage *recipient) { NJUDB_STUDENT_TODO(l4, t1); }

// BPTreeInternalPage implementation
void BPTreeInternalPage::Init(idx_id_t index_id, page_id_t page_id, page_id_t parent_id, int key_size, int max_size)
{
  NJUDB_STUDENT_TODO(l4, t1);
}

auto BPTreeInternalPage::KeyAt(int index) const -> const char *
{
  NJUDB_STUDENT_TODO(l4, t1);
  return nullptr;
}

auto BPTreeInternalPage::GetKeySize() const -> int
{
  NJUDB_STUDENT_TODO(l4, t1);
  return 0;
}

auto BPTreeInternalPage::ValueAt(int index) const -> page_id_t
{
  NJUDB_STUDENT_TODO(l4, t1);
  return INVALID_PAGE_ID;
}

void BPTreeInternalPage::SetKeyAt(int index, const char *key) { NJUDB_STUDENT_TODO(l4, t1); }

void BPTreeInternalPage::SetValueAt(int index, page_id_t value) { NJUDB_STUDENT_TODO(l4, t1); }

auto BPTreeInternalPage::Lookup(const Record &key, const RecordSchema *schema) const -> page_id_t
{
  NJUDB_STUDENT_TODO(l4, t1);
  return INVALID_PAGE_ID;
}

auto BPTreeInternalPage::LookupForLowerBound(const Record &key, const RecordSchema *schema) const -> page_id_t
{
  // For lower bound, we want to find the leftmost position where key could be inserted
  // This means finding the leftmost child that could contain keys >= key
  int index = 1;  // Start from 1 since first key is invalid

  NJUDB_STUDENT_TODO(l4, t1);
  return INVALID_PAGE_ID;
}

auto BPTreeInternalPage::LookupForUpperBound(const Record &key, const RecordSchema *schema) const -> page_id_t
{
  // For upper bound, we want to find the rightmost position where key could be inserted
  // This means finding the rightmost child that could contain keys <= key
  int index = 1;  // Start from 1 since first key is invalid

  NJUDB_STUDENT_TODO(l4, t1);
  return INVALID_PAGE_ID;
}

void BPTreeInternalPage::PopulateNewRoot(page_id_t old_root_id, const Record &new_key, page_id_t new_page_id)
{
  NJUDB_STUDENT_TODO(l4, t1);
}

auto BPTreeInternalPage::InsertNodeAfter(page_id_t old_value, const Record &new_key, page_id_t new_value) -> int
{
  NJUDB_STUDENT_TODO(l4, t1);
  return -1;
}

void BPTreeInternalPage::MoveHalfTo(BPTreeInternalPage *recipient, BufferPoolManager *buffer_pool_manager)
{
  NJUDB_STUDENT_TODO(l4, t1);
}

void BPTreeInternalPage::CopyNFrom(
    const char *keys, const page_id_t *values, int size, BufferPoolManager *buffer_pool_manager)
{
  NJUDB_STUDENT_TODO(l4, t1);
}

void BPTreeInternalPage::MoveAllTo(
    BPTreeInternalPage *recipient, const Record &middle_key, BufferPoolManager *buffer_pool_manager)
{
  // For internal nodes, we need to merge:
  // 1. The middle key from the parent (this becomes a key in the recipient)
  // 2. All keys and children from the source node

  NJUDB_STUDENT_TODO(l4, t1);
}

// BPTreeIndex implementation
BPTreeIndex::BPTreeIndex(DiskManager *disk_manager, BufferPoolManager *buffer_pool_manager, idx_id_t index_id,
    const RecordSchema *key_schema)
    : Index(disk_manager, buffer_pool_manager, IndexType::BPTREE, index_id, key_schema)
{

  // Initialize index header
  InitializeIndex();
}

void BPTreeIndex::InitializeIndex()
{
  // Get or create header page
  auto header_guard = buffer_pool_manager_->FetchPageWrite(index_id_, FILE_HEADER_PAGE_ID);
  if (!header_guard.IsValid()) {
    NJUDB_THROW(NJUDB_EXCEPTION_EMPTY, "Cannot fetch header page");
  }

  auto header = reinterpret_cast<BPTreeIndexHeader *>(header_guard.GetMutableData());

  // Check if already initialized
  if (header->page_num_ != 0) {
    return;
  }

  // first check if the header and the schema raw data can accomodate int the header file
  if (key_schema_->SerializeSize() + sizeof(BPTreeIndexHeader) > PAGE_SIZE) {
    NJUDB_THROW(NJUDB_INDEX_FAIL, "Key schema too large to fit in B+ tree header");
  }

  // Initialize header
  header->root_page_id_       = INVALID_PAGE_ID;
  header->first_free_page_id_ = INVALID_PAGE_ID;
  header->tree_height_        = 0;
  header->page_num_           = 1;  // Header page counts
  header->key_size_           = key_schema_->GetRecordLength();
  header->value_size_         = sizeof(RID);

  // Note: TEST_BPTREE mode is for testing your B+tree implementation.
  // you can only undef it after you have passed gtests and is ready to use it in executors.
#ifdef TEST_BPTREE
  std::cout << "TEST_BPTREE mode: using fixed max sizes" << std::endl;
  header->leaf_max_size_     = 4;
  header->internal_max_size_ = 4;
#else
  // Calculate max sizes based on page size

  size_t leaf_header_size     = sizeof(BPTreeLeafPage);
  size_t available_leaf_space = PAGE_SIZE - PAGE_HEADER_SIZE - leaf_header_size;
  header->leaf_max_size_      = available_leaf_space / (header->key_size_ + sizeof(RID));

  size_t internal_header_size     = sizeof(BPTreeInternalPage);
  size_t available_internal_space = PAGE_SIZE - PAGE_HEADER_SIZE - internal_header_size;
  header->internal_max_size_      = available_internal_space / (header->key_size_ + sizeof(page_id_t));

  // check if the max size of leaf and internal are valid
  if (static_cast<int>(header->leaf_max_size_) <= 0 || static_cast<int>(header->internal_max_size_) <= 0) {
    NJUDB_THROW(NJUDB_INDEX_FAIL, "Key too large for a B+ tree node to fit into a single page");
  }
#endif
}

auto BPTreeIndex::NewPage() -> page_id_t
{
  NJUDB_STUDENT_TODO(l4, t1);
  return INVALID_PAGE_ID;
}

void BPTreeIndex::DeletePage(page_id_t page_id) { NJUDB_STUDENT_TODO(l4, t1); }

auto BPTreeIndex::FindLeafPage(const Record &key, bool leftMost) -> page_id_t
{
  NJUDB_STUDENT_TODO(l4, t1);
  return INVALID_PAGE_ID;
}

auto BPTreeIndex::FindLeafPageForRange(const Record &key, bool isLowerBound) -> page_id_t
{
  NJUDB_STUDENT_TODO(l4, t1);
  return INVALID_PAGE_ID;
}

void BPTreeIndex::StartNewTree(const Record &key, const RID &value) { NJUDB_STUDENT_TODO(l4, t1); }

auto BPTreeIndex::InsertIntoLeaf(const Record &key, const RID &value) -> bool { NJUDB_STUDENT_TODO(l4, t1); }

void BPTreeIndex::InsertIntoParent(page_id_t old_node_id, const Record &key, page_id_t new_node_id)
{
  NJUDB_STUDENT_TODO(l4, t1);
}

void BPTreeIndex::InsertIntoNewRoot(page_id_t old_root_id, const Record &key, page_id_t new_page_id)
{
  NJUDB_STUDENT_TODO(l4, t1);
}

void BPTreeIndex::Insert(const Record &key, const RID &rid) { NJUDB_STUDENT_TODO(l4, t1); }

auto BPTreeIndex::Delete(const Record &key) -> bool
{
  NJUDB_STUDENT_TODO(l4, t1);
  return false;
}

auto BPTreeIndex::CoalesceOrRedistribute(page_id_t node_id) -> bool
{
  NJUDB_STUDENT_TODO(l4, t1);
  return false;
}

auto BPTreeIndex::Coalesce(page_id_t neighbor_node_id, page_id_t node_id, page_id_t parent_id, int index) -> bool
{
  // Fetch all pages needed for coalescing
  NJUDB_STUDENT_TODO(l4, t1);
  return false;
}

void BPTreeIndex::Redistribute(BPTreePage *neighbor_node, BPTreePage *node, int index) { NJUDB_STUDENT_TODO(l4, t1); }

auto BPTreeIndex::AdjustRoot(BPTreePage *old_root_node) -> bool
{
  NJUDB_STUDENT_TODO(l4, t1);
  return false;
}

auto BPTreeIndex::Search(const Record &key) -> std::vector<RID> { NJUDB_STUDENT_TODO(l4, t1); }

auto BPTreeIndex::SearchRange(const Record &low_key, const Record &high_key) -> std::vector<RID>
{
  NJUDB_STUDENT_TODO(l4, t1);
  return {};
}

// Iterator implementation
BPTreeIndex::BPTreeIterator::BPTreeIterator(BPTreeIndex *tree, page_id_t leaf_page_id, int index)
    : tree_(tree), leaf_page_id_(leaf_page_id), index_(index)
{}

auto BPTreeIndex::BPTreeIterator::IsValid() -> bool
{
  NJUDB_STUDENT_TODO(l4, t1);
  return false;
}

void BPTreeIndex::BPTreeIterator::Next()
{
  NJUDB_STUDENT_TODO(l4, t1);
}

auto BPTreeIndex::BPTreeIterator::GetKey() -> Record
{
  NJUDB_STUDENT_TODO(l4, t1);
}

auto BPTreeIndex::BPTreeIterator::GetRID() -> RID
{
  NJUDB_STUDENT_TODO(l4, t1);
  return INVALID_RID;
}

auto BPTreeIndex::Begin() -> std::unique_ptr<IIterator>
{
  NJUDB_STUDENT_TODO(l4, t1);
  return nullptr;
}

auto BPTreeIndex::Begin(const Record &key) -> std::unique_ptr<IIterator>
{
  NJUDB_STUDENT_TODO(l4, t1);
  return nullptr;
}

auto BPTreeIndex::End() -> std::unique_ptr<IIterator>
{
  NJUDB_STUDENT_TODO(l4, t1);
  return nullptr;
}

void BPTreeIndex::Clear()
{
  NJUDB_STUDENT_TODO(l4, t1);
}

void BPTreeIndex::ClearPage(page_id_t page_id)
{
  NJUDB_STUDENT_TODO(l4, t1);
}

auto BPTreeIndex::IsEmpty() -> bool
{
  NJUDB_STUDENT_TODO(l4, t1);
  return false;
}

auto BPTreeIndex::Size() -> size_t
{
  NJUDB_STUDENT_TODO(l4, t1);
  return 0;
}

auto BPTreeIndex::GetHeight() -> int
{
  NJUDB_STUDENT_TODO(l4, t1);
  return 0;
}

}  // namespace njudb
