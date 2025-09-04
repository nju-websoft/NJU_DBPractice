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

#ifndef NJUDB_INDEX_BP_TREE_H
#define NJUDB_INDEX_BP_TREE_H

#include "index_abstract.h"
#include "common/page.h"
#include "../buffer/page_guard.h"
#include <vector>
#include <memory>
#include <shared_mutex>

namespace njudb {

// Forward declarations
struct BPTreeNode;
struct BPTreeLeafNode;
struct BPTreeInternalNode;

// header should be located at the FILE_HEADER_PAGE, should manully set page_num and manage free pages
struct BPTreeIndexHeader
{
  page_id_t first_free_page_id_{INVALID_PAGE_ID};
  size_t    key_size_{0};
  size_t    value_size_{sizeof(RID)};
  size_t    num_entries_{0};
  page_id_t root_page_id_{INVALID_PAGE_ID};
  size_t    tree_height_{0};
  size_t    page_num_{0};
  size_t    leaf_max_size_{0};
  size_t    internal_max_size_{0};
};

// B+ tree node types
enum class BPTreeNodeType
{
  INTERNAL,
  LEAF
};

// B+ tree page structure
struct BPTreePage
{
  idx_id_t       index_id_;
  BPTreeNodeType node_type_;
  size_t         size_;
  size_t         max_size_;
  page_id_t      parent_page_id_;
  page_id_t      page_id_;

  void Init(idx_id_t index_id, page_id_t page_id, page_id_t parent_id, BPTreeNodeType node_type, int max_size);
  auto IsLeaf() const -> bool;
  auto IsRoot() const -> bool;
  auto GetSize() const -> int;
  auto GetMaxSize() const -> int;
  void SetSize(int size);
  auto GetPageId() const -> page_id_t;
  auto GetParentPageId() const -> page_id_t;
  void SetParentPageId(page_id_t parent_page_id);
  auto IsSafe(bool is_insert) const -> bool;
};

// Internal node structure
struct BPTreeInternalPage : public BPTreePage
{
  int key_size_;
  // Data layout: [BPTreeInternalPage header][children array][keys array]
  char data_[0];  // Flexible array member for both children and keys

  void Init(idx_id_t index_id, page_id_t page_id, page_id_t parent_id, int key_size, int max_size);
  auto KeyAt(int index) const -> const char *;
  auto GetKeySize() const -> int;
  void SetKeyAt(int index, const char *key);
  auto ValueAt(int index) const -> page_id_t;
  void SetValueAt(int index, page_id_t value);
  auto Lookup(const Record &key, const RecordSchema *schema) const -> page_id_t;
  auto LookupForLowerBound(const Record &key, const RecordSchema *schema) const -> page_id_t;
  auto LookupForUpperBound(const Record &key, const RecordSchema *schema) const -> page_id_t;
  void PopulateNewRoot(page_id_t old_root_id, const Record &new_key, page_id_t new_page_id);
  auto InsertNodeAfter(page_id_t old_value, const Record &new_key, page_id_t new_value) -> int;
  void MoveHalfTo(BPTreeInternalPage *recipient, BufferPoolManager *buffer_pool_manager);
  void MoveAllTo(BPTreeInternalPage *recipient, const Record &middle_key, BufferPoolManager *buffer_pool_manager);
  void CopyNFrom(const char *keys, const page_id_t *values, int size, BufferPoolManager *buffer_pool_manager);

private:
  // Helpers to locate the position of children and keys inside the page.
  auto GetChildrenArray() -> page_id_t * { return reinterpret_cast<page_id_t *>(data_); }
  auto GetKeysArray() -> char * { return data_ + (max_size_ + 1) * sizeof(page_id_t); }
  auto GetChildrenArray() const -> const page_id_t * { return reinterpret_cast<const page_id_t *>(data_); }
  auto GetKeysArray() const -> const char * { return data_ + (max_size_ + 1) * sizeof(page_id_t); }
};

// Leaf node structure
struct BPTreeLeafPage : public BPTreePage
{
  page_id_t next_page_id_;
  int       key_size_;
  // Data layout: [BPTreeLeafPage header][RID values array][keys array]
  char data_[0];  // Flexible array member for both values and keys

  void Init(idx_id_t index_id, page_id_t page_id, page_id_t parent_id, int key_size, int max_size);
  auto KeyAt(int index) const -> const char *;
  void SetKeyAt(int index, const char *key);
  auto ValueAt(int index) const -> RID;
  void SetValueAt(int index, const RID &value);
  auto GetNextPageId() const -> page_id_t;
  void SetNextPageId(page_id_t next_page_id);
  auto KeyIndex(const Record &key, const RecordSchema *schema) const -> int;
  auto LowerBound(const Record &key, const RecordSchema *schema) const -> int;
  auto UpperBound(const Record &key, const RecordSchema *schema) const -> int;
  auto Lookup(const Record &key, const RecordSchema *schema) const -> std::vector<RID>;
  auto Insert(const Record &key, const RID &value, const RecordSchema *schema) -> int;
  void MoveHalfTo(BPTreeLeafPage *recipient);
  void MoveAllTo(BPTreeLeafPage *recipient);
  auto RemoveRecord(const Record &key, const RecordSchema *schema) -> int;
  void CopyNFrom(const char *keys, const RID *values, int size);

private:
  auto GetValuesArray() -> RID * { return reinterpret_cast<RID *>(data_); }
  auto GetKeysArray() -> char * { return data_ + max_size_ * sizeof(RID); }
  auto GetValuesArray() const -> const RID * { return reinterpret_cast<const RID *>(data_); }
  auto GetKeysArray() const -> const char * { return data_ + max_size_ * sizeof(RID); }
};

class BPTreeIndex : public Index
{
public:
  // Iterator implementation
  class BPTreeIterator : public IIterator
  {
  public:
    BPTreeIterator(BPTreeIndex *tree, page_id_t leaf_page_id, int index);
    ~BPTreeIterator() override = default;

    auto IsValid() -> bool override;
    void Next() override;
    auto GetKey() -> Record override;
    auto GetRID() -> RID override;

    auto operator==(const BPTreeIterator &other) const -> bool
    {
      return leaf_page_id_ == other.leaf_page_id_ && index_ == other.index_;
    }

    auto operator!=(const BPTreeIterator &other) const -> bool { return !(*this == other); }

  private:
    BPTreeIndex *tree_;
    page_id_t    leaf_page_id_;
    int          index_;
  };

  BPTreeIndex(
      DiskManager *disk_manager, BufferPoolManager *buffer_pool_manager, idx_id_t index_id, const RecordSchema *key_schema);
  ~BPTreeIndex() override = default;

  // Core operations
  void Insert(const Record &key, const RID &rid) override;
  auto Delete(const Record &key) -> bool override;

  // Search operations
  auto Search(const Record &key) -> std::vector<RID> override;
  auto SearchRange(const Record &low_key, const Record &high_key) -> std::vector<RID> override;

  // Iterator interface
  auto Begin() -> std::unique_ptr<IIterator> override;
  auto Begin(const Record &key) -> std::unique_ptr<IIterator> override;
  auto End() -> std::unique_ptr<IIterator> override;

  // Maintenance operations
  void Clear() override;
  auto IsEmpty() -> bool override;
  auto Size() -> size_t override;

  // Index statistics
  auto GetHeight() -> int override;

  static auto GetIndexHeaderSize() -> size_t { return sizeof(BPTreeIndexHeader); }

private:
  // Helper functions
  void InitializeIndex();
  auto NewPage() -> page_id_t;
  void DeletePage(page_id_t page_id);
  auto FindLeafPage(const Record &key, bool leftMost = false) -> page_id_t;
  auto FindLeafPageForRange(const Record &key, bool isLowerBound = true) -> page_id_t;
  void StartNewTree(const Record &key, const RID &value);
  auto InsertIntoLeaf(const Record &key, const RID &value) -> bool;
  void InsertIntoParent(page_id_t old_node_id, const Record &key, page_id_t new_node_id);
  void InsertIntoNewRoot(page_id_t old_root_id, const Record &key, page_id_t new_page_id);
  auto CoalesceOrRedistribute(page_id_t node_id) -> bool;
  auto Coalesce(page_id_t neighbor_node_id, page_id_t node_id, page_id_t parent_id, int index) -> bool;
  void Redistribute(BPTreePage *neighbor_node, BPTreePage *node, int index);
  auto AdjustRoot(BPTreePage *old_root_node) -> bool;
  void ClearPage(page_id_t page_id);

  // Constants
  static constexpr int LEAF_PAGE_SIZE     = PAGE_SIZE;
  static constexpr int INTERNAL_PAGE_SIZE = PAGE_SIZE;

  // for the current implementation, we use a global latch to synchronize access to the index,
  // which is not optimal for performance but simplifies the implementation.
  // a more proper practice is to use Crab Walking (Lock Coupling) Protocol for a fine-grained locking mechanism.
  mutable std::shared_mutex index_latch_;
};

}  // namespace njudb

#endif  // NJUDB_INDEX_BP_TREE_H
