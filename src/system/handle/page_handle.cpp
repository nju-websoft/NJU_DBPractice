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
// Created by ziqi on 2024/7/27.
//

#include "page_handle.h"
#include "../../../common/error.h"
#include "storage/buffer/buffer_pool_manager.h"

namespace njudb {
PageHandle::PageHandle(const TableHeader *tab_hdr, Page *page, char *bit_map, char *slots_mem)
    : tab_hdr_(tab_hdr), page_(page), bitmap_(bit_map), slots_mem_(slots_mem)
{
  NJUDB_ASSERT(BITMAP_SIZE(tab_hdr->rec_per_page_) == tab_hdr->bitmap_size_, "bitmap size not match");
}

void PageHandle::WriteSlot(size_t slot_id, const char *null_map, const char *data, bool update)
{
  NJUDB_THROW(NJUDB_EXCEPTION_EMPTY, "");
}

void PageHandle::ReadSlot(size_t slot_id, char *null_map, char *data) { NJUDB_THROW(NJUDB_EXCEPTION_EMPTY, ""); }
auto PageHandle::ReadChunk(const RecordSchema *chunk_schema) -> ChunkUptr { NJUDB_THROW(NJUDB_EXCEPTION_EMPTY, ""); }

auto PageHandle::GetRecordHeader(size_t slot_id) -> char* {
  NJUDB_THROW(NJUDB_EXCEPTION_EMPTY, "GetRecordHeader must be implemented by subclass");
}

auto PageHandle::GetRecordHeader(size_t slot_id) const -> const char* {
  NJUDB_THROW(NJUDB_EXCEPTION_EMPTY, "GetRecordHeader must be implemented by subclass");
}

void PageHandle::SetXmin(size_t slot_id, txn_id_t xmin) {
  char *header = GetRecordHeader(slot_id);
  auto *hdr = reinterpret_cast<RecordHeader*>(header);
  hdr->xmin_ = xmin;
}

void PageHandle::SetXmax(size_t slot_id, txn_id_t xmax) {
  char *header = GetRecordHeader(slot_id);
  auto *hdr = reinterpret_cast<RecordHeader*>(header);
  hdr->xmax_ = xmax;
}

auto PageHandle::GetXmin(size_t slot_id) const -> txn_id_t {
  const char *header = GetRecordHeader(slot_id);
  auto *hdr = reinterpret_cast<const RecordHeader*>(header);
  return hdr->xmin_;
}

auto PageHandle::GetXmax(size_t slot_id) const -> txn_id_t {
  const char *header = GetRecordHeader(slot_id);
  auto *hdr = reinterpret_cast<const RecordHeader*>(header);
  return hdr->xmax_;
}

void PageHandle::SetXminCommitted(size_t slot_id) {
  char *header = GetRecordHeader(slot_id);
  RecordHeader::SetCommitted(header, true);
}

void PageHandle::SetXmaxCommitted(size_t slot_id) {
  char *header = GetRecordHeader(slot_id);
  RecordHeader::SetCommitted(header, false);
}

void PageHandle::SetXminInvalid(size_t slot_id) {
  char *header = GetRecordHeader(slot_id);
  RecordHeader::SetInvalid(header, true);
}

void PageHandle::SetXmaxInvalid(size_t slot_id) {
  char *header = GetRecordHeader(slot_id);
  RecordHeader::SetInvalid(header, false);
}

auto PageHandle::IsTupleVisible(size_t slot_id, txn_id_t txn_id) const -> bool {
  const char *header = GetRecordHeader(slot_id);
  // Kept only for recovery/diagnostic compatibility. Runtime MVCC reads use
  // TableHandle::ReadVersionAware() and the undo-version chain.
  return RecordHeader::IsVisible(header, txn_id);
}

NAryPageHandle::NAryPageHandle(const TableHeader *tab_hdr, Page *page)
    : PageHandle(
          tab_hdr, page, page->GetData() + PAGE_HEADER_SIZE, page->GetData() + PAGE_HEADER_SIZE + tab_hdr->bitmap_size_)
{}

void NAryPageHandle::WriteSlot(size_t slot_id, const char *null_map, const char *data, bool update)
{
  NJUDB_ASSERT(slot_id < tab_hdr_->rec_per_page_, "slot_id out of range");
  NJUDB_ASSERT(BitMap::GetBit(bitmap_, slot_id) == update, fmt::format("update: {}", update));
  
  // Calculate slot offset: each slot contains [RecordHeader + Data]
  size_t rec_full_size = tab_hdr_->tuple_header_size_ + tab_hdr_->rec_size_;
  char *slot_ptr = slots_mem_ + slot_id * rec_full_size;
  
  if (!update) {
    // New tuple: initialize header with INVALID_TXN_ID (will be set by caller)
    RecordHeader::Initialize(slot_ptr, INVALID_TXN_ID, tab_hdr_->field_num_);
  }
  
  // Copy nullmap into tuple header
  char *header_nullmap = RecordHeader::GetNullMapPtr(slot_ptr);
  memcpy(header_nullmap, null_map, tab_hdr_->nullmap_size_);
  
  // Copy data after tuple header
  memcpy(slot_ptr + tab_hdr_->tuple_header_size_, data, tab_hdr_->rec_size_);
}

void NAryPageHandle::ReadSlot(size_t slot_id, char *null_map, char *data)
{
  NJUDB_ASSERT(slot_id < tab_hdr_->rec_per_page_, "slot_id out of range");
  NJUDB_ASSERT(BitMap::GetBit(bitmap_, slot_id) == true, "slot is empty");
  
  // Calculate slot offset
  size_t rec_full_size = tab_hdr_->tuple_header_size_ + tab_hdr_->rec_size_;
  const char *slot_ptr = slots_mem_ + slot_id * rec_full_size;
  
  // Read nullmap from tuple header
  const char *header_nullmap = RecordHeader::GetNullMapPtr(slot_ptr);
  memcpy(null_map, header_nullmap, tab_hdr_->nullmap_size_);
  
  // Read data after tuple header
  memcpy(data, slot_ptr + tab_hdr_->tuple_header_size_, tab_hdr_->rec_size_);
}

auto NAryPageHandle::GetRecordHeader(size_t slot_id) -> char* {
  NJUDB_ASSERT(slot_id < tab_hdr_->rec_per_page_, "slot_id out of range");
  size_t rec_full_size = tab_hdr_->tuple_header_size_ + tab_hdr_->rec_size_;
  return slots_mem_ + slot_id * rec_full_size;
}

auto NAryPageHandle::GetRecordHeader(size_t slot_id) const -> const char* {
  NJUDB_ASSERT(slot_id < tab_hdr_->rec_per_page_, "slot_id out of range");
  size_t rec_full_size = tab_hdr_->tuple_header_size_ + tab_hdr_->rec_size_;
  return slots_mem_ + slot_id * rec_full_size;
}

PAXPageHandle::PAXPageHandle(
    const TableHeader *tab_hdr, Page *page, const RecordSchema *schema, const std::vector<size_t> &offsets)
    : PageHandle(tab_hdr, page, page->GetData() + PAGE_HEADER_SIZE,
          page->GetData() + PAGE_HEADER_SIZE + tab_hdr->bitmap_size_),
      schema_(schema),
      offsets_(offsets)
{}

PAXPageHandle::~PAXPageHandle() = default;

// PAX slot memory layout with tuple headers:
// | RecordHeader_1 | RecordHeader_2 | ... | RecordHeader_n |
// | field_1_1 | field_1_2 | ... | field_1_n |
// | field_2_1 | field_2_2 | ... | field_2_n |
// ...
// | field_m_1 | field_m_2 | ... | field_m_n |
void PAXPageHandle::WriteSlot(size_t slot_id, const char *null_map, const char *data, bool update)
{
  NJUDB_STUDENT_TODO(l1, f2);
}

void PAXPageHandle::ReadSlot(size_t slot_id, char *null_map, char *data)
{
  NJUDB_STUDENT_TODO(l1, f2);
}

auto PAXPageHandle::ReadChunk(const RecordSchema *chunk_schema) -> ChunkUptr
{
  std::vector<ArrayValueSptr> col_arrs;
  col_arrs.reserve(chunk_schema->GetFieldCount());
  NJUDB_STUDENT_TODO(l1, f2);
  return std::make_unique<Chunk>(chunk_schema, std::move(col_arrs));
}

auto PAXPageHandle::GetRecordHeader(size_t slot_id) -> char* {
  NJUDB_ASSERT(slot_id < tab_hdr_->rec_per_page_, "slot_id out of range");
  // Headers are at the beginning of slots_mem in PAX layout
  return slots_mem_ + slot_id * tab_hdr_->tuple_header_size_;
}

auto PAXPageHandle::GetRecordHeader(size_t slot_id) const -> const char* {
  NJUDB_ASSERT(slot_id < tab_hdr_->rec_per_page_, "slot_id out of range");
  // Headers are at the beginning of slots_mem in PAX layout
  return slots_mem_ + slot_id * tab_hdr_->tuple_header_size_;
}

}  // namespace njudb
