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

namespace wsdb {
PageHandle::PageHandle(const TableHeader *tab_hdr, Page *page, char *bit_map, char *slots_mem)
    : tab_hdr_(tab_hdr), page_(page), bitmap_(bit_map), slots_mem_(slots_mem)
{
  WSDB_ASSERT(
      PageHandle, PageHandle, BITMAP_SIZE(tab_hdr->rec_per_page_) == tab_hdr->bitmap_size_, "bitmap size not match");
}
void PageHandle::WriteSlot(size_t slot_id, const char *null_map, const char *data, bool update)
{
  throw WSDBException(WSDB_EXCEPTION_EMPTY, Q(PageHandle), Q(WriteSlot));
}

void PageHandle::ReadSlot(size_t slot_id, char *null_map, char *data)
{
  throw WSDBException(WSDB_EXCEPTION_EMPTY, Q(PageHandle), Q(ReadSlot));
}
auto PageHandle::ReadChunk(const RecordSchema *chunk_schema) -> ChunkUptr
{
  throw WSDBException(WSDB_EXCEPTION_EMPTY, Q(PageHandle), Q(ReadChunk));
}

NAryPageHandle::NAryPageHandle(const TableHeader *tab_hdr, Page *page)
    : PageHandle(
          tab_hdr, page, page->GetData() + PAGE_HEADER_SIZE, page->GetData() + PAGE_HEADER_SIZE + tab_hdr->bitmap_size_)
{}

void NAryPageHandle::WriteSlot(size_t slot_id, const char *null_map, const char *data, bool update)
{
  WSDB_ASSERT(NAryPageHandle, WriteSlot, slot_id < tab_hdr_->rec_per_page_, "slot_id out of range");
  WSDB_ASSERT(NAryPageHandle, WriteSlot, BitMap::GetBit(bitmap_, slot_id) == update, fmt::format("update: {}", update));
  // a record consists of null map and data
  size_t rec_full_size = tab_hdr_->nullmap_size_ + tab_hdr_->rec_size_;
  memcpy(slots_mem_ + slot_id * rec_full_size, null_map, tab_hdr_->nullmap_size_);
  memcpy(slots_mem_ + slot_id * rec_full_size + tab_hdr_->nullmap_size_, data, tab_hdr_->rec_size_);
}

void NAryPageHandle::ReadSlot(size_t slot_id, char *null_map, char *data)
{
  WSDB_ASSERT(NAryPageHandle, ReadSlot, slot_id < tab_hdr_->rec_per_page_, "slot_id out of range");
  WSDB_ASSERT(NAryPageHandle, ReadSlot, BitMap::GetBit(bitmap_, slot_id) == true, "slot is empty");
  size_t rec_full_size = tab_hdr_->nullmap_size_ + tab_hdr_->rec_size_;
  memcpy(null_map, slots_mem_ + slot_id * rec_full_size, tab_hdr_->nullmap_size_);
  memcpy(data, slots_mem_ + slot_id * rec_full_size + tab_hdr_->nullmap_size_, tab_hdr_->rec_size_);
}

PAXPageHandle::PAXPageHandle(
    const TableHeader *tab_hdr, Page *page, const RecordSchema *schema, const std::vector<size_t> &offsets)
    : PageHandle(tab_hdr, page, page->GetData() + PAGE_HEADER_SIZE,
          page->GetData() + PAGE_HEADER_SIZE + tab_hdr->bitmap_size_),
      schema_(schema),
      offsets_(offsets)
{}

PAXPageHandle::~PAXPageHandle() = default;

// slot memery
// | nullmap_1, nullmap_2, ... , nullmap_n|
// | field_1_1, field_1_2, ... , field_1_n |
// | field_2_1, field_2_2, ... , field_2_n |
// ...
// | field_m_1, field_m_2, ... , field_m_n |
void PAXPageHandle::WriteSlot(size_t slot_id, const char *null_map, const char *data, bool update)
{
  WSDB_STUDENT_TODO(L1, f2, PAXPageHandle, WriteSlot());
}

void PAXPageHandle::ReadSlot(size_t slot_id, char *null_map, char *data)
{
  WSDB_STUDENT_TODO(L1, f2, PAXPageHandle, ReadSlot());
}

auto PAXPageHandle::ReadChunk(const RecordSchema *chunk_schema) -> ChunkUptr
{
  WSDB_STUDENT_TODO(L1, f2, PAXPageHandle, ReadChunk());
}
}  // namespace wsdb
