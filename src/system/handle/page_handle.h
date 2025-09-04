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

#ifndef NJUDB_PAGE_HANDLE_H
#define NJUDB_PAGE_HANDLE_H

#include "common/meta.h"
#include "common/page.h"
#include "common/record.h"

namespace njudb {
class PageHandle
{
public:
  PageHandle() = delete;

  PageHandle(const TableHeader *tab_hdr, Page *page, char *bit_map, char *slots_mem);

  /**
   * Write a record to the slot
   * @param slot_id
   * @param null_map
   * @param data
   * @param update indicate whether there is already a record in the slot, if true, it is an update operation
   */
  virtual void WriteSlot(size_t slot_id, const char *null_map, const char *data, bool update);

  virtual void ReadSlot(size_t slot_id, char *null_map, char *data);

  virtual auto ReadChunk(const RecordSchema *chunk_schema) -> ChunkUptr;

  virtual ~PageHandle() = default;

  [[nodiscard]] auto GetPage() -> Page * { return page_; }

  [[nodiscard]] auto GetBitmap() -> char * { return bitmap_; }

protected:
  const TableHeader *tab_hdr_{nullptr};
  Page              *page_{nullptr};
  char              *bitmap_;
  char              *slots_mem_{nullptr};
};

class NAryPageHandle : public PageHandle
{
public:
  NAryPageHandle() = delete;

  NAryPageHandle(const TableHeader *tab_hdr, Page *page);

  void WriteSlot(size_t slot_id, const char *null_map, const char *data, bool update) override;

  void ReadSlot(size_t slot_id, char *null_map, char *data) override;
};

/**
 * test pax
 * create table pax_test (id int, f_1 float, f_2 float, i_1 int, i_2 int, s_1 char(10), s_2 char(14)) storage=pax;
 * insert into pax_test values (1, 1.1, 2.2, 3, 4, 'hello', 'world');
 * insert into pax_test values (2, 2.1, 3.2, 4, 5, 'world', 'hello');
 * insert into pax_test values (3, 3.1, 4.2, 5, 6, 'a', 'b');
 * insert into pax_test values (4, 4.1, 5.2, 6, 7, 'b', 'a');
 * insert into pax_test values (5, , 6.2, 7, , , 'd');
 * insert into pax_test values (, 6.1, , 8, 9, 'c', );
 */

class PAXPageHandle : public PageHandle
{
public:
  PAXPageHandle() = delete;

  PAXPageHandle(const TableHeader *tab_hdr, Page *page, const RecordSchema *schema, const std::vector<size_t> &offsets);

  ~PAXPageHandle() override;

  void WriteSlot(size_t slot_id, const char *null_map, const char *data, bool update) override;

  void ReadSlot(size_t slot_id, char *null_map, char *data) override;

  auto ReadChunk(const RecordSchema *chunk_schema) -> ChunkUptr override;

private:
  const RecordSchema        *schema_;
  const std::vector<size_t> &offsets_;
};

DEFINE_UNIQUE_PTR(PageHandle);
}  // namespace njudb

#endif  // NJUDB_PAGE_HANDLE_H
