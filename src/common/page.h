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
// Created by ziqi on 2024/7/18.
//

#ifndef NJUDB_PAGE_H
#define NJUDB_PAGE_H

#include "../../common/micro.h"
#include "config.h"
#include "types.h"
#include "../../common/error.h"

#define FILE_HEADER_PAGE_ID 0

#define PAGE_LSN_OFFSET 0
#define PAGE_NEXT_FREE_PAGE_ID_OFFSET (PAGE_LSN_OFFSET + sizeof(lsn_t))
#define PAGE_RECORD_NUM_OFFSET (PAGE_NEXT_FREE_PAGE_ID_OFFSET + sizeof(page_id_t))
#define PAGE_HEADER_SIZE (PAGE_RECORD_NUM_OFFSET + sizeof(size_t))

#define PageContentPtr(data) (data + PAGE_HEADER_SIZE)

class Page
{

public:
  Page() = default;
  DISABLE_COPY_MOVE_AND_ASSIGN(Page)

  [[nodiscard]] auto GetFileId() const -> file_id_t { return fid_; }
  [[nodiscard]] auto GetPageId() const -> page_id_t { return pid_; }

  void SetFilePageId(file_id_t fid, page_id_t pid)
  {
    fid_ = fid;
    pid_ = pid;
  }

  auto GetData() -> char * { return data_; }

  auto GetLsn() -> lsn_t
  {
    NJUDB_ASSERT(pid_ != FILE_HEADER_PAGE_ID, "Can't load data from file header page");
    return *reinterpret_cast<lsn_t *>(data_ + PAGE_LSN_OFFSET);
  }

  void SetLsn(lsn_t lsn)
  {
    NJUDB_ASSERT(pid_ != FILE_HEADER_PAGE_ID, "Can't set data from file header page");
    *reinterpret_cast<lsn_t *>(data_ + PAGE_LSN_OFFSET) = lsn;
  }

  auto GetNextFreePageId() -> page_id_t
  {
    NJUDB_ASSERT(pid_ != FILE_HEADER_PAGE_ID, "Can't load data from file header page");
    return *reinterpret_cast<page_id_t *>(data_ + PAGE_NEXT_FREE_PAGE_ID_OFFSET);
  }

  void SetNextFreePageId(page_id_t next_free_page_id)
  {
    NJUDB_ASSERT(pid_ != FILE_HEADER_PAGE_ID, "Can't set data from file header page");
    *reinterpret_cast<page_id_t *>(data_ + PAGE_NEXT_FREE_PAGE_ID_OFFSET) = next_free_page_id;
  }

  auto GetRecordNum() -> size_t
  {
    NJUDB_ASSERT(pid_ != FILE_HEADER_PAGE_ID, "Can't load data from file header page");
    return *reinterpret_cast<size_t *>(data_ + PAGE_RECORD_NUM_OFFSET);
  }

  void SetRecordNum(size_t record_num)
  {
    NJUDB_ASSERT(pid_ != FILE_HEADER_PAGE_ID, "Can't set data from file header page");
    *reinterpret_cast<size_t *>(data_ + PAGE_RECORD_NUM_OFFSET) = record_num;
  }

  void Clear()
  {
    fid_ = INVALID_FILE_ID;
    pid_ = INVALID_PAGE_ID;
    memset(data_, 0, PAGE_SIZE);
  }

private:
  file_id_t fid_{INVALID_FILE_ID};
  page_id_t pid_{INVALID_PAGE_ID};
  char      data_[PAGE_SIZE]{};
};

#endif  // NJUDB_PAGE_H
