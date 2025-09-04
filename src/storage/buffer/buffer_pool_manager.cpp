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
// Created by ziqi on 2024/7/17.
//
#include "buffer_pool_manager.h"
#include "page_guard.h"
#include "replacer/lru_replacer.h"
#include "replacer/lru_k_replacer.h"

#include "../../../common/error.h"

namespace njudb {

BufferPoolManager::BufferPoolManager(DiskManager *disk_manager, njudb::LogManager *log_manager, size_t replacer_lru_k)
    : disk_manager_(disk_manager), log_manager_(log_manager)
{
  if (REPLACER == "LRUReplacer") {
    replacer_ = std::make_unique<LRUReplacer>();
  } else if (REPLACER == "LRUKReplacer") {
    replacer_ = std::make_unique<LRUKReplacer>(replacer_lru_k);
  } else {
    NJUDB_FATAL("Unknown replacer: " + REPLACER);
  }
  // init free_list_
  for (frame_id_t i = 0; i < static_cast<int>(BUFFER_POOL_SIZE); i++) {
    free_list_.push_back(i);
  }
}

auto BufferPoolManager::FetchPage(file_id_t fid, page_id_t pid) -> Page * { NJUDB_STUDENT_TODO(l1, t2); }

auto BufferPoolManager::UnpinPage(file_id_t fid, page_id_t pid, bool is_dirty) -> bool { NJUDB_STUDENT_TODO(l1, t2); }

auto BufferPoolManager::DeletePage(file_id_t fid, page_id_t pid) -> bool { NJUDB_STUDENT_TODO(l1, t2); }

auto BufferPoolManager::DeleteAllPages(file_id_t fid) -> bool { NJUDB_STUDENT_TODO(l1, t2); }

auto BufferPoolManager::FlushPage(file_id_t fid, page_id_t pid) -> bool { NJUDB_STUDENT_TODO(l1, t2); }

auto BufferPoolManager::FlushAllPages(file_id_t fid) -> bool { NJUDB_STUDENT_TODO(l1, t2); }

auto BufferPoolManager::GetAvailableFrame() -> frame_id_t { NJUDB_STUDENT_TODO(l1, t2); }

void BufferPoolManager::UpdateFrame(frame_id_t frame_id, file_id_t fid, page_id_t pid) { NJUDB_STUDENT_TODO(l1, t2); }

auto BufferPoolManager::GetFrame(file_id_t fid, page_id_t pid) -> Frame *
{
  const auto it = page_frame_lookup_.find({fid, pid});
  return it == page_frame_lookup_.end() ? nullptr : &frames_[it->second];
}

auto BufferPoolManager::FetchPageRead(file_id_t fid, page_id_t pid) -> ReadPageGuard
{
  Page *page = FetchPage(fid, pid);
  return {this, page, fid, pid};
}

auto BufferPoolManager::FetchPageWrite(file_id_t fid, page_id_t pid) -> WritePageGuard
{
  Page *page = FetchPage(fid, pid);
  return {this, page, fid, pid};
}

}  // namespace njudb
