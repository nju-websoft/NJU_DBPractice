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
#include "replacer/lru_replacer.h"
#include "replacer/lru_k_replacer.h"

#include "../../../common/error.h"

namespace wsdb {

BufferPoolManager::BufferPoolManager(DiskManager *disk_manager, wsdb::LogManager *log_manager, size_t replacer_lru_k)
    : disk_manager_(disk_manager), log_manager_(log_manager)
{
  if (REPLACER == "LRUReplacer") {
    replacer_ = std::make_unique<LRUReplacer>();
  } else if (REPLACER == "LRUKReplacer") {
    replacer_ = std::make_unique<LRUKReplacer>(replacer_lru_k);
  } else {
    WSDB_FETAL(BufferPoolManager, BufferPoolManager, "Unknown replacer: " + REPLACER);
  }
  // TODO: do some initialization
  WSDB_STUDENT_TODO(L1, t2, BufferPoolManager, BufferPoolManager());
}

auto BufferPoolManager::FetchPage(file_id_t fid, page_id_t pid) -> Page *
{
  WSDB_STUDENT_TODO(L1, t2, BufferPoolManager, FetchPage());
}

auto BufferPoolManager::UnpinPage(file_id_t fid, page_id_t pid, bool is_dirty) -> bool
{
  WSDB_STUDENT_TODO(L1, t2, BufferPoolManager, UnpinPage());
}

auto BufferPoolManager::DeletePage(file_id_t fid, page_id_t pid) -> bool
{
  WSDB_STUDENT_TODO(L1, t2, BufferPoolManager, DeletePage());
}

auto BufferPoolManager::DeleteAllPages(file_id_t fid) -> bool
{
  WSDB_STUDENT_TODO(L1, t2, BufferPoolManager, DeleteAllPages());
}

auto BufferPoolManager::FlushPage(file_id_t fid, page_id_t pid) -> bool
{
  WSDB_STUDENT_TODO(L1, t2, BufferPoolManager, FlushPage());
}

auto BufferPoolManager::FlushAllPages(file_id_t fid) -> bool
{
  WSDB_STUDENT_TODO(L1, t2, BufferPoolManager, FlushAllPages());
}

auto BufferPoolManager::GetAvailableFrame() -> frame_id_t
{
  WSDB_STUDENT_TODO(L1, t2, BufferPoolManager, GetAvailableFrame());
}

void BufferPoolManager::UpdateFrame(frame_id_t frame_id, file_id_t fid, page_id_t pid)
{
  WSDB_STUDENT_TODO(L1, t2, BufferPoolManager, UpdateFrame());
}

auto BufferPoolManager::GetFrame(file_id_t fid, page_id_t pid) -> Frame*
{
  const auto it = page_frame_lookup_.find({fid,pid});
  return it == nullptr ? nullptr : &frames_[it->second];
}

}  // namespace wsdb
