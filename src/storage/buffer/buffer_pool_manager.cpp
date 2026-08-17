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
#include "log/recovery.h"  // For FilePageHash

#include "../../../common/error.h"
#include "../../common/config.h"

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
  // init free_set_
  for (frame_id_t i = 0; i < static_cast<int>(BUFFER_POOL_SIZE); i++) {
    free_list_.push_back(i);
  }
}

auto BufferPoolManager::FetchPage(file_id_t fid, page_id_t pid) -> Page *
{
  NJUDB_STUDENT_TODO(l1, t2);
}

auto BufferPoolManager::UnpinPage(file_id_t fid, page_id_t pid, bool is_dirty) -> bool
{
  NJUDB_STUDENT_TODO(l1, t2);
}

auto BufferPoolManager::GetDirtyPages() -> std::unordered_map<std::pair<std::string, page_id_t>, lsn_t, FilePageHash>
{
  std::lock_guard<std::mutex> latch_guard(latch_);
  std::unordered_map<std::pair<std::string, page_id_t>, lsn_t, FilePageHash> dirty_pages;
  
  for (auto &[fid_pid, frame_id] : page_frame_lookup_) {
    auto &frame = frames_[frame_id];
    if (frame.IsDirty() && IsWALManagedPage(fid_pid.fid, fid_pid.pid) &&
        frame.GetRecLSN() != INVALID_LSN) {
      // CRITICAL FIX: Store (filename, page_id) instead of (file_id, page_id)
      // because file IDs are transient and change across system restarts.
      // Filenames are persistent and can be resolved to current file_id during recovery.
      std::string filename = disk_manager_->GetFileName(fid_pid.fid);
      auto dpt_key = std::make_pair(filename, fid_pid.pid);
      dirty_pages[dpt_key] = frame.GetRecLSN();
    }
  }
  
  return dirty_pages;
}

auto BufferPoolManager::DeletePage(file_id_t fid, page_id_t pid) -> bool
{
  NJUDB_STUDENT_TODO(l1, t2);
}

auto BufferPoolManager::DeleteAllPages(file_id_t fid) -> bool
{
  NJUDB_STUDENT_TODO(l1, t2);
}

auto BufferPoolManager::FlushPage(file_id_t fid, page_id_t pid) -> bool
{
  NJUDB_STUDENT_TODO(l1, t2);
}

auto BufferPoolManager::FlushAllPages(file_id_t fid) -> bool
{
  NJUDB_STUDENT_TODO(l1, t2);
}

auto BufferPoolManager::FlushAllPages() -> bool
{
  std::lock_guard<std::mutex> latch_guard(latch_);
  bool all_flushed = true;
  for (frame_id_t frame_id = 0; frame_id < static_cast<frame_id_t>(frames_.size()); ++frame_id) {
    if (!FlushFrameLocked(frame_id)) {
      all_flushed = false;
    }
  }
  return all_flushed;
}

auto BufferPoolManager::GetAvailableFrame() -> frame_id_t
{
  NJUDB_STUDENT_TODO(l1, t2);
}

void BufferPoolManager::UpdateFrame(frame_id_t frame_id, file_id_t fid, page_id_t pid)
{
  NJUDB_STUDENT_TODO(l1, t2);
}

auto BufferPoolManager::FlushFrameLocked(frame_id_t frame_id) -> bool
{
  auto &frame = frames_[frame_id];
  if (!frame.IsDirty()) {
    return true;
  }
  if (frame.InUse()) {
    return false;
  }

  auto *page = frame.GetPage();
  if (log_manager_ != nullptr && IsWALManagedPage(page->GetFileId(), page->GetPageId())) {
    const auto page_lsn = page->GetLsn();
    if (page_lsn != INVALID_LSN) {
      log_manager_->FlushLogUpTo(page_lsn);
    }
  }
  disk_manager_->WritePage(page->GetFileId(), page->GetPageId(), page->GetData());
  frame.SetDirty(false);
  return true;
}

auto BufferPoolManager::IsWALManagedPage(file_id_t fid, page_id_t pid) const -> bool
{
  if (pid == FILE_HEADER_PAGE_ID) {
    return false;
  }
  const auto filename = disk_manager_->GetFileName(fid);
  return filename.size() >= TAB_SUFFIX.size() &&
         filename.compare(filename.size() - TAB_SUFFIX.size(), TAB_SUFFIX.size(), TAB_SUFFIX) == 0;
}

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
