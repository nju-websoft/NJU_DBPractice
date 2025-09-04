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

#ifndef NJUDB_BUFFER_POOL_MANAGER_H
#define NJUDB_BUFFER_POOL_MANAGER_H

#include <list>
#include <memory>
#include <mutex>  // NOLINT
#include <vector>
#include <array>
#include "storage/disk/disk_manager.h"
#include "log/log_manager.h"
#include "replacer/replacer.h"
#include "frame.h"
#include "common/page.h"

namespace njudb {

class ReadPageGuard;
class WritePageGuard;
struct fid_pid_t
{
  file_id_t fid;
  page_id_t pid;

  bool operator==(const fid_pid_t &rhs) const { return fid == rhs.fid && pid == rhs.pid; }
};
}  // namespace njudb

namespace std {
template <>
struct hash<njudb::fid_pid_t>
{
  size_t operator()(const njudb::fid_pid_t &fp) const
  {
    return std::hash<table_id_t>()(fp.fid) ^ std::hash<frame_id_t>()(fp.pid);
  }
};
}  // namespace std

namespace njudb {

class BufferPoolManager
{
public:
  explicit BufferPoolManager(DiskManager *disk_manager, LogManager *log_manager = nullptr, size_t replacer_lru_k = 0);

  ~BufferPoolManager() = default;

  DISABLE_COPY_MOVE_AND_ASSIGN(BufferPoolManager)

  /**
   * Fetch the requested page from disk.
   * 1. grant the latch
   * 2. check if the page is in the frame
   * 3. if the page is not in the frame, GetAvailableFrame and UpdateFrame
   * 4. else pin the frame both in the buffer and the replacer and return the page
   * @param fid file that the page belongs to
   * @param pid page id
   * @return the page
   */
  auto FetchPage(file_id_t fid, page_id_t pid) -> Page *;

  /**
   * Unpin the page indicating that it can be victimized
   * 1. grant the latch
   * 2. if the frame is not in the buffer or the frame is not in use, return false
   * 3. unpin the frame, after that if the frame is not in use, unpin the frame in the replacer
   * 4. set the frame dirty if the page is dirty
   * @param fid
   * @param pid
   * @param is_dirty
   * @return true if the page is unpinned successfully
   */
  auto UnpinPage(file_id_t fid, page_id_t pid, bool is_dirty) -> bool;

  /**
   * Delete the page from the buffer pool
   * 1. grant the latch
   * 2. if the page is not in the buffer, return true
   * 3. if the page is in use, return false
   * 4. flush the page to disk, reset the frame, add the frame to the free list and unpin the frame in the replacer
   * 5. update the page_frame_lookup_
   * @param fid
   * @param pid
   * @return true if the page is deleted successfully
   */
  auto DeletePage(file_id_t fid, page_id_t pid) -> bool;

  /**
   * Delete all pages belong to the file
   * @param fid
   * @return true if all pages are deleted successfully
   */
  auto DeleteAllPages(file_id_t fid) -> bool;

  /**
   * Flush the page to disk
   * 1. grant the latch
   * 2. if the page is not in the buffer, return false
   * 3. flush the page to disk if the page is dirty
   * @param fid
   * @param pid
   * @return true if the page is flushed successfully
   */
  auto FlushPage(file_id_t fid, page_id_t pid) -> bool;

  /**
   * Flush all pages to disk
   * @param fid
   * @return
   */
  auto FlushAllPages(file_id_t fid) -> bool;

 /**
   * Get the frame, used for test
   */
 auto GetFrame(file_id_t fid, page_id_t pid) -> Frame*;

  /**
   * Fetch a page and return a ReadPageGuard for read-only access
   * @param fid File ID
   * @param pid Page ID
   * @return ReadPageGuard for the page
   */
  auto FetchPageRead(file_id_t fid, page_id_t pid) -> ReadPageGuard;

  /**
   * Fetch a page and return a WritePageGuard for read-write access
   * @param fid File ID
   * @param pid Page ID
   * @return WritePageGuard for the page
   */
  auto FetchPageWrite(file_id_t fid, page_id_t pid) -> WritePageGuard;

private:
  /// sub procedures used by public APIs, should not be locked by latch

  /**
   * Get the available frame
   * 1. if the free list is not empty, get the frame id from the free list
   * 2. else use the replacer to get the frame id
   * 3. if no frame can be evicted, throw NJUDB_NO_FREE_FRAME
   * @return the frame id
   */
  auto GetAvailableFrame() -> frame_id_t;

  /**
   * Update the frame
   * 1. if the frame is dirty, flush the page to disk
   * 2. update the frame with the new page
   * 3. pin the frame in the buffer and the replacer
   * 4. update the page_frame_lookup_
   * @param frame_id the frame to update
   * @param fid the file needs to be updated to the frame
   * @param pid the page needs to be updated to the frame
   */
  void UpdateFrame(frame_id_t frame_id, file_id_t fid, page_id_t pid);

private:
  std::mutex                                latch_;
  DiskManager                              *disk_manager_;
  LogManager                               *log_manager_;
  std::unique_ptr<Replacer>                 replacer_;
  std::array<Frame, BUFFER_POOL_SIZE>       frames_;
  std::list<frame_id_t>                     free_list_;
  std::unordered_map<fid_pid_t, frame_id_t> page_frame_lookup_;
};

}  // namespace njudb

#endif  // NJUDB_BUFFER_POOL_MANAGER_H
