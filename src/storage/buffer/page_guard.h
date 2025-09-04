/*------------------------------------------------------------------------------
 - Copyright (c) 2024. Web program is free software: you can redistribute it and/or modify
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

#ifndef NJUDB_PAGE_GUARD_H
#define NJUDB_PAGE_GUARD_H

#include "../../common/page.h"
#include "buffer_pool_manager.h"

namespace njudb {

class BufferPoolManager;

/**
 * @brief Base page guard class that provides RAII-style automatic unpinning
 * 
 * This class ensures that pages are properly unpinned when the guard goes out of scope,
 * preventing buffer pool leaks and making the code exception-safe.
 */
class PageGuard {
public:
  /**
   * @brief Constructor for PageGuard
   * @param buffer_pool_manager Pointer to the buffer pool manager
   * @param page Pointer to the page being guarded
   * @param file_id File ID of the page
   * @param page_id Page ID of the page
   * @param is_dirty Whether the page is dirty
   */
  PageGuard(BufferPoolManager *buffer_pool_manager, Page *page, file_id_t file_id, page_id_t page_id, bool is_dirty = false);

  /**
   * @brief Destructor that automatically unpins the page
   */
  ~PageGuard();

  // Disable copy constructor and assignment operator
  PageGuard(const PageGuard &) = delete;
  PageGuard &operator=(const PageGuard &) = delete;

  /**
   * @brief Move constructor
   */
  PageGuard(PageGuard &&other) noexcept;

  /**
   * @brief Move assignment operator
   */
  PageGuard &operator=(PageGuard &&other) noexcept;

  /**
   * @brief Check if the guard is valid (not moved or dropped)
   * @return True if the guard is valid
   */
  auto IsValid() const -> bool;

  /**
   * @brief Get the page pointer
   * @return Pointer to the page
   */
  auto GetPage() -> Page *;

  /**
   * @brief Get the page data
   * @return Pointer to the page data
   */
  auto GetData() -> char *;

  /**
   * @brief Get the page ID
   * @return Page ID
   */
  auto GetPageId() const -> page_id_t;

  /**
   * @brief Get the file ID
   * @return File ID
   */
  auto GetFileId() const -> file_id_t;

  /**
   * @brief Check if the page is dirty
   * @return True if the page is dirty
   */
  auto IsDirty() const -> bool;

  /**
   * @brief Manually drop the guard (unpin the page)
   * This can be called to release the page before the destructor
   */
  void Drop();

protected:
  BufferPoolManager *buffer_pool_manager_;
  Page *page_;
  file_id_t file_id_;
  page_id_t page_id_;
  bool is_dirty_;
  bool is_valid_;  // Whether the guard is still valid (not moved or dropped)
};

/**
 * @brief Read page guard for read-only access
 * 
 * This guard ensures that the page is unpinned as non-dirty when it goes out of scope.
 */
class ReadPageGuard : public PageGuard {
public:
  /**
   * @brief Constructor for ReadPageGuard
   * @param buffer_pool_manager Pointer to the buffer pool manager
   * @param page Pointer to the page being guarded
   * @param file_id File ID of the page
   * @param page_id Page ID of the page
   */
  ReadPageGuard(BufferPoolManager *buffer_pool_manager, Page *page, file_id_t file_id, page_id_t page_id);

  /**
   * @brief Destructor that unpins the page as non-dirty
   */
  ~ReadPageGuard();

  // Disable copy constructor and assignment operator
  ReadPageGuard(const ReadPageGuard &) = delete;
  ReadPageGuard &operator=(const ReadPageGuard &) = delete;

  /**
   * @brief Move constructor
   */
  ReadPageGuard(ReadPageGuard &&other) noexcept;

  /**
   * @brief Move assignment operator
   */
  ReadPageGuard &operator=(ReadPageGuard &&other) noexcept;

  /**
   * @brief Get const page data for read-only access
   * @return Const pointer to the page data
   */
  auto GetData() const -> const char *;
};

/**
 * @brief Write page guard for read-write access
 * 
 * This guard ensures that the page is unpinned as dirty when it goes out of scope.
 */
class WritePageGuard : public PageGuard {
public:
  /**
   * @brief Constructor for WritePageGuard, by default, is_dirty is set to true
   * @param buffer_pool_manager Pointer to the buffer pool manager
   * @param page Pointer to the page being guarded
   * @param file_id File ID of the page
   * @param page_id Page ID of the page
   */
  WritePageGuard(BufferPoolManager *buffer_pool_manager, Page *page, file_id_t file_id, page_id_t page_id);

  /**
   * @brief Destructor that unpins the page as dirty
   */
  ~WritePageGuard();

  // Disable copy constructor and assignment operator
  WritePageGuard(const WritePageGuard &) = delete;
  WritePageGuard &operator=(const WritePageGuard &) = delete;

  /**
   * @brief Move constructor
   */
  WritePageGuard(WritePageGuard &&other) noexcept;

  /**
   * @brief Move assignment operator
   */
  WritePageGuard &operator=(WritePageGuard &&other) noexcept;

  /**
   * @brief Get mutable page data for read-write access
   * @return Mutable pointer to the page data
   */
  auto GetMutableData() -> char *;

  /**
   * @brief set dirty = false, should carefully use it if the page is sure to be clean
   * 
   */
  void UnsetDirty();
};

}  // namespace njudb

#endif  // NJUDB_PAGE_GUARD_H
