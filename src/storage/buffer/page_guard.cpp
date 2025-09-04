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

#include "page_guard.h"
#include "buffer_pool_manager.h"

namespace njudb {

// PageGuard implementation
PageGuard::PageGuard(
    BufferPoolManager *buffer_pool_manager, Page *page, file_id_t file_id, page_id_t page_id, bool is_dirty)
    : buffer_pool_manager_(buffer_pool_manager),
      page_(page),
      file_id_(file_id),
      page_id_(page_id),
      is_dirty_(is_dirty),
      is_valid_(true)
{}

PageGuard::~PageGuard()
{
  if (is_valid_ && buffer_pool_manager_ != nullptr && page_ != nullptr) {
    buffer_pool_manager_->UnpinPage(file_id_, page_id_, is_dirty_);
  }
}

PageGuard::PageGuard(PageGuard &&other) noexcept
    : buffer_pool_manager_(other.buffer_pool_manager_),
      page_(other.page_),
      file_id_(other.file_id_),
      page_id_(other.page_id_),
      is_dirty_(other.is_dirty_),
      is_valid_(other.is_valid_)
{
  other.is_valid_ = false;
}

PageGuard &PageGuard::operator=(PageGuard &&other) noexcept
{
  if (this != &other) {
    // Drop current guard
    Drop();

    // Move from other
    buffer_pool_manager_ = other.buffer_pool_manager_;
    page_                = other.page_;
    file_id_             = other.file_id_;
    page_id_             = other.page_id_;
    is_dirty_            = other.is_dirty_;
    is_valid_            = other.is_valid_;

    other.is_valid_ = false;
  }
  return *this;
}

auto PageGuard::IsValid() const -> bool { return is_valid_; }

auto PageGuard::GetPage() -> Page * { return page_; }

auto PageGuard::GetData() -> char * { return is_valid_ && page_ != nullptr ? page_->GetData() : nullptr; }

auto PageGuard::GetPageId() const -> page_id_t { return page_id_; }

auto PageGuard::GetFileId() const -> file_id_t { return file_id_; }

auto PageGuard::IsDirty() const -> bool { return is_dirty_; }

void PageGuard::Drop()
{
  if (is_valid_ && buffer_pool_manager_ != nullptr && page_ != nullptr) {
    buffer_pool_manager_->UnpinPage(file_id_, page_id_, is_dirty_);
    is_valid_ = false;
  }
}

// ReadPageGuard implementation
ReadPageGuard::ReadPageGuard(BufferPoolManager *buffer_pool_manager, Page *page, file_id_t file_id, page_id_t page_id)
    : PageGuard(buffer_pool_manager, page, file_id, page_id, false)
{}

ReadPageGuard::~ReadPageGuard() = default;

ReadPageGuard::ReadPageGuard(ReadPageGuard &&other) noexcept : PageGuard(std::move(other)) {}

ReadPageGuard &ReadPageGuard::operator=(ReadPageGuard &&other) noexcept
{
  PageGuard::operator=(std::move(other));
  return *this;
}

auto ReadPageGuard::GetData() const -> const char *
{
  return is_valid_ && page_ != nullptr ? page_->GetData() : nullptr;
}

// WritePageGuard implementation
WritePageGuard::WritePageGuard(BufferPoolManager *buffer_pool_manager, Page *page, file_id_t file_id, page_id_t page_id)
    : PageGuard(buffer_pool_manager, page, file_id, page_id, false)
{}

WritePageGuard::~WritePageGuard() = default;

WritePageGuard::WritePageGuard(WritePageGuard &&other) noexcept : PageGuard(std::move(other)) {}

WritePageGuard &WritePageGuard::operator=(WritePageGuard &&other) noexcept
{
  PageGuard::operator=(std::move(other));
  return *this;
}

auto WritePageGuard::GetMutableData() -> char *
{
  is_dirty_ = true;
  return is_valid_ && page_ != nullptr ? page_->GetData() : nullptr;
}

void WritePageGuard::UnsetDirty() { is_dirty_ = false; }
}  // namespace njudb
