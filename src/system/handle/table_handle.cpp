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
// Created by ziqi on 2024/7/19.
//

#include "table_handle.h"
#include "concurrency/transaction.h"
#include "concurrency/lock_manager.h"
#include "concurrency/version_manager.h"
#include "concurrency/concurrency_protocol.h"
#include "concurrency/mvcc_write_context.h"
#include "log/log_manager.h"
#include "common/bitmap.h"
#include "../../../common/error.h"
#include <cstring>

namespace njudb {

namespace {

auto FindFreeSlot(PageHandle *pg_hdl, const TableHeader &tab_hdr) -> size_t
{
  auto slot_id =
      BitMap::FindFirst(pg_hdl->GetBitmap(), tab_hdr.rec_per_page_, pg_hdl->GetPage()->GetRecordNum(), false);
  if (slot_id == tab_hdr.rec_per_page_) {
    slot_id = BitMap::FindFirst(pg_hdl->GetBitmap(), tab_hdr.rec_per_page_, 0, false);
  }
  return slot_id;
}

}  // namespace

TableHandle::TableHandle(DiskManager *disk_manager, BufferPoolManager *buffer_pool_manager, table_id_t table_id,
    TableHeader &hdr, RecordSchemaUptr &schema, StorageModel storage_model)
    : tab_hdr_(hdr),
      table_id_(table_id),
      disk_manager_(disk_manager),
      buffer_pool_manager_(buffer_pool_manager),
      schema_(std::move(schema)),
      storage_model_(storage_model)
{
  // set table id for table handle;
  schema_->SetTableId(table_id_);
  if (storage_model_ == PAX_MODEL) {
    NJUDB_STUDENT_TODO(l1, f2);
  }
}

void TableHandle::LogPageModification(Transaction *txn, const RID &rid,
                                      const char *old_data, const char *new_data,
                                      uint32_t length)
{
  if (txn == nullptr || log_manager_ == nullptr) {
    return;
  }

  LogRecord update_record(txn->GetTxnId(), txn->GetPrevLSN(),
                          LogRecordType::UPDATE,
                          table_id_, rid.PageID(), rid.SlotID(), length,
                          old_data, new_data, disk_manager_, txn->GetProtocol());
  const auto lsn = log_manager_->AppendLogRecord(update_record);
  txn->SetPrevLSN(lsn);

  auto *page = buffer_pool_manager_->FetchPage(table_id_, rid.PageID());
  if (page != nullptr) {
    page->SetLsn(lsn);
    buffer_pool_manager_->UnpinPage(table_id_, rid.PageID(), false);
  }
}

auto TableHandle::GetRecord(const RID &rid, Transaction *txn) -> RecordUptr
{
  // No transaction: direct read without concurrency control
  if (txn == nullptr) {
    return ReadDirectFromPage(rid);
  }

  if (txn->GetProtocol() == ConcurrencyProtocol::MVCC) {
    return ReadVersionAware(rid, txn);
  }

  // 2PL protects the page read with an S lock and never consults MVCC versions.
  if (ShouldAcquireReadLock(txn)) {
    // Table-level IS lock should already be held by the executor.
    AcquireTupleLock(txn, LockMode::SHARED, rid);
  }

  RecordUptr record;
  try {
    record = ReadDirectFromPage(rid);
  } catch (...) {
    ReleaseTupleLockImmediately(txn, rid);
    throw;
  }

  // READ_COMMITTED 2PL releases its tuple S lock immediately after the read.
  ReleaseTupleLockImmediately(txn, rid);

  return record;
}

auto TableHandle::ReadVersionAware(const RID &rid, Transaction *txn) -> RecordUptr
{
  // Runtime MVCC visibility is defined exclusively by the undo-version chain.
  // Tuple-header xmin/xmax fields are retained only as recovery/diagnostic
  // metadata and must never be used as a fallback visibility implementation.
  if (version_manager_ == nullptr) {
    NJUDB_THROW(NJUDB_UNSUPPORTED_OP, "MVCC table reads require a VersionManager");
  }

  auto visibility = version_manager_->ResolveVisibility(table_id_, rid, txn->GetReadTimestamp(), txn->GetTxnId());
  switch (visibility.state) {
    case VersionVisibilityState::READ_PAGE:
      return ReadDirectFromPage(rid);
    case VersionVisibilityState::READ_UNDO:
      NJUDB_ASSERT(visibility.undo_tuple != nullptr, "Visible undo result must contain a cloned tuple");
      return std::move(visibility.undo_tuple);
    case VersionVisibilityState::NOT_VISIBLE:
      NJUDB_THROW(NJUDB_RECORD_MISS, fmt::format("Tuple {} is not visible", rid.ToString()));
  }

  NJUDB_FATAL("Unknown version visibility state");
}

auto TableHandle::ReadDirectFromPage(const RID &rid) -> RecordUptr
{
  NJUDB_STUDENT_TODO(l1, t3);
}

//===--------------------------------------------------------------------===//
// Helper Methods for Lock Acquisition (2PL)
//===--------------------------------------------------------------------===//

auto TableHandle::AcquireTableLock(Transaction *txn, LockMode lock_mode) -> bool
{
  if (txn == nullptr || lock_manager_ == nullptr ||
      txn->GetProtocol() != ConcurrencyProtocol::TWO_PHASE_LOCKING) {
    return true;
  }

  if (!lock_manager_->LockTable(txn, lock_mode, table_id_)) {
    NJUDB_THROW(NJUDB_TXN_ABORTED,
        fmt::format("Deadlock detected for transaction {} on table {}", txn->GetTxnId(), table_id_));
  }
  return true;
}

auto TableHandle::AcquireTupleLock(Transaction *txn, LockMode lock_mode, const RID &rid) -> bool
{
  if (txn == nullptr || lock_manager_ == nullptr ||
      txn->GetProtocol() != ConcurrencyProtocol::TWO_PHASE_LOCKING) {
    return true;
  }

  if (!lock_manager_->LockTuple(txn, lock_mode, rid)) {
    NJUDB_THROW(NJUDB_TXN_ABORTED,
        fmt::format("Deadlock detected for transaction {} on tuple {}", txn->GetTxnId(), rid.ToString()));
  }
  return true;
}

auto TableHandle::ShouldAcquireReadLock(Transaction *txn) -> bool
{
  return txn != nullptr && txn->GetProtocol() == ConcurrencyProtocol::TWO_PHASE_LOCKING &&
         txn->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED;
}

void TableHandle::ReleaseTupleLockImmediately(Transaction *txn, const RID &rid)
{
  if (txn != nullptr && lock_manager_ != nullptr &&
      txn->GetProtocol() == ConcurrencyProtocol::TWO_PHASE_LOCKING &&
      txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
    lock_manager_->UnlockTuple(txn, rid);
  }
}

auto TableHandle::GetChunk(page_id_t pid, const RecordSchema *chunk_schema) -> ChunkUptr
{
  NJUDB_STUDENT_TODO(l1, f2);
}

auto TableHandle::InsertRecord(const Record &record, Transaction *txn) -> RID
{
  return InsertRecordBeforePageWrite(record, txn, {});
}

auto TableHandle::InsertRecordBeforePageWrite(
    const Record &record, Transaction *txn, const std::function<void(const RID &)> &before_page_write) -> RID
{
  NJUDB_STUDENT_TODO(l1, t3);
}

void TableHandle::InsertRecord(const RID &rid, const Record &record, Transaction *txn)
{
  if (rid.PageID() == INVALID_PAGE_ID) {
    NJUDB_THROW(NJUDB_PAGE_MISS, fmt::format("Page: {}", rid.PageID()));
  }
  NJUDB_STUDENT_TODO(l1, t3);
}

void TableHandle::DeleteRecord(const RID &rid, Transaction *txn)
{
  NJUDB_STUDENT_TODO(l1, t3);
}

void TableHandle::UpdateRecord(const RID &rid, const Record &record, Transaction *txn)
{
  NJUDB_STUDENT_TODO(l1, t3);
}

auto TableHandle::FetchPageHandle(page_id_t page_id) -> PageHandleUptr
{
  auto page = buffer_pool_manager_->FetchPage(table_id_, page_id);
  return WrapPageHandle(page);
}

auto TableHandle::CreatePageHandle() -> PageHandleUptr
{
  size_t scanned_pages = 0;
  while (tab_hdr_.first_free_page_ != INVALID_PAGE_ID) {
    auto page_id = tab_hdr_.first_free_page_;
    auto page    = buffer_pool_manager_->FetchPage(table_id_, page_id);
    auto pg_hdl  = WrapPageHandle(page);
    if (FindFreeSlot(pg_hdl.get(), tab_hdr_) != tab_hdr_.rec_per_page_) {
      return pg_hdl;
    }

    // The free-list head is stale: it points to a full page. Repair the list
    // and continue instead of failing an otherwise valid insert.
    tab_hdr_.first_free_page_ = page->GetNextFreePageId();
    page->SetNextFreePageId(INVALID_PAGE_ID);
    buffer_pool_manager_->UnpinPage(table_id_, page_id, true);

    scanned_pages++;
    NJUDB_ASSERT(scanned_pages <= tab_hdr_.page_num_, "Free page list contains a cycle");
  }
  return CreateNewPageHandle();
}

auto TableHandle::CreateNewPageHandle() -> PageHandleUptr
{
  auto page_id = static_cast<page_id_t>(tab_hdr_.page_num_);
  tab_hdr_.page_num_++;
  auto page   = buffer_pool_manager_->FetchPage(table_id_, page_id);
  auto pg_hdl = WrapPageHandle(page);
  page->SetNextFreePageId(tab_hdr_.first_free_page_);
  tab_hdr_.first_free_page_ = page_id;
  return pg_hdl;
}

auto TableHandle::WrapPageHandle(Page *page) -> PageHandleUptr
{
  switch (storage_model_) {
    case StorageModel::NARY_MODEL: return std::make_unique<NAryPageHandle>(&tab_hdr_, page);
    case StorageModel::PAX_MODEL: return std::make_unique<PAXPageHandle>(&tab_hdr_, page, schema_.get(), field_offset_);
    default: NJUDB_FATAL("Unknown storage model");
  }
}

auto TableHandle::IsPageInFreeList(page_id_t page_id) -> bool
{
  auto cur = tab_hdr_.first_free_page_;
  size_t scanned_pages = 0;
  while (cur != INVALID_PAGE_ID) {
    if (cur == page_id) {
      return true;
    }

    auto page = buffer_pool_manager_->FetchPage(table_id_, cur);
    auto next = page->GetNextFreePageId();
    buffer_pool_manager_->UnpinPage(table_id_, cur, false);

    cur = next;
    scanned_pages++;
    NJUDB_ASSERT(scanned_pages <= tab_hdr_.page_num_, "Free page list contains a cycle");
  }
  return false;
}

void TableHandle::AddPageToFreeListIfMissing(page_id_t page_id, Page *page)
{
  if (IsPageInFreeList(page_id)) {
    return;
  }

  page->SetNextFreePageId(tab_hdr_.first_free_page_);
  tab_hdr_.first_free_page_ = page_id;
}

void TableHandle::RemovePageFromFreeList(page_id_t page_id, Page *target_page)
{
  auto cur = tab_hdr_.first_free_page_;
  page_id_t prev = INVALID_PAGE_ID;
  size_t scanned_pages = 0;

  while (cur != INVALID_PAGE_ID) {
    Page *cur_page = cur == page_id && target_page != nullptr ? target_page : buffer_pool_manager_->FetchPage(table_id_, cur);
    auto next = cur_page->GetNextFreePageId();

    if (cur == page_id) {
      if (prev == INVALID_PAGE_ID) {
        tab_hdr_.first_free_page_ = next;
      } else {
        auto prev_page = buffer_pool_manager_->FetchPage(table_id_, prev);
        prev_page->SetNextFreePageId(next);
        buffer_pool_manager_->UnpinPage(table_id_, prev, true);
      }
      cur_page->SetNextFreePageId(INVALID_PAGE_ID);
      if (cur_page != target_page) {
        buffer_pool_manager_->UnpinPage(table_id_, cur, true);
      }
      return;
    }

    if (cur_page != target_page) {
      buffer_pool_manager_->UnpinPage(table_id_, cur, false);
    }
    prev = cur;
    cur = next;
    scanned_pages++;
    NJUDB_ASSERT(scanned_pages <= tab_hdr_.page_num_, "Free page list contains a cycle");
  }

  if (target_page != nullptr) {
    target_page->SetNextFreePageId(INVALID_PAGE_ID);
  }
}

auto TableHandle::GetTableId() const -> table_id_t { return table_id_; }

auto TableHandle::GetTableHeader() const -> const TableHeader & { return tab_hdr_; }

auto TableHandle::GetSchema() const -> const RecordSchema & { return *schema_; }

auto TableHandle::GetTableName() const -> std::string
{
  auto file_name = disk_manager_->GetFileName(table_id_);
  return OBJNAME_FROM_FILENAME(file_name);
}

auto TableHandle::GetTableFileName() const -> std::string
{
  return disk_manager_->GetFileName(table_id_);
}

auto TableHandle::GetStorageModel() const -> StorageModel { return storage_model_; }

auto TableHandle::GetFirstRID() -> RID
{
  auto page_id = FILE_HEADER_PAGE_ID + 1;
  while (page_id < static_cast<page_id_t>(tab_hdr_.page_num_)) {
    auto pg_hdl = FetchPageHandle(page_id);
    auto id     = BitMap::FindFirst(pg_hdl->GetBitmap(), tab_hdr_.rec_per_page_, 0, true);
    if (id != tab_hdr_.rec_per_page_) {
      buffer_pool_manager_->UnpinPage(table_id_, page_id, false);
      return {page_id, static_cast<slot_id_t>(id)};
    }
    buffer_pool_manager_->UnpinPage(table_id_, page_id, false);
    page_id++;
  }
  return INVALID_RID;
}

auto TableHandle::GetNextRID(const RID &rid) -> RID
{
  auto page_id = rid.PageID();
  auto slot_id = rid.SlotID();
  while (page_id < static_cast<page_id_t>(tab_hdr_.page_num_)) {
    auto pg_hdl = FetchPageHandle(page_id);
    slot_id = static_cast<slot_id_t>(BitMap::FindFirst(pg_hdl->GetBitmap(), tab_hdr_.rec_per_page_, slot_id + 1, true));
    if (slot_id == static_cast<slot_id_t>(tab_hdr_.rec_per_page_)) {
      buffer_pool_manager_->UnpinPage(table_id_, page_id, false);
      page_id++;
      slot_id = -1;
    } else {
      buffer_pool_manager_->UnpinPage(table_id_, page_id, false);
      return {page_id, static_cast<slot_id_t>(slot_id)};
    }
  }
  return INVALID_RID;
}

auto TableHandle::HasField(const std::string &field_name) const -> bool
{
  return schema_->HasField(table_id_, field_name);
}

//===--------------------------------------------------------------------===//
// Recovery Support (Lab06 Phase 4)
//===--------------------------------------------------------------------===//

auto TableHandle::ApplyRecoveryOperation(page_id_t page_id, uint32_t slot_id,
                                         DataOperationType operation,
                                         const std::vector<char> &image, lsn_t lsn,
                                         bool force_delete_if_visible) -> bool {
  std::lock_guard<std::mutex> lock(record_latch_);
  Page *page = buffer_pool_manager_->FetchPage(table_id_, page_id);
  NJUDB_ASSERT(page != nullptr, "Failed to fetch page during recovery");

  NJUDB_ASSERT(slot_id < tab_hdr_.rec_per_page_, "Recovery slot_id out of range");
  auto pg_handle = WrapPageHandle(page);
  bool exists = BitMap::GetBit(pg_handle->GetBitmap(), slot_id);
  auto expected_length = tab_hdr_.nullmap_size_ + tab_hdr_.rec_size_;

  // MVCC runtime deletes are logical: the slot can remain occupied while the
  // pageLSN already reflects the delete log. Recovery may explicitly request
  // physicalizing such a visible slot. 2PL DELETE and CLR DELETE keep the
  // normal pageLSN idempotence rule to avoid deleting later RID reuse.
  if (page->GetLsn() >= lsn && !(force_delete_if_visible && operation == DataOperationType::DELETE && exists)) {
    buffer_pool_manager_->UnpinPage(table_id_, page_id, false);
    return false;
  }

  if (operation == DataOperationType::INSERT || operation == DataOperationType::UPDATE) {
    NJUDB_ASSERT(image.size() == expected_length, "Recovery image length mismatch");
    NJUDB_ASSERT(operation != DataOperationType::UPDATE || exists,
                 "Cannot redo UPDATE for an empty slot");
    const char *nullmap = image.data();
    const char *data = image.data() + tab_hdr_.nullmap_size_;
    pg_handle->WriteSlot(slot_id, nullmap, data, exists);

    if (!exists) {
      BitMap::SetBit(pg_handle->GetBitmap(), slot_id, true);
      page->SetRecordNum(page->GetRecordNum() + 1);
      if (page->GetRecordNum() == tab_hdr_.rec_per_page_) {
        RemovePageFromFreeList(page_id, page);
      }
    }
  } else if (operation == DataOperationType::DELETE) {
    if (exists) {
      bool was_full = page->GetRecordNum() == tab_hdr_.rec_per_page_;
      BitMap::SetBit(pg_handle->GetBitmap(), slot_id, false);
      page->SetRecordNum(page->GetRecordNum() - 1);
      if (was_full) {
        AddPageToFreeListIfMissing(page_id, page);
      }
    }
  } else {
    buffer_pool_manager_->UnpinPage(table_id_, page_id, false);
    NJUDB_THROW(NJUDB_LOG_INVALID_RECORD, "Invalid recovery operation");
  }

  if (page_id >= static_cast<page_id_t>(tab_hdr_.page_num_)) {
    tab_hdr_.page_num_ = page_id + 1;
  }
  page->SetLsn(lsn);
  buffer_pool_manager_->UnpinPage(table_id_, page_id, true);
  return true;
}

}  // namespace njudb
