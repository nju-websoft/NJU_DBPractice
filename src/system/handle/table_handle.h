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

#ifndef NJUDB_TABLE_HANDLE_H
#define NJUDB_TABLE_HANDLE_H
#include <functional>
#include <mutex>
#include <utility>

#include "../../../common/micro.h"
#include "common/page.h"
#include "storage/storage.h"
#include "page_handle.h"
#include "concurrency/lock_manager.h"
#include "log/log_record.h"

namespace njudb {

// Forward declarations for concurrency control
class Transaction;
class VersionManager;
class LogManager;

/**
 * Table descriptor in memory, including the column schema of the table
 */
class TableHandle
{
public:
  TableHandle() = delete;

  TableHandle(DiskManager *disk_manager, BufferPoolManager *buffer_pool_manager, table_id_t table_id, TableHeader &hdr,
      RecordSchemaUptr &schema, StorageModel storage_model);

  /**
   * Get a record by rid
   * 1. fetch the page handle by rid
   * 2. check if there is a record in the slot, if not, unpin the page and throw NJUDB_RECORD_MISS
   * 3. read the record from the slot
   * 4. unpin the page
   * @param rid
   * @param txn Transaction context for concurrency control (nullptr for no transaction)
   * @return record
   */
  auto GetRecord(const RID &rid, Transaction *txn = nullptr) -> RecordUptr;

  /**
   * Get a chunk in page using record schema indicating which columns should be loaded
   * @param pid
   * @param chunk_schema
   * @return
   */
  auto GetChunk(page_id_t pid, const RecordSchema *chunk_schema) -> ChunkUptr;

  /**
   * Insert a record into the table
   * 1. create a page handle using CreatePageHandle
   * 2. get an empty slot in the page
   * 3. write the record into the slot
   * 4. update the bitmap and the number of records in the page header
   * 5. if the page is full after inserting the record, update the first free page id in the file header and set the
   * next page id of the current page
   * 6. unpin the page
   * @param record
   * @param txn Transaction context for concurrency control (nullptr for no transaction)
   * @return rid of the inserted record
   */
  auto InsertRecord(const Record &record, Transaction *txn = nullptr) -> RID;

  /**
   * Insert while invoking a coordinator callback after RID allocation but
   * before the tuple becomes visible on the page.
   */
  auto InsertRecordBeforePageWrite(const Record &record, Transaction *txn,
      const std::function<void(const RID &)> &before_page_write) -> RID;

  /**
   * Insert a record into the table given rid
   * 1. if rid is invalid, unpin the page and throw NJUDB_PAGE_MISS
   * 2. fetch the page handle and check the bitmap, if the slot is not empty, throw NJUDB_RECORD_EXISTS
   * 3. do the rest of the steps in InsertRecord 3-6
   * @param rid
   * @param record
   * @param txn Transaction context for concurrency control (nullptr for no transaction)
   */
  void InsertRecord(const RID &rid, const Record &record, Transaction *txn = nullptr);

  /**
   * Delete the record by rid
   * 1. if the slot is empty, unpin the page and throw NJUDB_RECORD_MISS
   * 2. update the bitmap and the number of records in the page header
   * 3. if the page is not full after deleting the record, update the first free page id in the file header and the next
   * page id in the page header
   * 4. unpin the page
   * @param rid
   * @param txn Transaction context for concurrency control (nullptr for no transaction)
   */
  void DeleteRecord(const RID &rid, Transaction *txn = nullptr);

  /**
   * Update the record by rid
   * 1. if the slot is empty, unpin the page and throw NJUDB_RECORD_MISS
   * 2. write slot
   * 3. unpin the page
   * @param rid
   * @param record
   * @param txn Transaction context for concurrency control (nullptr for no transaction)
   */
  void UpdateRecord(const RID &rid, const Record &record, Transaction *txn = nullptr);

  [[nodiscard]] auto GetTableId() const -> table_id_t;

  [[nodiscard]] auto GetTableHeader() const -> const TableHeader &;

  [[nodiscard]] auto GetSchema() const -> const RecordSchema &;

  [[nodiscard]] auto GetTableName() const -> std::string;

  [[nodiscard]] auto GetTableFileName() const -> std::string;

  [[nodiscard]] auto GetStorageModel() const -> StorageModel;

  [[nodiscard]] auto GetFirstRID() -> RID;

  [[nodiscard]] auto GetNextRID(const RID &rid) -> RID;

  [[nodiscard]] auto HasField(const std::string &field_name) const -> bool;

  /**
   * @brief Set lock manager for this table handle
   * @param lock_manager Lock manager instance
   */
  void SetLockManager(LockManager *lock_manager) {
    lock_manager_ = lock_manager;
  }
  
  /**
   * @brief Set version manager for MVCC support
   * @param version_manager Version manager instance
   */
  void SetVersionManager(VersionManager *version_manager) {
    version_manager_ = version_manager;
  }
  
  /**
   * @brief Set log manager for WAL support
   * @param log_manager Log manager instance
   */
  void SetLogManager(LogManager *log_manager) {
    log_manager_ = log_manager;
  }
  
  /**
   * @brief Get log manager (for rollback CLR generation)
   * @return Log manager instance
   */
  auto GetLogManager() const -> LogManager* {
    return log_manager_;
  }
  
  //===--------------------------------------------------------------------===//
  // Recovery Support (Lab06 Phase 4)
  //===--------------------------------------------------------------------===//
  
  /**
   * Apply one UPDATE/CLR operation atomically on a single fetched page.
   * The operation updates tuple contents, bitmap, record count, free-list
   * metadata, and pageLSN before releasing the page.
   */
  auto ApplyRecoveryOperation(page_id_t page_id, uint32_t slot_id, DataOperationType operation,
                              const std::vector<char> &image, lsn_t lsn,
                              bool force_delete_if_visible = false) -> bool;

private:
  /**
   * @brief Read record directly from page (no locking, no version check)
   * @param rid Record identifier
   * @return Record pointer
   * 
   * Internal helper method used by protocol-specific implementations
   */
  auto ReadDirectFromPage(const RID &rid) -> RecordUptr;

  /**
   * @brief Read the version visible to a transaction, falling back to the page
   * @param rid Record identifier
   * @param txn Transaction whose read timestamp determines visibility
   * @return Visible record
   *
   * This helper is only used by MVCC transactions. A database run uses one
   * concurrency protocol, so 2PL transactions never inspect MVCC versions.
   */
  auto ReadVersionAware(const RID &rid, Transaction *txn) -> RecordUptr;
  
  //===--------------------------------------------------------------------===//
  // WAL Integration (Protocol-Agnostic)
  //===--------------------------------------------------------------------===//
  
  /**
   * @brief Log page modification for WAL (protocol-agnostic)
   * @param txn Transaction performing the modification
   * @param rid Record identifier being modified
   * @param old_data Previous data (nullptr for INSERT)
   * @param new_data New data (nullptr for DELETE)
   * @param length Data length
   * 
   * This method doesn't check txn->GetProtocol() because logging is identical!
   * Both 2PL and MVCC log physical page changes the same way.
   */
  void LogPageModification(Transaction *txn, const RID &rid,
                          const char *old_data, const char *new_data,
                          uint32_t length);
  
  //===--------------------------------------------------------------------===//
  // Helper Methods for Lock Acquisition (2PL)
  //===--------------------------------------------------------------------===//
  
  /**
   * @brief Acquire table-level lock for 2PL
   * @param txn Transaction requesting lock
   * @param lock_mode Lock mode to acquire
   * @return true if successful, false if deadlock
   * @throws std::exception if deadlock detected
   */
  auto AcquireTableLock(Transaction *txn, LockMode lock_mode) -> bool;
  
  /**
   * @brief Acquire tuple-level lock for 2PL
   * @param txn Transaction requesting lock
   * @param lock_mode Lock mode to acquire
   * @param rid Record identifier
   * @return true if successful, false if deadlock
   * @throws std::exception if deadlock detected
   */
  auto AcquireTupleLock(Transaction *txn, LockMode lock_mode, const RID &rid) -> bool;
  
  /**
   * @brief Check if transaction should acquire read locks based on isolation level
   * @param txn Transaction context
   * @return true if should acquire S locks for reads
   */
  auto ShouldAcquireReadLock(Transaction *txn) -> bool;
  
  /**
   * @brief Release tuple lock immediately (for READ_COMMITTED)
   * @param txn Transaction context
   * @param rid Record identifier
   */
  void ReleaseTupleLockImmediately(Transaction *txn, const RID &rid);
  
  /**
   * Fetch the page handle by page id
   * @param page_id
   * @return
   */
  auto FetchPageHandle(page_id_t page_id) -> PageHandleUptr;

  /**
   * Create a page handle that has at least one empty slot
   * @return
   */
  auto CreatePageHandle() -> PageHandleUptr;

  /**
   * Create a fresh new page handle
   * @return
   */
  auto CreateNewPageHandle() -> PageHandleUptr;

  /**
   * Wrap the page handle according to the storage model
   * @param page
   * @return
   */
  auto WrapPageHandle(Page *page) -> PageHandleUptr;

  auto IsPageInFreeList(page_id_t page_id) -> bool;

  void AddPageToFreeListIfMissing(page_id_t page_id, Page *page);

  void RemovePageFromFreeList(page_id_t page_id, Page *target_page);

private:
  TableHeader tab_hdr_;
  table_id_t  table_id_;

  DiskManager       *disk_manager_;
  BufferPoolManager *buffer_pool_manager_;

  RecordSchemaUptr schema_;
  StorageModel     storage_model_;

  /// field below is available when storage model is pax
  /// field offsets is the offset of each field stored in page
  // pax model is stored like below, field_offset can be calculated by Record Schema
  // | page header | bitmap |
  // slot memery begins
  // | nullmap_1, nullmap_2, ... , nullmap_n|
  // | field_1_1, field_1_2, ... , field_1_n |
  // | field_2_1, field_2_2, ... , field_2_n |
  // ...
  // | field_m_1, field_m_2, ... , field_m_n |
  std::vector<size_t> field_offset_;

  std::mutex record_latch_;

  LockManager *lock_manager_{nullptr};     // Lock manager for concurrency control
  VersionManager *version_manager_{nullptr}; // Version manager for MVCC
  LogManager *log_manager_{nullptr};       // Log manager for WAL
};

DEFINE_UNIQUE_PTR(TableHandle);

}  // namespace njudb

#endif  // NJUDB_TABLE_HANDLE_H
