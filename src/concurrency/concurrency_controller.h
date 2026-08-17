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
// Created for Lab05 - Dual Protocol Architecture
//

#ifndef NJUDB_CONCURRENCY_CONTROLLER_H
#define NJUDB_CONCURRENCY_CONTROLLER_H

#include "common/rid.h"
#include "common/types.h"
#include "concurrency/lock_manager.h"
#include "concurrency/transaction.h"
#include "concurrency/version_manager.h"
#include "common/record.h"

namespace njudb {

// Forward declarations
class TableHandle;
class LockManager;
class VersionManager;
class TimestampOracle;

/**
 * @brief Unified concurrency controller facade
 * 
 * Routes operations to appropriate protocol implementation:
 *  - 2PL: Lock acquisition/release through LockManager
 *  - MVCC: Version creation/visibility through VersionManager
 * 
 * Key design decisions:
 *  - Single unified API for both protocols
 *  - Protocol determined by Transaction::GetProtocol()
 *  - Both protocols use X locks for writes (lost update prevention)
 *  - Version chains are NOT logged (crash recovery follows lock-based semantics)
 */
class ConcurrencyController {
public:
  ConcurrencyController(LockManager *lock_manager,
                        VersionManager *version_manager,
                        TimestampOracle *timestamp_oracle)
    : lock_manager_(lock_manager),
      version_manager_(version_manager),
      timestamp_oracle_(timestamp_oracle) {}
  
  ~ConcurrencyController() = default;
  
  //===--------------------------------------------------------------------===//
  // Read operations
  //===--------------------------------------------------------------------===//
  
  /**
   * @brief Acquire read access to a tuple
   * 
   * 2PL: Acquire S lock
   * MVCC: Find visible version based on read timestamp
   * 
   * @param txn Transaction requesting read
   * @param table_id Table identifier
   * @param rid Record identifier
   * @param[out] record Output parameter for tuple data
   * @return true if read successful, false on abort
   */
  auto ReadTuple(Transaction *txn, table_id_t table_id, const RID &rid, 
                 Record *record) -> bool;
  
  /**
   * @brief Acquire read access to entire table (scan)
   * 
   * 2PL: Acquire IS or S lock depending on isolation level
   * MVCC: Set read timestamp for snapshot isolation
   * 
   * @param txn Transaction requesting scan
   * @param table_id Table identifier
   * @return true if scan can proceed, false on abort
   */
  auto ScanTable(Transaction *txn, table_id_t table_id) -> bool;
  
  //===--------------------------------------------------------------------===//
  // Write operations (both protocols use X locks)
  //===--------------------------------------------------------------------===//
  
  /**
   * @brief Acquire write access to a tuple (UPDATE)
   * 
   * 2PL: Acquire X lock
   * MVCC: 
   *  1. Acquire X lock (prevent concurrent updates)
   *  2. Create new version with UNCOMMITTED state
   *  3. Add to transaction's undo_records
   * 
   * @param txn Transaction requesting update
   * @param table_id Table identifier
   * @param rid Record identifier
   * @param old_tuple Previous tuple data (for rollback)
   * @return true if update can proceed, false on abort
   */
  auto UpdateTuple(Transaction *txn, table_id_t table_id, const RID &rid,
                   RecordUptr old_tuple) -> bool;
  
  /**
   * @brief Acquire write access for new tuple (INSERT)
   * 
   * 2PL: Acquire X lock on new RID
   * MVCC: 
   *  1. Acquire X lock
   *  2. Create first version with UNCOMMITTED state
   * 
   * @param txn Transaction inserting tuple
   * @param table_id Table identifier
   * @param rid Record identifier (newly allocated)
   * @return true if insert can proceed, false on abort
   */
  auto InsertTuple(Transaction *txn, table_id_t table_id, const RID &rid) -> bool;
  
  /**
   * @brief Acquire write access for tuple deletion
   * 
   * 2PL: Acquire X lock
   * MVCC:
   *  1. Acquire X lock
   *  2. Create delete marker version with UNCOMMITTED state
   * 
   * @param txn Transaction deleting tuple
   * @param table_id Table identifier
   * @param rid Record identifier
   * @param old_tuple Deleted tuple data (for rollback)
   * @return true if delete can proceed, false on abort
   */
  auto DeleteTuple(Transaction *txn, table_id_t table_id, const RID &rid,
                   RecordUptr old_tuple) -> bool;
  
  //===--------------------------------------------------------------------===//
  // Transaction lifecycle
  //===--------------------------------------------------------------------===//
  
  /**
   * @brief Prepare transaction for commit
   * 
   * 2PL: Transition to SHRINKING state
   * MVCC: 
   *  1. Validate read set (SERIALIZABLE only)
   *  2. Acquire commit timestamp
   *  3. Mark all versions as COMMITTED with commit_ts
   * 
   * @param txn Transaction to commit
   * @return true if commit succeeds, false if validation fails
   */
  auto CommitTransaction(Transaction *txn) -> bool;
  
  /**
   * @brief Abort transaction and rollback changes
   * 
   * Both protocols:
   *  1. Rollback write set (restore old tuples)
   *  2. Release all locks
   * MVCC-specific:
   *  3. Mark all versions as ABORTED
   * 
   * @param txn Transaction to abort
   */
  auto AbortTransaction(Transaction *txn) -> void;
  
  //===--------------------------------------------------------------------===//
  // Lock management (used by both protocols for writes)
  //===--------------------------------------------------------------------===//
  
  auto GetLockManager() -> LockManager* { return lock_manager_; }
  
  /**
   * @brief Lock table with specified mode
   * 
   * Used by both protocols:
   *  - 2PL: All read/write operations
   *  - MVCC: Write operations only
   * 
   * @param txn Transaction requesting lock
   * @param lock_mode Lock mode (S, X, IS, IX, SIX)
   * @param table_id Table identifier
   * @return true if lock acquired, false on deadlock/abort
   */
  auto LockTable(Transaction *txn, LockMode lock_mode, table_id_t table_id) -> bool;
  
  /**
   * @brief Unlock table
   * 
   * @param txn Transaction releasing lock
   * @param table_id Table identifier
   * @return true if unlock successful
   */
  auto UnlockTable(Transaction *txn, table_id_t table_id) -> bool;
  
  /**
   * @brief Lock tuple with specified mode
   * 
   * @param txn Transaction requesting lock
   * @param lock_mode Lock mode (S or X)
   * @param table_id Table identifier
   * @param rid Record identifier
   * @return true if lock acquired, false on deadlock/abort
   */
  auto LockTuple(Transaction *txn, LockMode lock_mode, 
                 table_id_t table_id, const RID &rid) -> bool;
  
  /**
   * @brief Unlock tuple
   * 
   * @param txn Transaction releasing lock
   * @param table_id Table identifier
   * @param rid Record identifier
   * @return true if unlock successful
   */
  auto UnlockTuple(Transaction *txn, table_id_t table_id, const RID &rid) -> bool;

private:
  //===--------------------------------------------------------------------===//
  // Protocol-specific routing helpers
  //===--------------------------------------------------------------------===//
  
  /**
   * @brief Route read operation to appropriate protocol
   */
  auto ReadTuple2PL(Transaction *txn, table_id_t table_id, const RID &rid,
                    Record *record) -> bool;
  
  auto ReadTupleMVCC(Transaction *txn, table_id_t table_id, const RID &rid,
                     Record *record) -> bool;
  
  /**
   * @brief Route write operation to appropriate protocol
   */
  auto UpdateTuple2PL(Transaction *txn, table_id_t table_id, const RID &rid) -> bool;
  
  auto UpdateTupleMVCC(Transaction *txn, table_id_t table_id, const RID &rid,
                       RecordUptr old_tuple) -> bool;
  
  /**
   * @brief Route commit to appropriate protocol
   */
  auto CommitTransaction2PL(Transaction *txn) -> bool;
  
  auto CommitTransactionMVCC(Transaction *txn) -> bool;
  
  /**
   * @brief Validate MVCC read set for serializability
   * 
   * Check if any read tuple has been updated by concurrent committed transactions
   */
  auto ValidateReadSet(Transaction *txn) -> bool;
  
  LockManager *lock_manager_;
  VersionManager *version_manager_;
  TimestampOracle *timestamp_oracle_;
};

}  // namespace njudb

#endif  // NJUDB_CONCURRENCY_CONTROLLER_H
