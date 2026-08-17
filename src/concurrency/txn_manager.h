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

#ifndef NJUDB_TXN_MANAGER_H
#define NJUDB_TXN_MANAGER_H

#include <atomic>
#include <mutex>
#include <unordered_map>
#include <unordered_set>

#include "common/types.h"
#include "concurrency/transaction.h"
#include "log/log_manager.h"

namespace njudb {

// Forward declarations
class LockManager;
class DatabaseHandle;
class TimestampOracle;

/**
 * @brief Manages the lifecycle of all transactions
 *
 * Responsibilities:
 *  - Transaction creation and destruction
 *  - Lock coordination with LockManager
 *  - Write set rollback
 *  - Transaction ID and timestamp generation
 */
class TxnManager
{
public:
  TxnManager() = delete;

  /**
   * @brief Construct transaction manager
   * @param log_manager Log manager for recovery
   * @param lock_manager Lock manager for concurrency control (optional for testing)
   * @param timestamp_oracle Timestamp oracle for MVCC. MVCC requires a non-null oracle.
   */
  explicit TxnManager(
      LogManager *log_manager, LockManager *lock_manager = nullptr, TimestampOracle *timestamp_oracle = nullptr)
      : log_manager_(log_manager),
        lock_manager_(lock_manager),
        timestamp_oracle_(timestamp_oracle)
  {}

  virtual ~TxnManager() = default;

  void SetLockManager(LockManager *lock_manager) { lock_manager_ = lock_manager; }

  //===--------------------------------------------------------------------===//
  // Transaction Lifecycle
  //===--------------------------------------------------------------------===//

  /**
   * @brief Begin a new transaction with specified isolation level and concurrency protocol
   * @param txn_id Transaction identifier
   * @param isolation_level Isolation level (default: REPEATABLE_READ)
   * @param protocol Concurrency control protocol (default: TWO_PHASE_LOCKING)
   *
   * Creates transaction in GROWING state, assigns timestamps
   */
  void Begin(txn_id_t txn_id, IsolationLevel isolation_level = IsolationLevel::REPEATABLE_READ,
      ConcurrencyProtocol protocol = ConcurrencyProtocol::TWO_PHASE_LOCKING);

  /**
   * @brief Commit a transaction
   * @param txn_id Transaction identifier
   *
   * Steps:
   *  1. Validate transaction state (GROWING or SHRINKING)
   *  2. Assign commit timestamp (MVCC)
   *  3. Log COMMIT record
   *  4. Release all locks
   *  5. Update state to COMMITTED
   *  6. Remove from active transactions
   */
  void Commit(txn_id_t txn_id);

  /**
   * @brief Abort a transaction
   * @param txn_id Transaction identifier
   * @param db_handle DatabaseHandle for rollback (required if transaction has modifications)
   *
   * Steps:
   *  1. Validate transaction state (cannot abort committed txn)
   *  2. Rollback all table and index modifications (requires db_handle if write set non-empty)
   *  3. Log ABORT record
   *  4. Release all locks
   *  5. Set state to ABORTED
   *  6. Remove from active transactions
   */
  void Abort(
      txn_id_t txn_id, DatabaseHandle *db_handle =
                           nullptr);  //===--------------------------------------------------------------------===//
  // Transaction Access
  //===--------------------------------------------------------------------===//

  /**
   * @brief Set the current transaction in context (for thread-local storage)
   * @param txn Transaction pointer
   */
  void SetTransaction(Transaction *txn);

  /**
   * @brief Get a transaction by its ID
   * @param txn_id Transaction ID
   * @return Pointer to transaction, or nullptr if not found
   */
  auto GetTransaction(txn_id_t txn_id) -> Transaction *;

  /**
   * @brief Get a stable transaction handle.
   *
   * The returned shared_ptr keeps the transaction alive even if it is removed
   * from the active transaction map by Commit or Abort.
   */
  auto GetTransactionShared(txn_id_t txn_id) -> std::shared_ptr<Transaction>;

  /**
   * @brief Get all active transactions (for checkpoint)
   * @return Vector of pointers to active transactions
   */
  auto GetActiveTransactions() -> std::vector<Transaction *>;

  //===--------------------------------------------------------------------===//
  // ID and Timestamp Generation
  //===--------------------------------------------------------------------===//

  /**
   * @brief Generate next unique transaction ID
   * @return New transaction ID
   */
  auto GetNextTxnId() -> txn_id_t { return next_tid_.fetch_add(1); }

  /**
   * @brief Generate next unique timestamp
   * @return New timestamp
   *
   * Used for:
   *  - Deadlock prevention (Wait-Die, Wound-Wait)
   *  - MVCC snapshot isolation
   */
  auto GetNextTimestamp() -> timestamp_t { return next_ts_.fetch_add(1); }

private:
  auto ClaimTransactionForEnd(txn_id_t txn_id) -> std::shared_ptr<Transaction>;
  void FinishTransaction(txn_id_t txn_id);

  std::atomic<txn_id_t>    next_tid_{0};  // Next transaction ID
  std::atomic<timestamp_t> next_ts_{0};   // Next timestamp for standalone 2PL without TimestampOracle

  std::unordered_map<txn_id_t, std::shared_ptr<Transaction>> tid_to_ts_;  // Active transactions
  std::unordered_set<txn_id_t> ending_txns_;  // Transactions currently being committed or aborted

  LogManager      *log_manager_;       // For recovery (WAL)
  LockManager     *lock_manager_;      // For concurrency control (2PL/MVCC writes)
  TimestampOracle *timestamp_oracle_;  // For MVCC timestamp generation (optional)
  std::mutex latch_;  // Protects tid_to_ts_ map
};
}  // namespace njudb

#endif  // NJUDB_TXN_MANAGER_H
