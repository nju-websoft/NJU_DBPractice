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

#ifndef NJUDB_TRANSACTION_H
#define NJUDB_TRANSACTION_H

#include <algorithm>
#include <memory>
#include <unordered_set>
#include <vector>
#include "common/rid.h"
#include "common/types.h"
#include "common/record.h"
#include "concurrency/concurrency_protocol.h"

namespace njudb {

// Forward declarations
class UndoRecord;

enum class TxnState
{
  INVALID   = 0,
  GROWING   = 1,
  SHIRNKING = 2,
  COMMITTED = 3,
  ABORTED   = 4
};

/**
 * @brief Write record for rollback (used by both 2PL and MVCC)
 *
 * Tracks modifications for rollback:
 *  - INSERT: old_tuple = nullptr
 *  - UPDATE: old_tuple = previous data
 *  - DELETE: old_tuple = deleted data
 * 
 * ARIES requirement: Store LSN for proper CLR chain construction
 */
struct TableWriteRecord
{
  table_id_t table_id_;
  RID        rid_;
  RecordUptr old_tuple_;
  lsn_t      log_lsn_;  // LSN of the UPDATE log record for this operation

  TableWriteRecord(table_id_t table_id, const RID &rid, RecordUptr old_tuple, lsn_t log_lsn = INVALID_LSN)
      : table_id_(table_id), rid_(rid), old_tuple_(std::move(old_tuple)), log_lsn_(log_lsn)
  {}

  // Move semantics
  TableWriteRecord(TableWriteRecord &&)            = default;
  TableWriteRecord &operator=(TableWriteRecord &&) = default;

  // Delete copy
  TableWriteRecord(const TableWriteRecord &)            = delete;
  TableWriteRecord &operator=(const TableWriteRecord &) = delete;
};

enum class IndexWriteType
{
  INSERT = 0,
  DELETE = 1,
  UPDATE = 2
};

/**
 * @brief Index modification recorded for transaction rollback.
 *
 * Records are appended only after the corresponding index modification
 * succeeds. Abort replays them in reverse order.
 */
struct IndexWriteRecord
{
  IndexWriteType type_;
  idx_id_t       index_id_;
  RecordUptr     old_tuple_;
  RecordUptr     new_tuple_;

  IndexWriteRecord(IndexWriteType type, idx_id_t index_id, RecordUptr old_tuple, RecordUptr new_tuple)
      : type_(type),
        index_id_(index_id),
        old_tuple_(std::move(old_tuple)),
        new_tuple_(std::move(new_tuple))
  {}

  IndexWriteRecord(IndexWriteRecord &&)            = default;
  IndexWriteRecord &operator=(IndexWriteRecord &&) = default;

  IndexWriteRecord(const IndexWriteRecord &)            = delete;
  IndexWriteRecord &operator=(const IndexWriteRecord &) = delete;
};

/**
 * @brief Enhanced transaction supporting both 2PL and MVCC
 *
 * Key design decisions:
 *  - Single Transaction class for both protocols
 *  - Protocol selected at transaction creation time
 *  - Protocol-specific fields (locks for 2PL, versions for MVCC)
 *  - Common write set for rollback
 */
class Transaction
{
public:
  Transaction() = default;
  explicit Transaction(txn_id_t txn_id, IsolationLevel isolation_level = IsolationLevel::REPEATABLE_READ,
      ConcurrencyProtocol protocol = ConcurrencyProtocol::TWO_PHASE_LOCKING)
      : txn_id_(txn_id), isolation_level_(isolation_level), protocol_(protocol)
  {}
  virtual ~Transaction() = default;

  [[nodiscard]] auto GetTxnId() const -> txn_id_t { return txn_id_; }

  [[nodiscard]] auto GetState() const -> TxnState { return state_; }
  void               SetState(TxnState state) { state_ = state; }

  [[nodiscard]] auto IsExplicit() const -> bool { return is_explcit_; }
  void               SetExplicit(bool is_explicit) { is_explcit_ = is_explicit; }

  [[nodiscard]] auto GetIsolationLevel() const -> IsolationLevel { return isolation_level_; }
  [[nodiscard]] auto GetProtocol() const -> ConcurrencyProtocol { return protocol_; }

  // Timestamp management for deadlock prevention and MVCC
  [[nodiscard]] auto GetTimestamp() const -> timestamp_t { return timestamp_; }
  void               SetTimestamp(timestamp_t ts) { timestamp_ = ts; }

  /**
   * @brief Check if this transaction is older than another transaction
   * Used for Wait-Die and Wound-Wait deadlock prevention protocols
   * @param other The other transaction to compare with
   * @return true if this transaction has a smaller (older) timestamp
   */
  [[nodiscard]] auto IsOlderThan(const Transaction *other) const -> bool { return timestamp_ < other->GetTimestamp(); }

  //===--------------------------------------------------------------------===//
  // 2PL-specific: Lock management
  //===--------------------------------------------------------------------===//

  // Table-level locks
  auto GetSharedTableLockSet() -> std::unordered_set<table_id_t> * { return &s_table_lock_set_; }
  auto GetExclusiveTableLockSet() -> std::unordered_set<table_id_t> * { return &x_table_lock_set_; }
  auto GetIntentionSharedTableLockSet() -> std::unordered_set<table_id_t> * { return &is_table_lock_set_; }
  auto GetIntentionExclusiveTableLockSet() -> std::unordered_set<table_id_t> * { return &ix_table_lock_set_; }
  auto GetSharedIntentionExclusiveTableLockSet() -> std::unordered_set<table_id_t> * { return &six_table_lock_set_; }

  // Tuple-level locks
  auto GetSharedTupleLockSet() -> std::unordered_set<RID> * { return &s_tuple_lock_set_; }
  auto GetExclusiveTupleLockSet() -> std::unordered_set<RID> * { return &x_tuple_lock_set_; }

  // Page-level locks (IMPORTANT: Three-level hierarchy Table→Page→Tuple)
  auto GetSharedPageLockSet() -> std::unordered_set<page_id_t> * { return &s_page_lock_set_; }
  auto GetExclusivePageLockSet() -> std::unordered_set<page_id_t> * { return &x_page_lock_set_; }

  /**
   * @brief Check if transaction holds a table lock stronger than IS
   * Useful for child executors to avoid invalid lock upgrades when parent holds IX/X/SIX
   * @param table_id Table ID to check
   * @return true if holding IX, X, or SIX lock on the table
   */
  [[nodiscard]] auto HasStrongerTableLockThanIS(table_id_t table_id) const -> bool
  {
    return ix_table_lock_set_.count(table_id) > 0 ||
           x_table_lock_set_.count(table_id) > 0 ||
           six_table_lock_set_.count(table_id) > 0;
  }

  /**
   * @brief Check if transaction holds any locks
   * Useful for debugging and validation
   */
  [[nodiscard]] auto HasAnyLocks() const -> bool
  {
    return !s_table_lock_set_.empty() || !x_table_lock_set_.empty() || !is_table_lock_set_.empty() ||
           !ix_table_lock_set_.empty() || !six_table_lock_set_.empty() || !s_page_lock_set_.empty() ||
           !x_page_lock_set_.empty() || !s_tuple_lock_set_.empty() || !x_tuple_lock_set_.empty();
  }

  //===--------------------------------------------------------------------===//
  // MVCC-specific: Timestamp and version management
  //===--------------------------------------------------------------------===//

  [[nodiscard]] auto GetReadTimestamp() const -> timestamp_t { return read_ts_; }
  void               SetReadTimestamp(timestamp_t ts) { read_ts_ = ts; }

  [[nodiscard]] auto GetCommitTimestamp() const -> timestamp_t { return commit_ts_; }
  void               SetCommitTimestamp(timestamp_t ts) { commit_ts_ = ts; }

  // Undo records (versions) created by this transaction
  auto GetUndoRecords() -> std::vector<UndoRecord *> & { return undo_records_; }
  void AddUndoRecord(UndoRecord *undo_record) { undo_records_.push_back(undo_record); }
  void RemoveUndoRecord(UndoRecord *undo_record)
  {
    auto it = std::find(undo_records_.begin(), undo_records_.end(), undo_record);
    if (it != undo_records_.end()) {
      undo_records_.erase(it);
    }
  }

  // Read set for MVCC serializability validation
  auto GetReadSet() -> std::unordered_set<RID> * { return &read_set_; }

  //===--------------------------------------------------------------------===//
  // Common: Write set (used by both protocols for rollback)
  //===--------------------------------------------------------------------===//

  auto GetWriteSet() -> std::vector<TableWriteRecord> & { return write_set_; }
  void AppendWriteRecord(TableWriteRecord &&record) { write_set_.emplace_back(std::move(record)); }
  void ClearWriteSet() { write_set_.clear(); }
  void TruncateWriteSet(size_t size)
  {
    NJUDB_ASSERT(size <= write_set_.size(), "Cannot grow write set with truncate");
    write_set_.erase(write_set_.begin() + static_cast<std::ptrdiff_t>(size), write_set_.end());
  }

  auto GetIndexWriteSet() -> std::vector<IndexWriteRecord> & { return index_write_set_; }
  void AppendIndexWriteRecord(IndexWriteRecord &&record) { index_write_set_.emplace_back(std::move(record)); }
  void ClearIndexWriteSet() { index_write_set_.clear(); }
  void TruncateIndexWriteSet(size_t size)
  {
    NJUDB_ASSERT(size <= index_write_set_.size(), "Cannot grow index write set with truncate");
    index_write_set_.erase(index_write_set_.begin() + static_cast<std::ptrdiff_t>(size), index_write_set_.end());
  }

  //===--------------------------------------------------------------------===//
  // Recovery: LSN tracking for Write-Ahead Logging (WAL)
  //===--------------------------------------------------------------------===//

  /**
   * @brief Get previous LSN for this transaction
   * Used to chain log records in recovery
   */
  [[nodiscard]] auto GetPrevLSN() const -> lsn_t { return prev_lsn_; }

  /**
   * @brief Set previous LSN after writing log record
   * @param lsn Log Sequence Number of last log record written by this txn
   */
  void SetPrevLSN(lsn_t lsn) { prev_lsn_ = lsn; }

private:
  //===--------------------------------------------------------------------===//
  // Common fields
  //===--------------------------------------------------------------------===//

  txn_id_t            txn_id_{INVALID_TXN_ID};
  TxnState            state_{TxnState::INVALID};
  IsolationLevel      isolation_level_{IsolationLevel::REPEATABLE_READ};
  ConcurrencyProtocol protocol_{ConcurrencyProtocol::TWO_PHASE_LOCKING};
  timestamp_t         timestamp_{0};       // Transaction timestamp for ordering
  bool                is_explcit_{false};  // whether the transaction is explicit, i.e. started by begin command.

  std::vector<TableWriteRecord> write_set_;  // For rollback
  std::vector<IndexWriteRecord> index_write_set_;  // For index rollback

  //===--------------------------------------------------------------------===//
  // 2PL-specific fields
  //===--------------------------------------------------------------------===//

  // Table-level lock sets
  std::unordered_set<table_id_t> s_table_lock_set_;
  std::unordered_set<table_id_t> x_table_lock_set_;
  std::unordered_set<table_id_t> is_table_lock_set_;
  std::unordered_set<table_id_t> ix_table_lock_set_;
  std::unordered_set<table_id_t> six_table_lock_set_;

  // Page-level lock sets (IMPORTANT: For three-level locking hierarchy)
  std::unordered_set<page_id_t> s_page_lock_set_;
  std::unordered_set<page_id_t> x_page_lock_set_;

  // Tuple-level lock sets
  std::unordered_set<RID> s_tuple_lock_set_;
  std::unordered_set<RID> x_tuple_lock_set_;

  //===--------------------------------------------------------------------===//
  // MVCC-specific fields
  //===--------------------------------------------------------------------===//

  timestamp_t read_ts_{0};    // Snapshot timestamp (INVALID_TS)
  timestamp_t commit_ts_{0};  // Commit timestamp (INVALID_TS)

  std::vector<UndoRecord *> undo_records_;  // Versions created by this txn
  std::unordered_set<RID>   read_set_;      // For validation (SERIALIZABLE)

  //===--------------------------------------------------------------------===//
  // Recovery fields
  //===--------------------------------------------------------------------===//

  lsn_t prev_lsn_{INVALID_LSN};  // Previous LSN for log record chaining
};

}  // namespace njudb

#endif  // NJUDB_TRANSACTION_H
