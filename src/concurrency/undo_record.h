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
// Created for Lab05 - MVCC Implementation
//

#ifndef NJUDB_UNDO_RECORD_H
#define NJUDB_UNDO_RECORD_H

#include <memory>
#include "common/types.h"
#include "common/rid.h"
#include "common/record.h"

namespace njudb {

// Special timestamp values
constexpr timestamp_t INVALID_TS = 0;
constexpr timestamp_t MAX_TS = UINT64_MAX;
constexpr timestamp_t ABORTED_TS = MAX_TS - 1;

// Forward declaration
class Record;
using RecordUptr = std::unique_ptr<Record>;

/**
 * @brief Undo record representing a version of a tuple in MVCC
 * 
 * Structure:
 *   - Stores old tuple data before an update
 *   - Links to previous version (older)
 *   - Contains transaction and timestamp information
 * 
 * Version chain organization (newest to oldest):
 *   Current tuple (on page) ← [Version 1] ← [Version 2] ← ... ← nullptr
 */
class UndoRecord {
public:
  /**
   * @brief Construct an undo record
   * @param txn_id Transaction that created this version
   * @param rid Record identifier
   * @param old_tuple Previous tuple data
   * @param ts_begin Begin timestamp (transaction start time)
   */
  UndoRecord(txn_id_t txn_id, const RID &rid, RecordUptr old_tuple, timestamp_t ts_begin);
  
  ~UndoRecord() = default;
  
  //===--------------------------------------------------------------------===//
  // Accessors
  //===--------------------------------------------------------------------===//
  
  auto GetTxnId() const -> txn_id_t { return txn_id_; }
  
  auto GetRID() const -> RID { return rid_; }

  
  // Alias for GetTupleData() for convenience
  auto GetTuple() const -> const Record* { return tuple_data_.get(); }
  
  auto GetBeginTimestamp() const -> timestamp_t { return ts_begin_; }
  
  auto GetCommitTimestamp() const -> timestamp_t { return ts_commit_; }
  
  auto GetPrev() const -> UndoRecord* { return prev_; }
  
  void SetPrev(UndoRecord *prev) { prev_ = prev; }
  
  auto IsDeleted() const -> bool { return is_deleted_; }
  
  void SetDeleted(bool deleted) { is_deleted_ = deleted; }

  auto GetRecoveryLSN() const -> lsn_t { return recovery_lsn_; }

  void SetRecoveryLSN(lsn_t lsn) { recovery_lsn_ = lsn; }
  
  //===--------------------------------------------------------------------===//
  // Version lifecycle management
  //===--------------------------------------------------------------------===//
  
  /**
   * @brief Mark this version as committed
   * @param commit_ts Commit timestamp
   */
  void Commit(timestamp_t commit_ts);
  
  /**
   * @brief Mark this version as aborted
   */
  void Abort();
  
private:
  /** Transaction that created this version */
  txn_id_t txn_id_;
  
  /** Record identifier */
  RID rid_;
  
  /** Previous tuple data (nullptr for INSERT) */
  RecordUptr tuple_data_;
  
  /** Begin timestamp (when version was created) */
  timestamp_t ts_begin_;
  
  /** Commit timestamp (MAX_TS if uncommitted, ABORTED_TS if aborted) */
  timestamp_t ts_commit_{MAX_TS};
  
  /** Pointer to previous (older) version */
  UndoRecord *prev_{nullptr};
  
  /** True if this is a delete marker */
  bool is_deleted_{false};

  /** Physical UPDATE log LSN that created this version, if recovery needs it. */
  lsn_t recovery_lsn_{INVALID_LSN};
};

}  // namespace njudb

#endif  // NJUDB_UNDO_RECORD_H
