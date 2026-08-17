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
// Lab06 Phase 4: ARIES Recovery (Dual-Protocol Edition)
//

#ifndef NJUDB_RECOVERY_H
#define NJUDB_RECOVERY_H

#include <unordered_map>
#include <memory>
#include "storage/storage.h"
#include "system/handle/database_handle.h"
#include "log/log_manager.h"
#include "log/log_record.h"
#include "concurrency/transaction.h"
#include "concurrency/concurrency_protocol.h"

namespace njudb {

// Forward declarations
class TxnManager;

/**
 * @brief Entry in Active Transaction Table
 * 
 * Tracks transaction state during recovery, including protocol information
 * for dual-protocol (2PL + MVCC) support.
 */
struct ActiveTxnEntry {
  txn_id_t txn_id_;
  lsn_t last_lsn_;              // Last log record for this transaction
  lsn_t undo_next_lsn_;         // Next LSN to undo (for CLR optimization)
  ConcurrencyProtocol protocol_; // CRITICAL: Track protocol for undo phase
  TxnState state_;
  
  ActiveTxnEntry(txn_id_t txn_id, lsn_t last_lsn, 
                ConcurrencyProtocol protocol = ConcurrencyProtocol::TWO_PHASE_LOCKING,
                TxnState state = TxnState::GROWING)
      : txn_id_(txn_id), last_lsn_(last_lsn), undo_next_lsn_(INVALID_LSN),
        protocol_(protocol), state_(state) {}
};

/**
 * @brief ARIES Recovery Manager (Dual-Protocol Edition)
 * 
 * Implements three-phase ARIES recovery with support for BOTH 2PL and MVCC:
 *  1. Analysis: Reconstruct ATT (with protocol) and DPT
 *  2. Redo: Protocol-agnostic physical replay
 *  3. Undo: Physical rollback of all loser table changes with CLRs
 * 
 * Key Design:
 *  - 95% of recovery is protocol-agnostic (physical redo)
 *  - Protocol metadata is retained for diagnostics and future extensions
 *  - MVCC losers require physical undo because version chains are not persistent
 */
class Recovery
{
public:
  Recovery(DiskManager *disk_manager, BufferPoolManager *buffer_pool_manager,
          LogManager *log_manager, TxnManager *txn_manager = nullptr)
      : disk_manager_(disk_manager), 
        buffer_pool_manager_(buffer_pool_manager),
        log_manager_(log_manager),
        txn_manager_(txn_manager)
  {}

  void SetDBHandle(DatabaseHandle *db_hdl);
  void SetDBHandles(const std::unordered_map<std::string, std::unique_ptr<DatabaseHandle>> &db_hdls);

  /**
   * @brief Main recovery entry point
   * 
   * Executes three-phase ARIES recovery:
   *  1. Analysis: Build ATT and DPT
   *  2. Redo: Repeat history (protocol-agnostic)
   *  3. Undo: Rollback uncommitted transactions (protocol-aware)
   */
  void Recover();

  /**
   * @brief Phase 1: Analysis
   * 
   * Reconstructs system state by scanning log:
   *  - Build Active Transaction Table (ATT) with protocol info
   *  - Build Dirty Page Table (DPT)
   *  - Determine redo start LSN
   */
  void AnalyzeLog();

  /**
   * @brief Phase 2: Redo (Protocol-Agnostic)
   * 
   * Repeats history by replaying all UPDATE and CLR records.
   * Works identically for both 2PL and MVCC (physical redo).
   */
  void Redo();

  /**
   * @brief Phase 3: Undo loser table changes
   * 
   * Rolls back uncommitted transactions:
   * Both 2PL and MVCC losers use physical undo with CLRs after a crash.
   */
  void Undo();

  //===--------------------------------------------------------------------===//
  // Helper Methods
  //===--------------------------------------------------------------------===//

  /**
   * @brief Find last checkpoint LSN in log
   * @return Checkpoint LSN or INVALID_LSN if none exists
   */
  auto FindLastCheckpoint() -> lsn_t;

  /**
   * @brief Apply a log record (redo operation)
   * 
   * Protocol-agnostic: Works identically for 2PL and MVCC
   * 
   * @param log_record Log record to apply
   */
  void ApplyLogRecord(const LogRecord &log_record);

  /**
   * @brief Undo a log record (rollback operation)
   * 
   * Called for loser transactions from either protocol.
   * 
   * @param log_record Log record to undo
   * @param txn_id Transaction being rolled back
   * @return LSN of CLR written
   */
  auto UndoLogRecord(const LogRecord &log_record, txn_id_t txn_id) -> lsn_t;

  /**
   * @brief Check if transaction needs physical undo after a crash
   */
  auto NeedsUndo(ConcurrencyProtocol protocol) const -> bool {
    (void)protocol;
    return true;
  }

  //===--------------------------------------------------------------------===//
  // Accessors (for testing)
  //===--------------------------------------------------------------------===//

  auto GetATT() const -> const std::unordered_map<txn_id_t, ActiveTxnEntry>& {
    return att_;
  }

  auto GetDPT() const -> const std::unordered_map<std::pair<std::string, page_id_t>, lsn_t, FilePageHash>& {
    return dpt_;
  }

  auto GetRedoStartLSN() const -> lsn_t {
    return redo_start_lsn_;
  }

private:
  auto ResolveTable(const LogRecord &log_record) -> TableHandle *;

  DiskManager       *disk_manager_;
  BufferPoolManager *buffer_pool_manager_;
  LogManager        *log_manager_;
  [[maybe_unused]] TxnManager *txn_manager_;  // Reserved for future transaction management
  std::unordered_map<std::string, DatabaseHandle *> db_hdls_;

  // Hash function for std::pair<file_id_t, page_id_t> (for DPT)
  struct PairHash {
    std::size_t operator()(const std::pair<file_id_t, page_id_t>& p) const {
      return std::hash<file_id_t>()(p.first) ^ (std::hash<page_id_t>()(p.second) << 1);
    }
  };
  
  // Recovery state
  std::unordered_map<txn_id_t, ActiveTxnEntry> att_;  // Active Transaction Table
  // CRITICAL FIX: DPT uses (filename, page_id) instead of (file_id, page_id)
  // because file_ids are transient and change between system restarts.
  // Filenames are persistent and can be resolved to current file_id during recovery.
  // Key is (filename, page_id) pair, value is recLSN (first LSN that modified the page)
  std::unordered_map<std::pair<std::string, page_id_t>, lsn_t, FilePageHash> dpt_;
  lsn_t redo_start_lsn_{0};                            // Start LSN for redo phase
};

}  // namespace njudb

#endif  // NJUDB_RECOVERY_H
