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
// Lab06 Phase 3: Checkpoint Manager for WAL (Dual-Protocol Edition)
//

#ifndef NJUDB_CHECKPOINT_MANAGER_H
#define NJUDB_CHECKPOINT_MANAGER_H

#include <unordered_map>
#include <vector>
#include <thread>
#include <atomic>
#include <chrono>
#include "log/log_manager.h"
#include "common/types.h"
#include "concurrency/concurrency_protocol.h"
#include "concurrency/transaction.h"

namespace njudb {

// Forward declarations
class TxnManager;
class BufferPoolManager;
class VersionManager;

/**
 * @brief Checkpoint data structure (protocol-agnostic)
 * 
 * Records:
 *  - Active Transaction Table (ATT): ALL transactions (2PL + MVCC)
 *  - Dirty Page Table (DPT): ALL dirty pages (regardless of protocol)
 *  
 * Note: Protocol information stored per-transaction, not in checkpoint
 */
struct Checkpoint {
  lsn_t checkpoint_lsn_{INVALID_LSN};  // LSN of checkpoint begin record
  
  // Active Transaction Table (includes both 2PL and MVCC transactions)
  struct ATTEntry {
    txn_id_t txn_id_;
    lsn_t last_lsn_;
    ConcurrencyProtocol protocol_;  // Track protocol for recovery
    TxnState state_;
  };
  std::vector<ATTEntry> active_txns_;
  
  // Dirty Page Table (includes pages modified by both protocols)
  // CRITICAL FIX: Store (filename, page_id) pairs instead of (file_id, page_id)
  // because file IDs are transient and change across system restarts.
  // Filenames are persistent and can be resolved to current file_id during recovery.
  std::unordered_map<std::pair<std::string, page_id_t>, lsn_t, FilePageHash> dirty_pages_;
  
  /**
   * @brief Serialize checkpoint to buffer
   * @param buffer Output buffer
   */
  void SerializeTo(char *buffer) const;
  
  /**
   * @brief Deserialize checkpoint from buffer
   * @param buffer Input buffer
   * @return Checkpoint instance
   */
  static auto DeserializeFrom(const char *buffer, size_t buffer_size) -> Checkpoint;
  
  /**
   * @brief Get serialized size
   * @return Size in bytes
   */
  auto GetSize() const -> size_t;
};

/**
 * @brief Checkpoint Manager for fuzzy checkpoints
 * 
 * Works with BOTH 2PL and MVCC protocols:
 *  - Collects mixed-protocol Active Transaction Table
 *  - Collects protocol-agnostic Dirty Page Table
 *  - Supports recovery with protocol-specific handling
 */
class CheckpointManager {
public:
  CheckpointManager(LogManager *log_manager, 
                   TxnManager *txn_manager,
                   BufferPoolManager *buffer_pool_manager,
                   VersionManager *version_manager = nullptr)
      : log_manager_(log_manager),
        txn_manager_(txn_manager),
        buffer_pool_manager_(buffer_pool_manager),
        version_manager_(version_manager),
        stop_flag_(false) {}
  
  ~CheckpointManager() {
    StopPeriodicCheckpointing();
  }
  
  /**
   * @brief Create a fuzzy checkpoint
   * 
   * Works with BOTH protocols:
   *  1. Log CHECKPOINT_BEGIN record
   *  2. Collect ATT (all active transactions, 2PL + MVCC)
   *  3. Collect DPT (all dirty pages, regardless of protocol)
   *  4. Log CHECKPOINT_END record with ATT and DPT
   *  5. Update master record (last checkpoint LSN)
   *  
   * Mixed Protocol Example:
   *   ATT: [Txn1(2PL), Txn2(MVCC), Txn3(2PL), Txn4(MVCC)]
   *   DPT: [Page10(modified by Txn1-2PL), Page20(modified by Txn2-MVCC)]
   */
  void CreateCheckpoint();
  
  /**
   * @brief Start periodic checkpointing in background thread
   * @param interval_seconds Interval between checkpoints in seconds
   */
  void StartPeriodicCheckpointing(uint32_t interval_seconds = 300);
  
  /**
   * @brief Stop periodic checkpointing background thread
   */
  void StopPeriodicCheckpointing();
  
  /**
   * @brief Get last checkpoint LSN
   * @return Last checkpoint LSN or INVALID_LSN if none
   */
  auto GetLastCheckpointLSN() const -> lsn_t { return last_checkpoint_lsn_; }

private:
  /**
   * @brief Collect Active Transaction Table (both protocols)
   * @return Vector of ATT entries
   */
  auto CollectATT() -> std::vector<Checkpoint::ATTEntry>;
  
  /**
   * @brief Collect Dirty Page Table (protocol-agnostic)
   * @return Map of (filename, page_id) to recLSN
   */
  auto CollectDPT() -> std::unordered_map<std::pair<std::string, page_id_t>, lsn_t, FilePageHash>;
  
  LogManager *log_manager_;
  TxnManager *txn_manager_;
  BufferPoolManager *buffer_pool_manager_;
  VersionManager *version_manager_;
  
  lsn_t last_checkpoint_lsn_{INVALID_LSN};
  
  // Periodic checkpointing support
  std::thread checkpoint_thread_;
  std::atomic<bool> stop_flag_;
  uint32_t checkpoint_interval_seconds_{300};  // Default: 5 minutes
};

}  // namespace njudb

#endif  // NJUDB_CHECKPOINT_MANAGER_H
