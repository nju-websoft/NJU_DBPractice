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
// Created for Lab05 - MVCC Timestamp Management
//

#ifndef NJUDB_TIMESTAMP_ORACLE_H
#define NJUDB_TIMESTAMP_ORACLE_H

#include <atomic>
#include <mutex>
#include <unordered_set>
#include "common/types.h"

namespace njudb {

/**
 * @brief Timestamp oracle for MVCC
 *
 * Responsibilities:
 *  - Generate monotonically increasing timestamps
 *  - Track active transactions for garbage collection
 *  - Provide watermark for version pruning
 *
 * Thread-safe: All methods use atomic operations or mutexes
 */
class TimestampOracle
{
public:
  TimestampOracle() : next_timestamp_(1) {}

  ~TimestampOracle() = default;

  //===--------------------------------------------------------------------===//
  // Timestamp generation
  //===--------------------------------------------------------------------===//

  /**
   * @brief Get next monotonically increasing timestamp
   * @return New timestamp
   *
   * Thread-safe using atomic fetch_add
   */
  auto GetNextTimestamp() -> timestamp_t;

  /**
   * @brief Get current timestamp without incrementing
   * @return Current timestamp value
   */
  auto GetCurrentTimestamp() const -> timestamp_t;

  //===--------------------------------------------------------------------===//
  // Active transaction tracking
  //===--------------------------------------------------------------------===//

  /**
   * @brief Register a transaction as active
   * @param read_ts Read timestamp of the transaction
   *
   * Called when transaction begins (for MVCC transactions)
   */
  void RegisterTransaction(timestamp_t read_ts);

  /**
   * @brief Unregister a transaction (commit or abort)
   * @param read_ts Read timestamp of the transaction
   */
  void UnregisterTransaction(timestamp_t read_ts);

  /**
   * @brief Get minimum active transaction timestamp (watermark)
   * @return Minimum read timestamp among active transactions
   *
   * Used by garbage collector to determine which versions can be pruned.
   * Versions older than watermark and not visible to any active transaction
   * can be safely removed.
   */
  auto GetWatermark() const -> timestamp_t;

  /**
   * @brief Get count of active transactions
   * @return Number of registered transactions
   */
  auto GetActiveTransactionCount() const -> size_t;

private:
  /** Next timestamp to allocate */
  std::atomic<timestamp_t> next_timestamp_;

  /** Set of active transaction read timestamps */
  std::unordered_set<timestamp_t> active_transactions_;

  /** Mutex for active_transactions_ */
  mutable std::mutex latch_;
};

}  // namespace njudb

#endif  // NJUDB_TIMESTAMP_ORACLE_H
