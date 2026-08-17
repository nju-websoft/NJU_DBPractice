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

#ifndef NJUDB_GARBAGE_COLLECTOR_H
#define NJUDB_GARBAGE_COLLECTOR_H

#include <atomic>
#include <mutex>
#include <thread>
#include "common/types.h"
#include "common/rid.h"

namespace njudb {

// Forward declarations
class VersionManager;
class TimestampOracle;

/**
 * @brief Background garbage collector for MVCC version chains
 *
 * Prunes obsolete versions that are no longer visible to any active transaction.
 *
 * Key concepts:
 *  - Watermark: Minimum read timestamp of all active transactions
 *  - Undo records at or before the watermark are no longer needed once every
 *    active snapshot can read the current page or a newer undo boundary
 *
 * Design decisions:
 *  - Separate background thread for non-blocking pruning
 *  - Periodic wake-up (configurable interval)
 *  - Cooperative iteration with VersionManager
 */
class GarbageCollector
{
public:
  explicit GarbageCollector(
      VersionManager *version_manager, TimestampOracle *timestamp_oracle, uint32_t gc_interval_ms = 1000)
      : version_manager_(version_manager),
        timestamp_oracle_(timestamp_oracle),
        gc_interval_ms_(gc_interval_ms),
        running_(false)
  {}

  ~GarbageCollector() { Stop(); }

  //===--------------------------------------------------------------------===//
  // Lifecycle management
  //===--------------------------------------------------------------------===//

  /**
   * @brief Start garbage collection background thread
   */
  auto Start() -> void;

  /**
   * @brief Stop garbage collection and join thread
   */
  auto Stop() -> void;

  /**
   * @brief Check if garbage collector is running
   */
  auto IsRunning() const -> bool { return running_; }

  //===--------------------------------------------------------------------===//
  // Manual garbage collection
  //===--------------------------------------------------------------------===//

  /**
   * @brief Trigger immediate garbage collection (blocking)
   *
   * For testing or explicit cleanup after bulk operations
   */
  auto CollectGarbage() -> void;

  /**
   * @brief Prune versions for a specific tuple
   *
   * @param table_id Table identifier
   * @param rid Record identifier
   * @return Number of versions pruned
   */
  auto PruneTuple(table_id_t table_id, const RID &rid) -> size_t;

  //===--------------------------------------------------------------------===//
  // Statistics
  //===--------------------------------------------------------------------===//

  /**
   * @brief Get total number of versions pruned since start
   */
  auto GetTotalPrunedVersions() const -> size_t { return total_pruned_; }

  /** Get total number of logically deleted tuple slots physically reclaimed. */
  auto GetTotalReclaimedTuples() const -> size_t { return total_reclaimed_; }

  /**
   * @brief Get number of GC cycles completed
   */
  auto GetGCCycles() const -> size_t { return gc_cycles_; }

  /**
   * @brief Reset statistics
   */
  auto ResetStatistics() -> void
  {
    total_pruned_.store(0);
    total_reclaimed_.store(0);
    gc_cycles_.store(0);
  }

private:
  /**
   * @brief Background thread main loop
   *
   * Periodically:
   *  1. Get watermark from TimestampOracle
   *  2. Iterate all version chains via VersionManager
   *  3. Call PruneVersions(watermark) on each chain
   */
  auto RunGarbageCollection() -> void;

  VersionManager  *version_manager_;
  TimestampOracle *timestamp_oracle_;

  uint32_t gc_interval_ms_;  // Sleep interval between GC cycles

  std::atomic<bool> running_;    // Flag to control thread lifecycle
  std::thread       gc_thread_;  // Background GC thread
  std::mutex        collection_latch_;  // Serializes manual/background GC cycles

  // Statistics
  std::atomic<size_t> total_pruned_{0};  // Total versions pruned
  std::atomic<size_t> total_reclaimed_{0};  // Total logical deletes physically reclaimed
  std::atomic<size_t> gc_cycles_{0};     // Number of GC cycles
};

}  // namespace njudb

#endif  // NJUDB_GARBAGE_COLLECTOR_H
