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
// Created for Lab05 Phase 4.1 - Version Chain Infrastructure
//

#ifndef NJUDB_VERSION_CHAIN_H
#define NJUDB_VERSION_CHAIN_H

#include <mutex>
#include <shared_mutex>
#include "concurrency/undo_record.h"
#include "common/rid.h"

namespace njudb {

enum class VersionVisibilityState {
  READ_PAGE,
  READ_UNDO,
  NOT_VISIBLE
};

struct VersionVisibilityResult {
  VersionVisibilityState state{VersionVisibilityState::NOT_VISIBLE};
  RecordUptr undo_tuple{nullptr};
};

struct ExistingVersionPreparation {
  UndoRecord *undo_record{nullptr};
  bool conflict{false};
  bool previous_deleted{false};
};

/**
 * @brief Manages a version chain for a single tuple
 * 
 * Version chain is organized as a linked list in newest-to-oldest order:
 * head_ → [newest] → [older] → [even older] → ... → nullptr
 * 
 * Thread-safety: Uses a shared_mutex to allow concurrent reads
 */
class VersionChain {
public:
  VersionChain() : head_(nullptr), version_count_(0) {}
  
  ~VersionChain() {
    // Free all versions in the chain
    std::unique_lock<std::shared_mutex> lock(latch_);
    UndoRecord *curr = head_;
    while (curr != nullptr) {
      UndoRecord *next = curr->GetPrev();
      delete curr;
      curr = next;
    }
  }
  
  // Disable copy and move
  VersionChain(const VersionChain&) = delete;
  VersionChain& operator=(const VersionChain&) = delete;
  VersionChain(VersionChain&&) = delete;
  VersionChain& operator=(VersionChain&&) = delete;
  
  /**
   * @brief Check if a new version can be inserted (no uncommitted head)
   * @return true if insertion is safe, false if write-write conflict exists
   * 
   * MUST be called BEFORE creating the UndoRecord to avoid race conditions.
   * Thread-safe: Acquires shared lock (read-only check)
   */
  auto CanInsertVersion() const -> bool;
  
  /**
   * @brief Insert a new version at the head of the chain
   * @param new_version Version to insert (becomes newest)
   * 
   * CRITICAL: Caller MUST call CanInsertVersion() first and hold no other locks
   * between the check and this call to avoid TOCTOU issues.
   * 
   * Thread-safe: Acquires exclusive lock
   */
  void InsertVersion(UndoRecord *new_version);
  
  /**
   * @brief Add new version to head of chain (alias for InsertVersion)
   * @param undo_record New version to add
   * 
   * Thread-safe: Acquires exclusive lock
   */
  void AddVersion(UndoRecord *undo_record) {
    InsertVersion(undo_record);
  }

  /**
   * @brief Remove a newly inserted uncommitted head after its physical write failed
   * @return true if the version was the current head and was removed
   */
  auto RemoveUncommittedHead(UndoRecord *version) -> bool;

  /**
   * Atomically reuse the current transaction's uncommitted head, or report a
   * conflict if another transaction owns it.
   */
  auto PrepareExistingWrite(txn_id_t txn_id, bool deleted) -> ExistingVersionPreparation;

  /** Restore the delete state changed by PrepareExistingWrite after failure. */
  void RestoreExistingWrite(UndoRecord *version, bool previous_deleted);

  /**
   * Check whether the expected head is a committed delete no active snapshot
   * can still need.
   */
  auto CanReclaimDelete(UndoRecord *expected_head, timestamp_t watermark) const -> bool;
  
  /**
   * @brief Resolve a tuple read against an undo-version chain
   * @param read_ts Snapshot timestamp of the reader
   * @param reader_txn_id Transaction ID of the reader
   * @return Whether to read the page, a cloned undo tuple, or treat the tuple as invisible
   *
   * The page stores the newest tuple. Each undo record stores the tuple value
   * that existed before the creating transaction replaced or deleted it.
   */
  auto ResolveVisibility(timestamp_t read_ts, txn_id_t reader_txn_id) const -> VersionVisibilityResult;
  
  /**
   * @brief Get the head (newest) version
   * @return Head version pointer
   * 
   * Thread-safe: Acquires shared lock
   */
  auto GetHead() const -> UndoRecord*;
  
  /**
   * @brief Get number of versions in chain
   * @return Version count
   */
  auto GetVersionCount() const -> size_t;
  
  /**
   * @brief Prune old versions that are no longer needed
   * @param min_read_ts Minimum active read timestamp (versions older than this can be pruned)
   * @return Number of versions pruned
   * 
   * Thread-safe: Acquires exclusive lock
   */
  auto PruneVersions(timestamp_t min_read_ts) -> size_t;
  
  /**
   * @brief Check if version chain needs garbage collection
   * @return True if chain is too long
   */
  auto NeedsGC() const -> bool;

private:
  /** Head of version chain (newest version) */
  UndoRecord *head_;
  
  /** Number of versions in chain */
  size_t version_count_;
  
  /** Latch protecting version chain */
  mutable std::shared_mutex latch_;
  
  /** Threshold for triggering GC */
  static constexpr size_t GC_THRESHOLD = 10;
};

}  // namespace njudb

#endif  // NJUDB_VERSION_CHAIN_H
