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
// Created for Lab05 - MVCC Version Management
//

#ifndef NJUDB_VERSION_MANAGER_H
#define NJUDB_VERSION_MANAGER_H

#include <memory>
#include <list>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>
#include "common/rid.h"
#include "common/types.h"
#include "concurrency/undo_record.h"
#include "concurrency/version_chain.h"  // Include separate version chain implementation
#include "concurrency/version_table.h"  // Include version table implementation
#include "common/record.h"

namespace njudb {

// Forward declarations
class Transaction;
class TableHandle;
class IndexHandle;

struct VersionPreparation {
  UndoRecord *undo_record{nullptr};
  bool created{false};
  bool previous_deleted{false};
  VersionChainHandle chain;
};

/**
 * @brief Global version manager for MVCC
 * 
 * Responsibilities:
 *  - Maintain version chains for all tuples
 *  - Create new versions on updates
 *  - Retrieve visible versions for reads
 *  - Coordinate with garbage collector for version pruning
 * 
 * Thread-safe: All public methods are thread-safe
 */
class VersionManager {
public:
  VersionManager() = default;
  
  ~VersionManager();
  
  //===--------------------------------------------------------------------===//
  // Version creation
  //===--------------------------------------------------------------------===//
  
  /**
   * @brief Create new version for a tuple update
   * @param txn Transaction creating the version
   * @param rid Record identifier
   * @param old_tuple Previous tuple data
   * @return Pointer to created undo record
   * 
   * Adds version to head of version chain for the tuple.
   * Creates chain if it doesn't exist.
   */
  auto CreateVersion(Transaction *txn, const RID &rid, RecordUptr old_tuple) -> UndoRecord*;
  
  /**
   * @brief Create delete marker version
   * @param txn Transaction deleting the tuple
   * @param rid Record identifier
   * @param old_tuple Tuple data before deletion
   * @return Pointer to created undo record
   */
  auto CreateDeleteVersion(Transaction *txn, const RID &rid, RecordUptr old_tuple) -> UndoRecord*;

  /**
   * @brief Prepare one transaction's logical write, reusing its uncommitted head
   * when the same transaction modifies the RID repeatedly.
   */
  auto PrepareVersion(Transaction *txn, const RID &rid, RecordUptr old_tuple, bool deleted) -> VersionPreparation;
  auto PrepareVersion(Transaction *txn, table_id_t table_id, const RID &rid, RecordUptr old_tuple, bool deleted)
      -> VersionPreparation;

  /** Restore version-chain state when the associated physical write fails. */
  void RollbackPreparedVersion(Transaction *txn, const VersionPreparation &preparation);

  /** Register a logical delete for physical reclamation after the watermark advances. */
  void RegisterPendingDelete(TableHandle *table, const std::list<IndexHandle *> &indexes,
      const Record &indexed_tuple, UndoRecord *delete_version);

  /** Remove a pending logical delete when the associated write fails. */
  void CancelPendingDelete(TableHandle *table, const RID &rid, UndoRecord *delete_version);

  //===--------------------------------------------------------------------===//
  // Version retrieval
  //===--------------------------------------------------------------------===//
  
  /**
   * @brief Resolve whether a tuple read should use the page, an undo tuple, or no tuple
   * @param rid Record identifier
   * @param read_ts Read timestamp of requesting transaction
   * The RID-only overload is retained for isolated version-manager tests.
   * Runtime table reads must use the table-aware overload.
   *
   * This is the authoritative runtime MVCC visibility interface. Tuple-header
   * xmin/xmax fields are recovery/diagnostic metadata, not a fallback resolver.
   */
  auto ResolveVisibility(const RID &rid, timestamp_t read_ts, txn_id_t reader_txn_id) -> VersionVisibilityResult;
  auto ResolveVisibility(table_id_t table_id, const RID &rid, timestamp_t read_ts, txn_id_t reader_txn_id)
      -> VersionVisibilityResult;
  
  /**
   * @brief Check if tuple has version chain
   * @param rid Record identifier
   * @return true if version chain exists
   */
  auto HasVersionChain(const RID &rid) const -> bool;
  auto HasVersionChain(table_id_t table_id, const RID &rid) const -> bool;
  
  /**
   * @brief Get version chain for a tuple
   * @param rid Record identifier
   * @return Shared version-chain handle, or nullptr if it does not exist
   */
  auto GetVersionChain(const RID &rid) -> VersionChainHandle;
  auto GetVersionChain(table_id_t table_id, const RID &rid) -> VersionChainHandle;
  
  //===--------------------------------------------------------------------===//
  // Garbage collection support
  //===--------------------------------------------------------------------===//
  
  /**
   * @brief Prune old versions across all tuples
   * @param watermark Minimum active transaction timestamp
   * @return Total number of versions pruned
   * 
   * Iterates through all version chains and removes versions older than
   * watermark that are no longer needed by any active transaction.
   */
  auto PruneVersions(timestamp_t watermark) -> size_t;

  /**
   * Physically remove committed logical deletes older than the watermark.
   * @return Number of tuple slots reclaimed
   */
  auto ReclaimDeletedTuples(timestamp_t watermark) -> size_t;

  /**
   * @brief Return redo-needed pages for MVCC logical deletes not reclaimed yet.
   *
   * Runtime MVCC may keep a committed deleted tuple physically present on page
   * so old snapshots can read its undo version. Version chains are in-memory,
   * so a checkpoint must keep the corresponding DELETE log in the DPT until
   * physical reclamation removes the tuple.
   */
  auto GetPendingDeleteDPT() const
      -> std::unordered_map<std::pair<std::string, page_id_t>, lsn_t, FilePageHash>;
  
  /**
   * @brief Get total number of version chains
   */
  auto GetVersionChainCount() const -> size_t;
  
  /**
   * @brief Get total number of versions across all chains
   */
  auto GetTotalVersionCount() const -> size_t;

private:
  /**
   * @brief Get or create version chain for a tuple
   * @param rid Record identifier
   * @return Pointer to version chain (never nullptr)
   */
  auto GetOrCreateVersionChain(const RID &rid) -> VersionChainHandle;
  
  /** Version table managing all version chains (mutable for const methods) */
  mutable VersionTable version_table_;

  struct PendingDelete {
    TableHandle *table;
    VersionKey key;
    UndoRecord *delete_version;
    VersionChainHandle chain;
    std::vector<IndexHandle *> indexes;
    RecordUptr indexed_tuple;
  };

  std::vector<PendingDelete> pending_deletes_;
  mutable std::mutex pending_delete_latch_;
};

}  // namespace njudb

#endif  // NJUDB_VERSION_MANAGER_H
