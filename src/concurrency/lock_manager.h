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

#ifndef NJUDB_LOCK_MANAGER_H
#define NJUDB_LOCK_MANAGER_H

#include <algorithm>
#include <condition_variable>
#include <list>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "../../common/micro.h"
#include "common/rid.h"
#include "common/types.h"
#include "txn_manager.h"

namespace njudb {

/**
 * @brief Lock modes supported by the lock manager
 *
 * Lock compatibility matrix (true = compatible):
 *        IS    IX    S     SIX   X
 * IS   | T     T     T     T     F
 * IX   | T     T     F     F     F
 * S    | T     F     T     F     F
 * SIX  | T     F     F     F     F
 * X    | F     F     F     F     F
 */
enum class LockMode
{
  SHARED                     = 0,  // S: Shared lock (read)
  EXCLUSIVE                  = 1,  // X: Exclusive lock (write)
  INTENTION_SHARED           = 2,  // IS: Intention to acquire S locks
  INTENTION_EXCLUSIVE        = 3,  // IX: Intention to acquire X locks
  SHARED_INTENTION_EXCLUSIVE = 4   // SIX: S + IX
};

/**
 * @brief Deadlock handling policy
 *
 * - DETECTION: Wait-for graph with cycle detection (default for 2PL)
 * - WAIT_DIE: Timestamp-based prevention - older waits, younger aborts
 * - WOUND_WAIT: Timestamp-based prevention - older aborts younger, younger waits
 * - HYBRID: Use prevention for MVCC, detection for 2PL (future)
 */
enum class DeadlockPolicy
{
  DETECTION,   // Wait-for graph + DFS cycle detection
  WAIT_DIE,    // Older waits, younger dies
  WOUND_WAIT,  // Older wounds younger, younger waits
  HYBRID       // Mode-specific (not implemented yet)
};

/**
 * @brief Action to take when lock cannot be granted immediately
 * Used by prevention protocols
 */
enum class PreventionAction
{
  GRANT,         // Grant lock immediately
  WAIT,          // Wait for lock (may use detection)
  ABORT_SELF,    // Abort requesting transaction
  ABORT_HOLDERS  // Abort holding transactions (wound)
};

/**
 * @brief Represents a single lock request from a transaction
 */
struct LockRequest
{
  txn_id_t txn_id_;     // Transaction requesting the lock
  LockMode lock_mode_;  // Type of lock requested
  bool     granted_;    // Whether the lock has been granted

  LockRequest(txn_id_t txn_id, LockMode lock_mode) : txn_id_(txn_id), lock_mode_(lock_mode), granted_(false) {}
};

/**
 * @brief Wrapper for lock request queue with metadata
 */
class LockRequestQueue
{
public:
  std::list<LockRequest>  request_queue_;                     // Queue of lock requests
  std::condition_variable cv_;                                // For waiting transactions
  bool                    upgrading_{false};                  // Whether upgrade is in progress
  txn_id_t                upgrading_txn_id_{INVALID_TXN_ID};  // Transaction doing upgrade

  /**
   * @brief Find a lock request for a transaction in the queue
   * @return Iterator to the request, or end() if not found
   */
  auto FindRequest(txn_id_t txn_id) -> std::list<LockRequest>::iterator
  {
    return std::find_if(request_queue_.begin(), request_queue_.end(), [txn_id](const LockRequest &req) {
      return req.txn_id_ == txn_id;
    });
  }

  /**
   * @brief Check if lock can be granted based on compatibility
   * @param txn_id Transaction requesting the lock (skip this one)
   * @param lock_mode Mode being requested
   * @return true if can grant, false if must wait
   */
  auto CanGrant(txn_id_t txn_id, LockMode lock_mode) const -> bool;

  /**
   * @brief Get all granted lock modes (for compatibility check)
   * @param skip_txn Transaction to skip (usually the requester)
   */
  auto GetGrantedModes(txn_id_t skip_txn = INVALID_TXN_ID) const -> std::vector<LockMode>;
};

/**
 * @brief Lock Manager handles lock acquisition, release, and deadlock handling
 *
 * Supports both deadlock detection (wait-for graph) and prevention (timestamp-based).
 * Default policy is DETECTION for compatibility with Phase 1 plan.
 */
class LockManager
{
public:
  /**
   * @brief Construct lock manager with specified deadlock policy
   * @param policy Deadlock handling policy (default: DETECTION)
   */
  explicit LockManager(DeadlockPolicy policy = DeadlockPolicy::DETECTION, TxnManager *txn_mgr = nullptr)
      : deadlock_policy_(policy), txn_mgr_(txn_mgr)
  {}

  ~LockManager() = default;

  // Disable copy/move
  DISABLE_COPY_MOVE_AND_ASSIGN(LockManager)

  //===--------------------------------------------------------------------===//
  // Configuration
  //===--------------------------------------------------------------------===//

  /**
   * @brief Set the deadlock handling policy
   */
  void SetDeadlockPolicy(DeadlockPolicy policy) { deadlock_policy_ = policy; }

  /**
   * @brief Get current deadlock policy
   */
  auto GetDeadlockPolicy() const -> DeadlockPolicy { return deadlock_policy_; }

  /**
   * @brief Set transaction manager reference (for timestamp lookups)
   */
  void SetTxnManager(TxnManager *txn_mgr) { txn_mgr_ = txn_mgr; }

  //===--------------------------------------------------------------------===//
  // Table-level locking
  //===--------------------------------------------------------------------===//

  /**
   * @brief Acquire a lock on a table
   * @param txn Transaction requesting the lock
   * @param lock_mode Type of lock to acquire
   * @param table_id ID of the table
   * @return true if lock acquired, false if deadlock/abort
   */
  auto LockTable(Transaction *txn, LockMode lock_mode, table_id_t table_id) -> bool;

  /**
   * @brief Release a lock on a table
   * @param txn Transaction releasing the lock
   * @param table_id ID of the table
   * @return true if lock released successfully
   */
  auto UnlockTable(Transaction *txn, table_id_t table_id) -> bool;

  //===--------------------------------------------------------------------===//
  // Page-level locking
  //===--------------------------------------------------------------------===//

  /**
   * @brief Acquire a lock on a page
   * @param txn Transaction requesting the lock
   * @param lock_mode Type of lock to acquire
   * @param page_id ID of the page
   * @return true if lock acquired, false if deadlock/abort
   */
  auto LockPage(Transaction *txn, LockMode lock_mode, page_id_t page_id) -> bool;

  /**
   * @brief Release a lock on a page
   * @param txn Transaction releasing the lock
   * @param page_id ID of the page
   * @return true if lock released successfully
   */
  auto UnlockPage(Transaction *txn, page_id_t page_id) -> bool;

  //===--------------------------------------------------------------------===//
  // Tuple-level locking
  //===--------------------------------------------------------------------===//

  /**
   * @brief Acquire a lock on a tuple (record)
   * @param txn Transaction requesting the lock
   * @param lock_mode Type of lock to acquire
   * @param rid Record ID (page_id + slot_id)
   * @return true if lock acquired, false if deadlock/abort
   */
  auto LockTuple(Transaction *txn, LockMode lock_mode, const RID &rid) -> bool;

  /**
   * @brief Release a lock on a tuple
   * @param txn Transaction releasing the lock
   * @param rid Record ID
   * @return true if lock released successfully
   */
  auto UnlockTuple(Transaction *txn, const RID &rid) -> bool;

  /**
   * @brief Release all locks held by a transaction
   * @param txn Transaction to release all locks for
   *
   * This method is called during commit/abort to release all locks
   * held by the transaction according to the 2PL protocol.
   *
   * For strict 2PL: Locks are released only at commit/abort
   * For non-strict 2PL: Some locks may be released earlier (e.g., READ_COMMITTED)
   */
  void UnlockAll(Transaction *txn);

  //===--------------------------------------------------------------------===//
  // Deadlock detection (for DETECTION policy)
  //===--------------------------------------------------------------------===//

  /**
   * @brief Detect deadlock using wait-for graph
   * @param abort_txn_id Output parameter for transaction to abort
   * @return true if deadlock detected
   */
  auto HasCycle(txn_id_t *abort_txn_id) -> bool;

  /**
   * @brief Get all edges in the wait-for graph (for debugging/testing)
   * @return Vector of (waiting_txn, holding_txn) pairs
   */
  auto GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>>;

  /**
   * @brief Add an edge to the wait-for graph
   * @param t1 Waiting transaction
   * @param t2 Holding transaction
   */
  void AddEdge(txn_id_t t1, txn_id_t t2);

  /**
   * @brief Remove all edges from/to a transaction
   * @param txn_id Transaction ID
   */
  void RemoveEdge(txn_id_t txn_id);

private:
  //===--------------------------------------------------------------------===//
  // Helper methods - Lock compatibility
  //===--------------------------------------------------------------------===//

  /**
   * @brief Check if two lock modes are compatible
   * @param held Lock mode currently held
   * @param requested Lock mode being requested
   * @return true if compatible
   */
  auto AreLocksCompatible(LockMode held, LockMode requested) const -> bool;

  /**
   * @brief Check if lock upgrade is valid
   * @param old_mode Current lock mode
   * @param new_mode Requested lock mode
   * @return true if upgrade is valid
   */
  auto CanLockUpgrade(LockMode old_mode, LockMode new_mode) const -> bool;

  //===--------------------------------------------------------------------===//
  // Helper methods - Lock granting
  //===--------------------------------------------------------------------===//

  /**
   * @brief Grant all compatible waiting locks after a lock release
   * @param lock_queue Lock request queue for the resource
   */
  void GrantWaitingLocks(LockRequestQueue *lock_queue);

  //===--------------------------------------------------------------------===//
  // Helper methods - Deadlock detection
  //===--------------------------------------------------------------------===//

  /**
   * @brief Build wait-for graph from current lock queues
   */
  void BuildWaitForGraph();

  /**
   * @brief DFS helper for cycle detection
   * @param txn_id Current transaction
   * @param visited Set of visited transactions
   * @param rec_stack Recursion stack for cycle detection
   * @param abort_txn_id Output for youngest transaction in cycle
   * @return true if cycle detected
   */
  auto DFS(txn_id_t txn_id, std::unordered_set<txn_id_t> &visited, std::unordered_set<txn_id_t> &rec_stack,
      txn_id_t *abort_txn_id) -> bool;

  //===--------------------------------------------------------------------===//
  // Helper methods - Deadlock prevention
  //===--------------------------------------------------------------------===//

  /**
   * @brief Apply prevention protocol (Wait-Die or Wound-Wait)
   * @param txn Transaction requesting lock
   * @param queue Lock request queue
   * @param lock_mode Requested lock mode
   * @return Action to take (GRANT, WAIT, ABORT_SELF, ABORT_HOLDERS)
   */
  auto ApplyPreventionProtocol(Transaction *txn, LockRequestQueue *queue, LockMode lock_mode) -> PreventionAction;

  /**
   * @brief Get transaction by ID (helper for prevention)
   * @param txn_id Transaction ID
   * @return Stable transaction handle, or nullptr if not found
   */
  auto GetTransaction(txn_id_t txn_id) -> std::shared_ptr<Transaction>;

  //===--------------------------------------------------------------------===//
  // Helper methods - Request cleanup
  //===--------------------------------------------------------------------===//

  /**
   * @brief Clean up a lock request and upgrading state
   * @param lock_queue Lock request queue
   * @param txn_id Transaction ID to clean up
   */
  void CleanupLockRequest(LockRequestQueue *lock_queue, txn_id_t txn_id);

  /**
   * @brief Handle deadlock abort for a transaction
   * @param lock_queue Lock request queue
   * @param txn Transaction to abort
   * @param lock_type Type of lock ("LockTable", "LockPage", "LockTuple")
   */
  void HandleDeadlockAbort(LockRequestQueue *lock_queue, Transaction *txn, const std::string &lock_type);

  /**
   * @brief Release all lock-manager state for a wounded transaction.
   *
   * Caller must hold latch_. This is intentionally lock-layer cleanup only:
   * transaction rollback is still completed later by TxnManager::Abort().
   */
  void ReleaseWoundedTransactionLocks(Transaction *txn);

  //===--------------------------------------------------------------------===//
  // Helper methods - Lock tracking
  //===--------------------------------------------------------------------===//

  /**
   * @brief Track a table lock in transaction's lock sets
   */
  void TrackTableLock(Transaction *txn, table_id_t table_id, LockMode lock_mode);

  /**
   * @brief Untrack a table lock from transaction's lock sets
   */
  void UntrackTableLock(Transaction *txn, table_id_t table_id, LockMode lock_mode);

  //===--------------------------------------------------------------------===//
  // Generic lock/unlock implementation
  //===--------------------------------------------------------------------===//

  /**
   * @brief Generic lock implementation used by LockTable, LockPage, LockTuple
   * @tparam ResourceId Type of resource identifier (table_id_t, page_id_t, RID)
   * @param txn Transaction requesting lock
   * @param lock_mode Lock mode requested
   * @param resource_id Resource identifier
   * @param lock_map Lock map to use
   * @param track_lock Lambda to track lock in transaction
   * @param untrack_lock Lambda to untrack lock from transaction (for upgrades)
   * @param lock_type_name Name for error messages ("LockTable", "LockPage", "LockTuple")
   * @return true if lock acquired, false otherwise
   */
  template<typename ResourceId>
  auto GenericLock(
    Transaction *txn,
    LockMode lock_mode,
    const ResourceId &resource_id,
    std::unordered_map<ResourceId, std::shared_ptr<LockRequestQueue>> &lock_map,
    std::function<void(Transaction*, const ResourceId&, LockMode)> track_lock,
    std::function<void(Transaction*, const ResourceId&, LockMode)> untrack_lock,
    const std::string &lock_type_name
  ) -> bool;

  /**
   * @brief Generic unlock implementation used by UnlockTable, UnlockPage, UnlockTuple
   * @tparam ResourceId Type of resource identifier (table_id_t, page_id_t, RID)
   * @param txn Transaction releasing lock
   * @param resource_id Resource identifier
   * @param lock_map Lock map to use
   * @param untrack_lock Lambda to untrack lock from transaction
   * @return true if lock released, false otherwise
   */
  template<typename ResourceId>
  auto GenericUnlock(
    Transaction *txn,
    const ResourceId &resource_id,
    std::unordered_map<ResourceId, std::shared_ptr<LockRequestQueue>> &lock_map,
    std::function<void(Transaction*, const ResourceId&, LockMode)> untrack_lock,
    bool force_unlock = false
  ) -> bool;

  //===--------------------------------------------------------------------===//
  // Member variables
  //===--------------------------------------------------------------------===//

  /** Deadlock handling policy */
  DeadlockPolicy deadlock_policy_;

  /** Transaction manager reference (for timestamp lookups) */
  TxnManager *txn_mgr_;

  /** Map from table_id to lock request queue */
  std::unordered_map<table_id_t, std::shared_ptr<LockRequestQueue>> table_lock_map_;

  /** Map from page_id to lock request queue */
  std::unordered_map<page_id_t, std::shared_ptr<LockRequestQueue>> page_lock_map_;

  /** Map from RID to lock request queue */
  std::unordered_map<RID, std::shared_ptr<LockRequestQueue>> tuple_lock_map_;

  /** Wait-for graph: txn_id -> list of txns it's waiting for */
  std::unordered_map<txn_id_t, std::vector<txn_id_t>> wait_for_graph_;

  /** Global latch for lock manager */
  std::mutex latch_;

  /** Latch for wait-for graph */
  std::mutex waits_for_latch_;
};

}  // namespace njudb

#endif  // NJUDB_LOCK_MANAGER_H
