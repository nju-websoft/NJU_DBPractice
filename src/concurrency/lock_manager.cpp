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
// Created by ziqi on 2025/10/30.
//

#include "lock_manager.h"
#include "../common/error.h"
#include <stdexcept>
#include <chrono>

namespace njudb {

static void UntrackPageLock(Transaction *txn, const page_id_t &page_id, LockMode lock_mode);
static void UntrackTupleLock(Transaction *txn, const RID &rid, LockMode lock_mode);

//===--------------------------------------------------------------------===//
// LockRequestQueue Implementation
//===--------------------------------------------------------------------===//

auto LockRequestQueue::CanGrant(txn_id_t txn_id, LockMode lock_mode) const -> bool {
  NJUDB_STUDENT_TODO(l5, t1);
}

//===--------------------------------------------------------------------===//
// Lock Compatibility Matrix
//===--------------------------------------------------------------------===//

auto LockManager::AreLocksCompatible(LockMode held, LockMode requested) const -> bool {
  NJUDB_STUDENT_TODO(l5, t1);
}

auto LockManager::CanLockUpgrade(LockMode old_mode, LockMode new_mode) const -> bool {
  NJUDB_STUDENT_TODO(l5, t1);
}

//===--------------------------------------------------------------------===//
// Helper Methods
//===--------------------------------------------------------------------===//

void LockManager::CleanupLockRequest(LockRequestQueue *lock_queue, txn_id_t txn_id) {
  lock_queue->request_queue_.remove_if(
    [txn_id](const LockRequest &req) { return req.txn_id_ == txn_id; }
  );
  if (lock_queue->upgrading_ && lock_queue->upgrading_txn_id_ == txn_id) {
    lock_queue->upgrading_ = false;
    lock_queue->upgrading_txn_id_ = INVALID_TXN_ID;
  }
}

void LockManager::HandleDeadlockAbort(LockRequestQueue *lock_queue, Transaction *txn, const std::string &lock_type) {
  txn_id_t txn_id = txn->GetTxnId();
  
  // Clean up request and upgrading state
  CleanupLockRequest(lock_queue, txn_id);
  
  // Remove from wait-for graph
  RemoveEdge(txn_id);
  
  // Mark transaction as aborted
  txn->SetState(TxnState::ABORTED);
  
  // Throw exception to propagate abort to upper layers
  throw NJUDBException_(NJUDB_TXN_ABORTED, "LockManager", lock_type, 
                        fmt::format("Transaction {} aborted due to deadlock", txn_id));
}

void LockManager::ReleaseWoundedTransactionLocks(Transaction *txn) {
  txn_id_t txn_id = txn->GetTxnId();

  auto release_from_map = [this, txn_id](auto &lock_map, const auto &resource, auto untrack_lock) {
    auto map_iter = lock_map.find(resource);
    if (map_iter == lock_map.end()) {
      return;
    }

    auto lock_queue = map_iter->second;
    auto request_iter = lock_queue->FindRequest(txn_id);
    if (request_iter == lock_queue->request_queue_.end()) {
      return;
    }

    LockMode mode = request_iter->lock_mode_;
    untrack_lock(mode);
    lock_queue->request_queue_.erase(request_iter);
    GrantWaitingLocks(lock_queue.get());
    lock_queue->cv_.notify_all();

    if (lock_queue->request_queue_.empty()) {
      lock_map.erase(map_iter);
    }
  };

  auto release_table_locks = [this, txn, &release_from_map](auto *lock_set) {
    std::vector<table_id_t> resources(lock_set->begin(), lock_set->end());
    for (table_id_t table_id : resources) {
      release_from_map(table_lock_map_, table_id, [this, txn, table_id](LockMode mode) {
        UntrackTableLock(txn, table_id, mode);
      });
    }
  };

  auto release_page_locks = [this, txn, &release_from_map](auto *lock_set) {
    std::vector<page_id_t> resources(lock_set->begin(), lock_set->end());
    for (page_id_t page_id : resources) {
      release_from_map(page_lock_map_, page_id, [txn, page_id](LockMode mode) {
        UntrackPageLock(txn, page_id, mode);
      });
    }
  };

  auto release_tuple_locks = [this, txn, &release_from_map](auto *lock_set) {
    std::vector<RID> resources(lock_set->begin(), lock_set->end());
    for (const RID &rid : resources) {
      release_from_map(tuple_lock_map_, rid, [txn, &rid](LockMode mode) {
        UntrackTupleLock(txn, rid, mode);
      });
    }
  };

  release_tuple_locks(txn->GetSharedTupleLockSet());
  release_tuple_locks(txn->GetExclusiveTupleLockSet());
  release_page_locks(txn->GetSharedPageLockSet());
  release_page_locks(txn->GetExclusivePageLockSet());
  release_table_locks(txn->GetSharedTableLockSet());
  release_table_locks(txn->GetExclusiveTableLockSet());
  release_table_locks(txn->GetIntentionSharedTableLockSet());
  release_table_locks(txn->GetIntentionExclusiveTableLockSet());
  release_table_locks(txn->GetSharedIntentionExclusiveTableLockSet());

  RemoveEdge(txn_id);
}

void LockManager::GrantWaitingLocks(LockRequestQueue *lock_queue) {
  for (auto &request : lock_queue->request_queue_) {
    if (request.granted_) {
      continue;
    }
    
    bool compatible = true;
    for (const auto &granted_req : lock_queue->request_queue_) {
      if (!granted_req.granted_) {
        break;
      }
      if (granted_req.txn_id_ == request.txn_id_) {
        continue;
      }
      if (!AreLocksCompatible(granted_req.lock_mode_, request.lock_mode_)) {
        compatible = false;
        break;
      }
    }
    
    if (compatible) {
      request.granted_ = true;
    } else {
      break; // FIFO - stop here
    }
  }
}

auto LockManager::GetTransaction(txn_id_t txn_id) -> std::shared_ptr<Transaction> {
  if (txn_mgr_ == nullptr) {
    return nullptr;
  }
  return txn_mgr_->GetTransactionShared(txn_id);
}

void LockManager::TrackTableLock(Transaction *txn, table_id_t table_id, LockMode lock_mode) {
  switch (lock_mode) {
    case LockMode::SHARED:
      txn->GetSharedTableLockSet()->insert(table_id);
      break;
    case LockMode::EXCLUSIVE:
      txn->GetExclusiveTableLockSet()->insert(table_id);
      break;
    case LockMode::INTENTION_SHARED:
      txn->GetIntentionSharedTableLockSet()->insert(table_id);
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      txn->GetIntentionExclusiveTableLockSet()->insert(table_id);
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      txn->GetSharedIntentionExclusiveTableLockSet()->insert(table_id);
      break;
  }
}

void LockManager::UntrackTableLock(Transaction *txn, table_id_t table_id, LockMode lock_mode) {
  switch (lock_mode) {
    case LockMode::SHARED:
      txn->GetSharedTableLockSet()->erase(table_id);
      break;
    case LockMode::EXCLUSIVE:
      txn->GetExclusiveTableLockSet()->erase(table_id);
      break;
    case LockMode::INTENTION_SHARED:
      txn->GetIntentionSharedTableLockSet()->erase(table_id);
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      txn->GetIntentionExclusiveTableLockSet()->erase(table_id);
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      txn->GetSharedIntentionExclusiveTableLockSet()->erase(table_id);
      break;
  }
}

// Static helper functions for page lock tracking (used in lambdas)
static void TrackPageLock(Transaction *txn, const page_id_t &page_id, LockMode lock_mode) {
  if (lock_mode == LockMode::SHARED) {
    txn->GetSharedPageLockSet()->insert(page_id);
  } else if (lock_mode == LockMode::EXCLUSIVE) {
    txn->GetExclusivePageLockSet()->insert(page_id);
  }
}

static void UntrackPageLock(Transaction *txn, const page_id_t &page_id, LockMode /* lock_mode */) {
  txn->GetSharedPageLockSet()->erase(page_id);
  txn->GetExclusivePageLockSet()->erase(page_id);
}

// Static helper functions for tuple lock tracking (used in lambdas)
static void TrackTupleLock(Transaction *txn, const RID &rid, LockMode lock_mode) {
  if (lock_mode == LockMode::SHARED) {
    txn->GetSharedTupleLockSet()->insert(rid);
  } else if (lock_mode == LockMode::EXCLUSIVE) {
    txn->GetExclusiveTupleLockSet()->insert(rid);
  }
}

static void UntrackTupleLock(Transaction *txn, const RID &rid, LockMode /* lock_mode */) {
  txn->GetSharedTupleLockSet()->erase(rid);
  txn->GetExclusiveTupleLockSet()->erase(rid);
}

//===--------------------------------------------------------------------===//
// Deadlock Prevention
//===--------------------------------------------------------------------===//

auto LockManager::ApplyPreventionProtocol(Transaction *txn,
                                          LockRequestQueue *queue,
                                          LockMode lock_mode) -> PreventionAction {
  NJUDB_STUDENT_TODO(l5, t3);
}

//===--------------------------------------------------------------------===//
// Deadlock Detection
//===--------------------------------------------------------------------===//

void LockManager::BuildWaitForGraph() {
  NJUDB_STUDENT_TODO(l5, t3);
}

auto LockManager::HasCycle(txn_id_t *abort_txn_id) -> bool {
  NJUDB_STUDENT_TODO(l5, t3);
}

auto LockManager::DFS(txn_id_t txn_id,
                      std::unordered_set<txn_id_t> &visited,
                      std::unordered_set<txn_id_t> &rec_stack,
                      txn_id_t *abort_txn_id) -> bool {
  NJUDB_STUDENT_TODO(l5, t3);
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  wait_for_graph_[t1].push_back(t2);
}

void LockManager::RemoveEdge(txn_id_t txn_id) {
  wait_for_graph_.erase(txn_id);
  
  for (auto &pair : wait_for_graph_) {
    auto &vec = pair.second;
    vec.erase(std::remove(vec.begin(), vec.end(), txn_id), vec.end());
  }
}

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::lock_guard<std::mutex> lock(waits_for_latch_);
  std::vector<std::pair<txn_id_t, txn_id_t>> edges;
  
  for (const auto &pair : wait_for_graph_) {
    for (txn_id_t neighbor : pair.second) {
      edges.emplace_back(pair.first, neighbor);
    }
  }
  
  return edges;
}

//===--------------------------------------------------------------------===//
// Generic Lock/Unlock Implementation
//===--------------------------------------------------------------------===//

template<typename ResourceId>
auto LockManager::GenericLock(
  Transaction *txn,
  LockMode lock_mode,
  const ResourceId &resource_id,
  std::unordered_map<ResourceId, std::shared_ptr<LockRequestQueue>> &lock_map,
  std::function<void(Transaction*, const ResourceId&, LockMode)> track_lock,
  std::function<void(Transaction*, const ResourceId&, LockMode)> untrack_lock,
  const std::string &lock_type_name
) -> bool {
  NJUDB_STUDENT_TODO(l5, t2);
}

template<typename ResourceId>
auto LockManager::GenericUnlock(
  Transaction *txn,
  const ResourceId &resource_id,
  std::unordered_map<ResourceId, std::shared_ptr<LockRequestQueue>> &lock_map,
  std::function<void(Transaction*, const ResourceId&, LockMode)> untrack_lock,
  bool force_unlock
) -> bool {
  NJUDB_STUDENT_TODO(l5, t2);
}

//===--------------------------------------------------------------------===//
// Table Locking - Full Implementation
//===--------------------------------------------------------------------===//

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, table_id_t table_id) -> bool {
  return GenericLock<table_id_t>(
    txn, lock_mode, table_id, table_lock_map_,
    [this](Transaction *t, const table_id_t &tid, LockMode mode) { TrackTableLock(t, tid, mode); },
    [this](Transaction *t, const table_id_t &tid, LockMode mode) { UntrackTableLock(t, tid, mode); },
    "LockTable"
  );
}

auto LockManager::UnlockTable(Transaction *txn, table_id_t table_id) -> bool {
  return GenericUnlock<table_id_t>(
    txn, table_id, table_lock_map_,
    [this](Transaction *t, const table_id_t &tid, LockMode mode) { UntrackTableLock(t, tid, mode); }
  );
}

//===--------------------------------------------------------------------===//
// Page Locking - Full Implementation
//===--------------------------------------------------------------------===//

auto LockManager::LockPage(Transaction *txn, LockMode lock_mode, page_id_t page_id) -> bool {
  return GenericLock<page_id_t>(
    txn, lock_mode, page_id, page_lock_map_,
    TrackPageLock,
    UntrackPageLock,
    "LockPage"
  );
}

auto LockManager::UnlockPage(Transaction *txn, page_id_t page_id) -> bool {
  return GenericUnlock<page_id_t>(
    txn, page_id, page_lock_map_,
    UntrackPageLock
  );
}

//===--------------------------------------------------------------------===//
// Tuple Locking - Full Implementation
//===--------------------------------------------------------------------===//

auto LockManager::LockTuple(Transaction *txn, LockMode lock_mode, const RID &rid) -> bool {
  return GenericLock<RID>(
    txn, lock_mode, rid, tuple_lock_map_,
    TrackTupleLock,
    UntrackTupleLock,
    "LockTuple"
  );
}

auto LockManager::UnlockTuple(Transaction *txn, const RID &rid) -> bool {
  return GenericUnlock<RID>(
    txn, rid, tuple_lock_map_,
    UntrackTupleLock
  );
}

void LockManager::UnlockAll(Transaction *txn) {
  NJUDB_STUDENT_TODO(l5, t2);
}

}  // namespace njudb
