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
// Created for Lab05 Phase 4.1/4.2 - MVCC Version Management
//

#include "concurrency/version_manager.h"
#include "concurrency/transaction.h"
#include "system/handle/index_handle.h"
#include "system/handle/table_handle.h"

#include <algorithm>

namespace njudb {

//===--------------------------------------------------------------------===//
// VersionManager Implementation
//===--------------------------------------------------------------------===//

VersionManager::~VersionManager() {
  // Version table destructor will clean up all version chains
  // Nothing else to do here
}

auto VersionManager::CreateVersion(Transaction *txn, const RID &rid, RecordUptr old_tuple) -> UndoRecord* {
  return PrepareVersion(txn, rid, std::move(old_tuple), false).undo_record;
}

auto VersionManager::CreateDeleteVersion(Transaction *txn, const RID &rid, RecordUptr old_tuple) -> UndoRecord* {
  return PrepareVersion(txn, rid, std::move(old_tuple), true).undo_record;
}

auto VersionManager::PrepareVersion(
    Transaction *txn, const RID &rid, RecordUptr old_tuple, bool deleted) -> VersionPreparation {
  return PrepareVersion(txn, INVALID_TABLE_ID, rid, std::move(old_tuple), deleted);
}

auto VersionManager::PrepareVersion(
    Transaction *txn, table_id_t table_id, const RID &rid, RecordUptr old_tuple, bool deleted) -> VersionPreparation {
  NJUDB_STUDENT_TODO(l5_bonus, b3);
}

void VersionManager::RollbackPreparedVersion(Transaction *txn, const VersionPreparation &preparation) {
  NJUDB_STUDENT_TODO(l5_bonus, b3);
}

void VersionManager::RegisterPendingDelete(TableHandle *table, const std::list<IndexHandle *> &indexes,
    const Record &indexed_tuple, UndoRecord *delete_version) {
  NJUDB_STUDENT_TODO(l5_bonus, b3);
}

void VersionManager::CancelPendingDelete(TableHandle *table, const RID &rid, UndoRecord *delete_version) {
  NJUDB_STUDENT_TODO(l5_bonus, b3);
}

auto VersionManager::ResolveVisibility(
    const RID &rid, timestamp_t read_ts, txn_id_t reader_txn_id) -> VersionVisibilityResult {
  return ResolveVisibility(INVALID_TABLE_ID, rid, read_ts, reader_txn_id);
}

auto VersionManager::ResolveVisibility(
    table_id_t table_id, const RID &rid, timestamp_t read_ts, txn_id_t reader_txn_id) -> VersionVisibilityResult {
  NJUDB_STUDENT_TODO(l5_bonus, b3);
}

auto VersionManager::HasVersionChain(const RID &rid) const -> bool {
  return HasVersionChain(INVALID_TABLE_ID, rid);
}

auto VersionManager::HasVersionChain(table_id_t table_id, const RID &rid) const -> bool {
  return version_table_.HasVersionChain({table_id, rid});
}

auto VersionManager::GetVersionChain(const RID &rid) -> VersionChainHandle {
  return GetVersionChain(INVALID_TABLE_ID, rid);
}

auto VersionManager::GetVersionChain(table_id_t table_id, const RID &rid) -> VersionChainHandle {
  return version_table_.FindVersionChain({table_id, rid});
}

auto VersionManager::PruneVersions(timestamp_t watermark) -> size_t {
  NJUDB_STUDENT_TODO(l5_bonus, b3);
}

auto VersionManager::ReclaimDeletedTuples(timestamp_t watermark) -> size_t {
  NJUDB_STUDENT_TODO(l5_bonus, b3);
}

auto VersionManager::GetPendingDeleteDPT() const
    -> std::unordered_map<std::pair<std::string, page_id_t>, lsn_t, FilePageHash> {
  NJUDB_STUDENT_TODO(l5_bonus, b3);
}

auto VersionManager::GetVersionChainCount() const -> size_t {
  auto stats = version_table_.GetStats();
  return stats.total_chains;
}

auto VersionManager::GetTotalVersionCount() const -> size_t {
  auto stats = version_table_.GetStats();
  return stats.total_versions;
}

auto VersionManager::GetOrCreateVersionChain(const RID &rid) -> VersionChainHandle {
  return version_table_.GetOrCreateVersionChain(rid);
}

}  // namespace njudb
