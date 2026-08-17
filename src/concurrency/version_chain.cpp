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

#include "concurrency/version_chain.h"
#include "../../common/error.h"

namespace njudb {

auto VersionChain::CanInsertVersion() const -> bool {
  NJUDB_STUDENT_TODO(l5_bonus, b2);
}

void VersionChain::InsertVersion(UndoRecord *new_version) {
  NJUDB_STUDENT_TODO(l5_bonus, b2);
}

auto VersionChain::RemoveUncommittedHead(UndoRecord *version) -> bool {
  NJUDB_STUDENT_TODO(l5_bonus, b2);
}

auto VersionChain::PrepareExistingWrite(txn_id_t txn_id, bool deleted) -> ExistingVersionPreparation {
  NJUDB_STUDENT_TODO(l5_bonus, b2);
}

void VersionChain::RestoreExistingWrite(UndoRecord *version, bool previous_deleted) {
  NJUDB_STUDENT_TODO(l5_bonus, b2);
}

auto VersionChain::CanReclaimDelete(UndoRecord *expected_head, timestamp_t watermark) const -> bool {
  NJUDB_STUDENT_TODO(l5_bonus, b2);
}

auto VersionChain::ResolveVisibility(timestamp_t read_ts, txn_id_t reader_txn_id) const -> VersionVisibilityResult {
  NJUDB_STUDENT_TODO(l5_bonus, b2);
}

auto VersionChain::GetHead() const -> UndoRecord* {
  std::shared_lock<std::shared_mutex> lock(latch_);
  return head_;
}

auto VersionChain::GetVersionCount() const -> size_t {
  std::shared_lock<std::shared_mutex> lock(latch_);
  return version_count_;
}

auto VersionChain::NeedsGC() const -> bool {
  std::shared_lock<std::shared_mutex> lock(latch_);
  return version_count_ > GC_THRESHOLD;
}

auto VersionChain::PruneVersions(timestamp_t min_read_ts) -> size_t {
  NJUDB_STUDENT_TODO(l5_bonus, b2);
}

}  // namespace njudb
