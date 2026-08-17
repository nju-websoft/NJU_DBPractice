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

#include "concurrency/undo_record.h"

namespace njudb {

//===--------------------------------------------------------------------===//
// UndoRecord Implementation (TODO: Complete these methods)
//===--------------------------------------------------------------------===//

UndoRecord::UndoRecord(txn_id_t txn_id, const RID &rid, RecordUptr old_tuple, timestamp_t ts_begin)
  : txn_id_(txn_id),
    rid_(rid),
    tuple_data_(std::move(old_tuple)),
    ts_begin_(ts_begin),
    ts_commit_(MAX_TS),
    prev_(nullptr),
    is_deleted_(false) {
  // Constructor implementation
}

void UndoRecord::Commit(timestamp_t commit_ts) {
  // Mark version as committed with the given timestamp
  // This makes the version visible to transactions with read_ts >= commit_ts
  ts_commit_ = commit_ts;
}

void UndoRecord::Abort() {
  // Mark version as aborted using special ABORTED_TS marker
  // This ensures version is never visible (IsVisible always returns false)
  ts_commit_ = ABORTED_TS;
}

}  // namespace njudb
