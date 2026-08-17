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

#include "concurrency/concurrency_controller.h"

namespace njudb {

// TODO: Implement ConcurrencyController methods
//
// Read operations:
// - ReadTuple(): Route to ReadTuple2PL() or ReadTupleMVCC()
// - ScanTable(): Acquire appropriate locks or set read timestamp
//
// Write operations:
// - UpdateTuple(): Acquire X lock, create version if MVCC
// - InsertTuple(): Acquire X lock, create first version if MVCC
// - DeleteTuple(): Acquire X lock, create delete marker if MVCC
//
// Transaction lifecycle:
// - CommitTransaction(): Release locks (2PL) or validate and commit versions (MVCC)
// - AbortTransaction(): Rollback write set, release locks, mark versions ABORTED
//
// 2PL-specific routing:
// - ReadTuple2PL(): Acquire S lock based on isolation level
// - UpdateTuple2PL(): Acquire IX on table, X on tuple
// - CommitTransaction2PL(): Transition to SHRINKING, release locks
//
// MVCC-specific routing:
// - ReadTupleMVCC(): Find visible version using read timestamp
// - UpdateTupleMVCC(): Create new version, link to chain
// - CommitTransactionMVCC(): Validate read set, assign commit_ts, mark versions COMMITTED
//
// Helper methods:
// - ValidateReadSet(): Check if any read tuple updated by concurrent txn

}  // namespace njudb
