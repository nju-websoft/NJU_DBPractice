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

#include "concurrency/timestamp_oracle.h"
#include "../../common/error.h"
#include <algorithm>

namespace njudb {

auto TimestampOracle::GetNextTimestamp() -> timestamp_t {
  NJUDB_STUDENT_TODO(l5_bonus, b1);
}

auto TimestampOracle::GetCurrentTimestamp() const -> timestamp_t {
  NJUDB_STUDENT_TODO(l5_bonus, b1);
}

void TimestampOracle::RegisterTransaction(timestamp_t read_ts) {
  NJUDB_STUDENT_TODO(l5_bonus, b1);
}

void TimestampOracle::UnregisterTransaction(timestamp_t read_ts) {
  NJUDB_STUDENT_TODO(l5_bonus, b1);
}

auto TimestampOracle::GetWatermark() const -> timestamp_t {
  NJUDB_STUDENT_TODO(l5_bonus, b1);
}

auto TimestampOracle::GetActiveTransactionCount() const -> size_t {
  NJUDB_STUDENT_TODO(l5_bonus, b1);
}

}  // namespace njudb
