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
// Created by ziqi on 2024/7/18.
//

#include "executor_update.h"
#include "concurrency/lock_manager.h"
#include "concurrency/version_manager.h"
#include "concurrency/concurrency_protocol.h"
#include "mvcc_write_coordinator.h"
#include <fmt/format.h>

namespace njudb {

UpdateExecutor::UpdateExecutor(AbstractExecutorUptr child, TableHandle *tbl, std::list<IndexHandle *> indexes,
    std::vector<std::pair<RTField, ValueSptr>> updates, Context *context)
    : AbstractExecutor(DML),
      child_(std::move(child)),
      tbl_(tbl),
      indexes_(std::move(indexes)),
      updates_(std::move(updates)),
      context_(context),
      is_end_(false)
{
  std::vector<RTField> fields(1);
  fields[0]   = RTField{.field_ = {.field_name_ = "updated", .field_size_ = sizeof(int), .field_type_ = TYPE_INT}};
  out_schema_ = std::make_unique<RecordSchema>(fields);
}

void UpdateExecutor::Init()
{
  // Lab 02 may skip Lab 04's all-or-nothing unique-index validation.
  // 1. Build old/new record pairs.
  // 2. Validate the complete batch. NJUDB_STUDENT_TODO(l4, t2)
  // 3. Apply table and index updates atomically.
  NJUDB_STUDENT_TODO(l2, t1);
}

void UpdateExecutor::Next() { is_end_ = true; }

auto UpdateExecutor::IsEnd() const -> bool { return is_end_; }

}  // namespace njudb
