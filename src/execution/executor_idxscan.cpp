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

#include "executor_idxscan.h"
#include "common/value.h"
#include "expr/condition_expr.h"
#include <algorithm>

namespace njudb {

IdxScanExecutor::IdxScanExecutor(TableHandle *tbl, IndexHandle *idx, ConditionVec conds, bool is_ascending)
    : AbstractExecutor(Basic), tbl_(tbl), idx_(idx), conds_(std::move(conds)), is_ascending_(is_ascending)
{
  NJUDB_STUDENT_TODO(l4, t2);
}

void IdxScanExecutor::Init() { NJUDB_STUDENT_TODO(l4, t2); }

void IdxScanExecutor::Next() { NJUDB_STUDENT_TODO(l4, t2); }

auto IdxScanExecutor::IsEnd() const -> bool { NJUDB_STUDENT_TODO(l4, t2); }

auto IdxScanExecutor::GetOutSchema() const -> const RecordSchema * { return &tbl_->GetSchema(); }

}  // namespace njudb