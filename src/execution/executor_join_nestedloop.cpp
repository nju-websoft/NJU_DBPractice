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
// Created by ziqi on 2024/8/4.
//

#include "executor_join_nestedloop.h"
#include "expr/condition_expr.h"

namespace njudb {
NestedLoopJoinExecutor::NestedLoopJoinExecutor(
    JoinType join_type, AbstractExecutorUptr left, AbstractExecutorUptr right, ConditionVec conditions)
    : JoinExecutor(join_type, std::move(left), std::move(right), std::move(conditions))
{}

/// inner join
void NestedLoopJoinExecutor::InitInnerJoin()
{
  NJUDB_STUDENT_TODO(l2, f1);
  NJUDB_STUDENT_TODO(l3, t1);
}

void NestedLoopJoinExecutor::NextInnerJoin()
{
  NJUDB_STUDENT_TODO(l2, f1);
  NJUDB_STUDENT_TODO(l3, t1);
}

auto NestedLoopJoinExecutor::IsEndInnerJoin() const -> bool
{
  NJUDB_STUDENT_TODO(l2, f1);
  NJUDB_STUDENT_TODO(l3, t1);
}

/// outer join
void NestedLoopJoinExecutor::InitOuterJoin() { NJUDB_STUDENT_TODO(l3, t2); }

void NestedLoopJoinExecutor::NextOuterJoin() { NJUDB_STUDENT_TODO(l3, t2); }

auto NestedLoopJoinExecutor::IsEndOuterJoin() const -> bool { NJUDB_STUDENT_TODO(l3, t2); }

}  // namespace njudb