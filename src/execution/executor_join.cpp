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
#include "executor_join.h"

#include <utility>
namespace njudb {
JoinExecutor::JoinExecutor(
    JoinType join_type, AbstractExecutorUptr left, AbstractExecutorUptr right, ConditionVec conditions)
    : AbstractExecutor(Basic),
      join_type_(join_type),
      left_(std::move(left)),
      right_(std::move(right)),
      conditions_(std::move(conditions))
{
  // init the schema
  auto                 left_schema  = left_->GetOutSchema();
  auto                 right_schema = right_->GetOutSchema();
  std::vector<RTField> fields;
  fields.reserve(left_schema->GetFieldCount() + right_schema->GetFieldCount());
  for (size_t i = 0; i < left_schema->GetFieldCount(); ++i) {
    fields.push_back(left_schema->GetFieldAt(i));
  }
  for (size_t i = 0; i < right_schema->GetFieldCount(); ++i) {
    fields.push_back(right_schema->GetFieldAt(i));
  }
  out_schema_ = std::make_unique<RecordSchema>(fields);
}

void JoinExecutor::Init()
{
  switch (join_type_) {
    case INNER_JOIN: return InitInnerJoin();
    case OUTER_JOIN: return InitOuterJoin();
    default: NJUDB_FATAL("Unknown join type");
  }
}

void JoinExecutor::Next()
{
  switch (join_type_) {
    case INNER_JOIN: return NextInnerJoin();
    case OUTER_JOIN: return NextOuterJoin();
    default: NJUDB_FATAL("Unknown join type");
  }
}

auto JoinExecutor::IsEnd() const -> bool
{
  switch (join_type_) {
    case INNER_JOIN: return IsEndInnerJoin();
    case OUTER_JOIN: return IsEndOuterJoin();
    default: NJUDB_FATAL("Unknown join type");
  }
}

}  // namespace njudb