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
// Created by ziqi on 2024/8/7.
//

#include "executor_join_sortmerge.h"

namespace wsdb {
SortMergeJoinExecutor::SortMergeJoinExecutor(JoinType join_type, AbstractExecutorUptr left, AbstractExecutorUptr right,
    RecordSchemaUptr left_key_schema, RecordSchemaUptr right_key_schema)
    // condition vec is not used in sort merge join, it has been converted to key schemas
    : JoinExecutor(join_type, std::move(left), std::move(right), {}),
      left_key_schema_(std::move(left_key_schema)),
      right_key_schema_(std::move(right_key_schema))
{}

auto SortMergeJoinExecutor::Compare(const wsdb::Record &left, const wsdb::Record &right) const -> int
{
  auto left_key  = std::make_unique<Record>(left_key_schema_.get(), left);
  auto right_key = std::make_unique<Record>(right_key_schema_.get(), right);
  return Record::Compare(*left_key, *right_key);
}

void SortMergeJoinExecutor::InitInnerJoin() { WSDB_STUDENT_TODO(l3, f1); }

void SortMergeJoinExecutor::NextInnerJoin() { WSDB_STUDENT_TODO(l3, f1); }

auto SortMergeJoinExecutor::IsEndInnerJoin() const -> bool { WSDB_STUDENT_TODO(l3, f1); }

void SortMergeJoinExecutor::InitOuterJoin() {WSDB_THROW(WSDB_NOT_IMPLEMENTED, "");}

void SortMergeJoinExecutor::NextOuterJoin() {WSDB_THROW(WSDB_NOT_IMPLEMENTED, "");}

auto SortMergeJoinExecutor::IsEndOuterJoin() const -> bool { return false; }
}  // namespace wsdb