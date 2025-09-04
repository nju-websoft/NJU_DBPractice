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
// Created by ziqi on 2024/8/9.
//

#include "executor_join_hash.h"
#include "expr/condition_expr.h"
#include "common/bloom_filter.h"
#include <functional>

namespace njudb {

// ===== HashJoinExecutor Implementation =====

HashJoinExecutor::HashJoinExecutor(JoinType join_type, AbstractExecutorUptr left, AbstractExecutorUptr right,
                                   RecordSchemaUptr left_key_schema, RecordSchemaUptr right_key_schema, ConditionVec conditions, bool use_bloom_filter)
    : JoinExecutor(join_type, std::move(left), std::move(right), std::move(conditions)),
      left_key_schema_(std::move(left_key_schema)),
      right_key_schema_(std::move(right_key_schema)),
      use_bloom_filter_(use_bloom_filter),
      is_probing_(false),
      current_left_has_match_(false),
      need_output_null_match_(false),
      build_records_(0),
      probe_records_(0)
{
  if (use_bloom_filter_) {
    bloom_filter_ = std::make_unique<BloomFilter>(8192, 3);
  }
}


void HashJoinExecutor::BuildHashTable()
{
  NJUDB_STUDENT_TODO(l3, f1);
}

void HashJoinExecutor::InitInnerJoin()
{
  NJUDB_STUDENT_TODO(l3, f1);
}

void HashJoinExecutor::NextInnerJoin()
{
  NJUDB_STUDENT_TODO(l3, f1);
}

auto HashJoinExecutor::IsEndInnerJoin() const -> bool
{
  NJUDB_STUDENT_TODO(l3, f1);
}

void HashJoinExecutor::InitOuterJoin()
{
  NJUDB_STUDENT_TODO(l3, f1);
}

void HashJoinExecutor::NextOuterJoin()
{
  NJUDB_STUDENT_TODO(l3, f1);
}

auto HashJoinExecutor::IsEndOuterJoin() const -> bool
{
  NJUDB_STUDENT_TODO(l3, f1);
}

}  // namespace njudb
