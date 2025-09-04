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

/**
 * @brief Join the two ordered tables by sort-merge join
 *
 */

#ifndef NJUDB_EXECUTOR_JOIN_SORTMERGE_H
#define NJUDB_EXECUTOR_JOIN_SORTMERGE_H

#include "executor_join.h"
#include "common/types.h"

namespace njudb {
class SortMergeJoinExecutor : public JoinExecutor
{
public:
  SortMergeJoinExecutor(JoinType join_type, AbstractExecutorUptr left, AbstractExecutorUptr right,
      RecordSchemaUptr left_key_schema, RecordSchemaUptr right_key_schema, CompOp join_op = OP_EQ);

private:
  void InitInnerJoin() override;

  void NextInnerJoin() override;

  [[nodiscard]] auto IsEndInnerJoin() const -> bool override;

  void InitOuterJoin() override;

  void NextOuterJoin() override;

  [[nodiscard]] auto IsEndOuterJoin() const -> bool override;

  /**
   * lexical compare used in equality join
   */
  [[nodiscard]] auto Compare(const Record &left, const Record &right) const -> int;

  // Tip: you can define these Helper methods for different join types
  // void InitInequalityJoinLessFamily();
  // void InitInequalityJoinGreaterFamily();
  // void NextInequalityJoinLessFamily();
  // void NextInequalityJoinGreaterFamily();
  // void InitEqualityJoin();
  // void NextEqualityJoin();

private:
  RecordSchemaUptr left_key_schema_;
  RecordSchemaUptr right_key_schema_;
  CompOp           join_op_;  // Join operation type (OP_EQ, OP_LT, OP_GT, OP_LE, OP_GE)

  // temporarily store record from the left executor
  RecordUptr left_rec_;
  // buffer to store matching values in right executor (for all join types)
  size_t right_idx_{0};
  // TODO: this right buffer can be optimized into an outer memory structure
  std::vector<std::shared_ptr<Record>> right_buffer_;
  // for outer join, indicates whether a valid right value is found, if not, we need to generate all null values for the
  // right.
  bool need_gen_null_{false};

  // For inequality joins: track right iterator position
  RecordUptr right_rec_;
  bool       right_exhausted_{false};
};
}  // namespace njudb

#endif  // NJUDB_EXECUTOR_JOIN_SORTMERGE_H
