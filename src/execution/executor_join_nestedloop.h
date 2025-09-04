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

/**
 * @brief Make a nested loop join between two tables, for outer join, the left table is the outer table
 * 
 */

#ifndef NJUDB_EXECUTOR_JOIN_NESTEDLOOP_H
#define NJUDB_EXECUTOR_JOIN_NESTEDLOOP_H

#include "executor_join.h"

namespace njudb {

class NestedLoopJoinExecutor : public JoinExecutor
{
public:
  NestedLoopJoinExecutor(
      JoinType join_type, AbstractExecutorUptr left, AbstractExecutorUptr right, ConditionVec conditions);

private:
  void InitInnerJoin() override;

  void NextInnerJoin() override;

  [[nodiscard]] auto IsEndInnerJoin() const -> bool override;

  void InitOuterJoin() override;

  void NextOuterJoin() override;

  [[nodiscard]] auto IsEndOuterJoin() const -> bool override;

private:
  RecordUptr left_rec_ = nullptr;
  // for outer join, indicates whether a valid right value is found
  bool need_gen_null_{false};
};

}  // namespace njudb

#endif  // NJUDB_EXECUTOR_JOIN_NESTEDLOOP_H
