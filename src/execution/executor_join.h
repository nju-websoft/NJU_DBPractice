
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
 * @brief abstract class for join executor
 *
 */

#ifndef NJUDB_EXECUTOR_JOIN_H
#define NJUDB_EXECUTOR_JOIN_H

#include "executor_abstract.h"
#include "common/condition.h"

namespace njudb {
class JoinExecutor : public AbstractExecutor
{
public:
  JoinExecutor(JoinType join_type, AbstractExecutorUptr left, AbstractExecutorUptr right, ConditionVec conditions);

  void Init() override;

  void Next() override;

  [[nodiscard]] auto IsEnd() const -> bool override;

protected:
  virtual void InitInnerJoin() = 0;

  virtual void NextInnerJoin() = 0;

  [[nodiscard]] virtual auto IsEndInnerJoin() const -> bool = 0;

  virtual void InitOuterJoin() = 0;

  virtual void NextOuterJoin() = 0;

  [[nodiscard]] virtual auto IsEndOuterJoin() const -> bool = 0;

protected:
  JoinType             join_type_;
  AbstractExecutorUptr left_;
  AbstractExecutorUptr right_;
  ConditionVec         conditions_;
};

}  // namespace njudb

#endif  // NJUDB_EXECUTOR_JOIN_H
