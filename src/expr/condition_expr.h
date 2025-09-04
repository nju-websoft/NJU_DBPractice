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

#ifndef NJUDB_CONDITION_EXPR_H
#define NJUDB_CONDITION_EXPR_H

#include "common/condition.h"
#include "common/record.h"

namespace njudb {

class ConditionExpr
{
public:
  ConditionExpr() = delete;
  DISABLE_COPY_MOVE_AND_ASSIGN(ConditionExpr);

  static auto Eval(const ConditionVec &condition, const Record &record)-> bool;

private:
  static auto EvalCond(const Condition &condition, const Record &record) -> bool;
};

}  // namespace njudb

#endif  // NJUDB_CONDITION_EXPR_H
