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

#include "condition_expr.h"

namespace njudb {

auto ConditionExpr::Eval(const ConditionVec &condition, const njudb::Record &record) -> bool
{
  return std::all_of(
      condition.begin(), condition.end(), [&record](const Condition &cond) { return EvalCond(cond, record); });
}

auto ConditionExpr::EvalCond(const Condition &condition, const njudb::Record &record) -> bool
{
  // first get the lhs value according to condition
  auto idx = record.GetSchema()->GetRTFieldIndex(condition.GetLCol());
  NJUDB_ASSERT(idx != record.GetSchema()->GetFieldCount(), "Invalid field");
  auto lhs = record.GetValueAt(idx);
  NJUDB_ASSERT(condition.GetRhsType() == kValue || condition.GetRhsType() == kColumn, "Invalid condition type");
  ValueSptr rhs;
  if (condition.GetRhsType() == kValue) {
    rhs = condition.GetRVal();
  } else {
    idx = record.GetSchema()->GetRTFieldIndex(condition.GetRCol());
    NJUDB_ASSERT(idx != record.GetSchema()->GetFieldCount(), "Invalid field");
    rhs = record.GetValueAt(idx);
  }
  ValueFactory::AlignTypes(lhs, rhs);
  switch (condition.GetOp()) {
    case OP_EQ: return *lhs == *rhs;
    case OP_NE: return *lhs != *rhs;
    case OP_LT: return *lhs < *rhs;
    case OP_LE: return *lhs <= *rhs;
    case OP_GT: return *lhs > *rhs;
    case OP_GE: return *lhs >= *rhs;
    case OP_IN: return std::dynamic_pointer_cast<ArrayValue>(rhs)->Contains(lhs);
    default: NJUDB_FATAL(CompOpToString(condition.GetOp()));
  }
  // should never reach here
}

}  // namespace njudb