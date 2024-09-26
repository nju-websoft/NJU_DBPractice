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
// Created by ziqi on 2024/8/1.
//

#ifndef WSDB_CONDITION_H
#define WSDB_CONDITION_H

#include <utility>

#include "value.h"
#include "types.h"
#include "meta.h"

namespace wsdb {

enum CondRvalType
{
  kNone = 0,
  kValue,
  kColumn,
  kSubquery
};

static auto CondRvalTypeToString(CondRvalType type) -> std::string
{
  switch (type) {
    case kNone: return "None";
    case kValue: return "Value";
    case kColumn: return "Column";
    case kSubquery: return "Subquery";
    default: return "Unknown";
  }
  return "Unknown";
}

class Condition;
using ConditionVec = std::vector<Condition>;

class Condition
{
public:
  Condition() = default;

  Condition(const Condition &other) = default;

  Condition(CompOp op, RTField l_col, ValueSptr &r_val)
      : rval_type_(kValue), l_col_(std::move(l_col)), r_val_(r_val), op_(op)
  {}

  Condition(CompOp op, RTField l_col, RTField r_col)
      : rval_type_(kColumn), l_col_(std::move(l_col)), r_col_(std::move(r_col)), op_(op)
  {}

  Condition(CompOp op, RTField l_col, int32_t subquery_id)
      : rval_type_(kSubquery), l_col_(std::move(l_col)), op_(op), subquery_id_(subquery_id)
  {}

  auto operator=(const Condition &other) -> Condition & = default;

  [[nodiscard]] auto GetLCol() const -> const RTField & { return l_col_; }

  [[nodiscard]] auto GetRhsType() const -> CondRvalType { return rval_type_; }

  [[nodiscard]] auto GetRCol() const -> const RTField &
  {
    WSDB_ASSERT(rval_type_ == kColumn, fmt::format("should be: {}", CondRvalTypeToString(rval_type_)));
    return r_col_;
  }

  [[nodiscard]] auto GetRVal() const -> ValueSptr
  {
    WSDB_ASSERT(rval_type_ == kValue, fmt::format("should be: {}", CondRvalTypeToString(rval_type_)));
    return r_val_;
  }

  [[nodiscard]] auto GetOp() const -> CompOp { return op_; }

  [[nodiscard]] auto GetSubqueryId() const -> int32_t
  {
    WSDB_ASSERT(rval_type_ == kSubquery, "should be subquery");
    return subquery_id_;
  }

  auto ToString() const -> std::string
  {
    std::string str;
    str += fmt::format("{} {} ", l_col_.ToString(), CompOpToString(op_));
    switch (rval_type_) {
      case kValue: str += r_val_->ToString(); break;
      case kColumn: str += r_col_.ToString(); break;
      case kSubquery: str += fmt::format("subquery {}", subquery_id_); break;
      default: break;
    }
    return str;
  }

private:
  CondRvalType rval_type_{kNone};
  RTField      l_col_{};
  RTField      r_col_{};
  ValueSptr    r_val_{nullptr};
  CompOp       op_{};
  int32_t      subquery_id_{-1};
};

}  // namespace wsdb

#endif  // WSDB_CONDITION_H
