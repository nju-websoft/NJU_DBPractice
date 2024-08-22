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

#include "executor_idxscan.h"

namespace wsdb {

IdxScanExecutor::IdxScanExecutor(TableHandle *tbl, IndexHandle *idx, ConditionVec conds, int cmp_field_num)
    : AbstractExecutor(Basic), tbl_(tbl), idx_(idx), conds_(std::move(conds)), cmp_field_num_(cmp_field_num)
{
  // TODO: generate low key and high key using conds, conds has been rearranged to match the index key prefix
}
// TODO(ziqi): implement the following functions to support index scan
void IdxScanExecutor::Init() {}
void IdxScanExecutor::Next() {}
auto IdxScanExecutor::IsEnd() const -> bool { return false; }

}  // namespace wsdb