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
// Created by ziqi on 2024/8/5.
//

/**
 * @brief scan the table using the index range
 * 
 */

#ifndef WSDB_EXECUTOR_IDXSCAN_H
#define WSDB_EXECUTOR_IDXSCAN_H

#include "executor_abstract.h"
#include "system/handle/index_handle.h"
#include "system/handle/table_handle.h"
#include "common/condition.h"

namespace wsdb {
class IdxScanExecutor : public AbstractExecutor
{
public:
  IdxScanExecutor(TableHandle *tbl, IndexHandle *idx, ConditionVec conds, int cmp_field_num);

  void Init() override;

  void Next() override;

  [[nodiscard]] auto IsEnd() const -> bool override;

private:
  /// Index scan should find all the records in the range [low, high),
  /// where the comparison is based on the first cmp_field_num fields.
  /// low, high should be generated using conds. Both the schema of
  /// record low and record high are the same as the index key
  /// schema. Store the record fetched from the table handle with the
  /// indexed rid into AbstractExecutor::record_. Remove [[maybe_unused]]
  /// when you implement this executor
  TableHandle *tbl_;            // table handle
  IndexHandle *idx_;            // index handle
  ConditionVec conds_;          // conditions
  RecordUptr   low_;            // low key
  RecordUptr   high_;           // high key
  int          cmp_field_num_;  // number of field to be compared from the 0th field
};
}  // namespace wsdb

#endif  // WSDB_EXECUTOR_IDXSCAN_H
