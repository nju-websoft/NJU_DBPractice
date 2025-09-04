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
 * @brief Iterate over all records in the table, check TableHandle for more details
 * 
 */

#ifndef NJUDB_EXECUTOR_SEQSCAN_H
#define NJUDB_EXECUTOR_SEQSCAN_H
#include "executor_abstract.h"
#include "system/handle/table_handle.h"

namespace njudb {
class SeqScanExecutor : public AbstractExecutor
{
public:
  explicit SeqScanExecutor(TableHandle *tab);

  void Init() override;

  void Next() override;

  [[nodiscard]] auto IsEnd() const -> bool override;

  [[nodiscard]] auto GetOutSchema() const -> const RecordSchema * override;

private:
  TableHandle *tab_;
  RID          rid_;
};
}  // namespace njudb

#endif  // NJUDB_EXECUTOR_SEQSCAN_H
