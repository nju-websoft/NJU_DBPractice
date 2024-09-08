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
 * @brief delete the records returned by the child executor
 * should delete the records both in the table and the indexes
 * 
 */

#ifndef WSDB_EXECUTOR_DELETE_H
#define WSDB_EXECUTOR_DELETE_H

#include "executor_abstract.h"
#include "system/handle/database_handle.h"

namespace wsdb {
class DeleteExecutor : public AbstractExecutor
{
public:
  DeleteExecutor(AbstractExecutorUptr child, TableHandle *tbl, std::list<IndexHandle *> indexes);

  void Init() override;

  void Next() override;

  [[nodiscard]] auto IsEnd() const -> bool override;

private:
  AbstractExecutorUptr     child_;
  TableHandle             *tbl_;
  std::list<IndexHandle *> indexes_;
  bool                     is_end_;
};
}  // namespace wsdb

#endif  // WSDB_EXECUTOR_DELETE_H
