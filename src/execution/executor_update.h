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
 * @brief set the fields in the records extracted from the child executor to the new values and update the tables and indexes
 */

#ifndef NJUDB_EXECUTOR_UPDATE_H
#define NJUDB_EXECUTOR_UPDATE_H

#include "executor_abstract.h"
#include "system/handle/table_handle.h"
#include "system/handle/index_handle.h"

namespace njudb {
class UpdateExecutor : public AbstractExecutor
{
public:
  UpdateExecutor(AbstractExecutorUptr child, TableHandle *tbl, std::list<IndexHandle *> indexes,
      std::vector<std::pair<RTField, ValueSptr>> updates);

  void Init() override;

  void Next() override;

  [[nodiscard]] auto IsEnd() const -> bool override;

private:
  AbstractExecutorUptr                       child_;
  TableHandle                               *tbl_;
  std::list<IndexHandle *>                   indexes_;
  std::vector<std::pair<RTField, ValueSptr>> updates_;
  std::vector<std::pair<std::unique_ptr<Record>, std::unique_ptr<Record>>> updates_to_perform_;
  bool                                       is_end_;
};
}  // namespace njudb

#endif  // NJUDB_EXECUTOR_UPDATE_H
