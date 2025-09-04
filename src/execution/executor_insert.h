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

#include "executor_abstract.h"
#include "system/handle/table_handle.h"
#include "system/handle/index_handle.h"

#ifndef NJUDB_EXECUTOR_INSERT_H
#define NJUDB_EXECUTOR_INSERT_H

namespace njudb {
class InsertExecutor : public AbstractExecutor
{
public:
  InsertExecutor(TableHandle *tbl, std::list<IndexHandle *> indexes, std::vector<RecordUptr> inserts);

  void Init() override;

  void Next() override;

  [[nodiscard]] auto IsEnd() const -> bool override;

private:
  TableHandle             *tbl_;
  std::list<IndexHandle *> indexes_;
  std::vector<RecordUptr>  inserts_;
  bool                     is_end_;
};
}  // namespace njudb

#endif  // NJUDB_EXECUTOR_INSERT_H
