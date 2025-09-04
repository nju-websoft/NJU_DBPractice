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

#ifndef NJUDB_EXECUTOR_H
#define NJUDB_EXECUTOR_H

#include "plan/plan.h"
#include "executor_abstract.h"
#include "system/context.h"

namespace njudb {
class Executor
{
public:
  Executor() = default;

  auto Translate(const std::shared_ptr<AbstractPlan> &plan, DatabaseHandle *db) -> AbstractExecutorUptr;

  void Execute(const AbstractExecutorUptr &executor, Context *ctx);
};
}  // namespace njudb

#endif  // NJUDB_EXECUTOR_H
