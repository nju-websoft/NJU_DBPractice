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
// Created by ziqi on 2024/8/7.
//

#ifndef WSDB_OPTIMIZER_H
#define WSDB_OPTIMIZER_H
#include "plan/plan.h"
#include "system/handle/database_handle.h"

namespace wsdb {
class Optimizer
{
public:
  Optimizer()  = default;
  ~Optimizer() = default;

  static auto Optimize(std::shared_ptr<AbstractPlan> plan, DatabaseHandle *db) -> std::shared_ptr<AbstractPlan>;

private:
  static auto LogicalOptimize(std::shared_ptr<AbstractPlan> plan, DatabaseHandle *db) -> std::shared_ptr<AbstractPlan>;

  static auto LogicalOptimizeScan(const std::shared_ptr<ScanPlan> &scan, ConditionVec conds,
      wsdb::DatabaseHandle *db) -> std::shared_ptr<AbstractPlan>;

  static auto LogicalOptimizeJoin(std::shared_ptr<JoinPlan> join) -> std::shared_ptr<AbstractPlan>;

  static auto PhysicalOptimize(std::shared_ptr<AbstractPlan> plan, DatabaseHandle *db) -> std::shared_ptr<AbstractPlan>;

  /**
   * check if there is an index that can be used to scan the table,
   * and return the index with the most matched fields, should store
   * the rearranged conditions in index_conds and erase them from conds
   * @param conds
   * @param index_conds
   * @param indexes
   * @param max_matched_fields
   * @return
   */
  static auto CanIndexScan(ConditionVec &conds, ConditionVec &index_conds, const std::list<IndexHandle *> &indexes,
      size_t &max_matched_fields) -> IndexHandle *;
};
}  // namespace wsdb

#endif  // WSDB_OPTIMIZER_H
