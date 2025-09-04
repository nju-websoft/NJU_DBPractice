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

#ifndef NJUDB_OPTIMIZER_H
#define NJUDB_OPTIMIZER_H
#include "plan/plan.h"
#include "system/handle/database_handle.h"

namespace njudb {
class Optimizer
{
public:
  Optimizer()  = default;
  ~Optimizer() = default;

  auto Optimize(std::shared_ptr<AbstractPlan> plan, DatabaseHandle *db) -> std::shared_ptr<AbstractPlan>;

private:
  auto LogicalOptimize(std::shared_ptr<AbstractPlan> plan, DatabaseHandle *db) -> std::shared_ptr<AbstractPlan>;

  /**
   * Physical optimization, which includes:
   * - Reordering joins
   * - Using indexes for scans
   * - Eliminating unnecessary sorts
   * - Choosing join strategies (nested loop, hash join, sort merge)
   * @param plan
   * @param db
   * @return optimized plan
   */
  auto PhysicalOptimize(std::shared_ptr<AbstractPlan> plan, DatabaseHandle *db) -> std::shared_ptr<AbstractPlan>;

  auto PhysicalOptimizeScan(const std::shared_ptr<ScanPlan> &scan, ConditionVec &conds,
      njudb::DatabaseHandle *db) -> std::shared_ptr<AbstractPlan>;

  auto PhysicalOptimizeJoin(std::shared_ptr<JoinPlan> join, DatabaseHandle *db) -> std::shared_ptr<AbstractPlan>;


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
  auto CanIndexScan(ConditionVec &conds, ConditionVec &index_conds, const std::list<IndexHandle *> &indexes,
      size_t &max_matched_fields) -> IndexHandle *;

  /**
   * Try to eliminate sort by using index scan for simple table scan
   * @param sort
   * @param scan
   * @param db
   * @return
   */
  auto TryEliminateSortWithIndex(std::shared_ptr<SortPlan> sort, std::shared_ptr<ScanPlan> scan, 
      DatabaseHandle *db) -> std::shared_ptr<AbstractPlan>;

  /**
   * Try to eliminate sort by using index scan for filtered table scan
   * @param sort
   * @param filter
   * @param scan
   * @param db
   * @return
   */
  auto TryEliminateSortWithIndexAndFilter(std::shared_ptr<SortPlan> sort, std::shared_ptr<FilterPlan> filter,
      std::shared_ptr<ScanPlan> scan, DatabaseHandle *db) -> std::shared_ptr<AbstractPlan>;

  /**
   * Check if an index can be used for ORDER BY
   * @param order_schema
   * @param index_schema
   * @param is_desc
   * @return
   */
  auto CanUseIndexForOrderBy(const RecordSchema *order_schema, const RecordSchema *index_schema, 
      IndexType index_type, bool is_desc) -> bool;

  /**
   * Check if conditions are suitable for sort merge join
   * @param conds
   * @return true if all conditions have the same operator and are supported (=, <, <=, >, >=)
   */
  auto CanUseSortMergeJoin(const ConditionVec &conds) -> bool;

  /**
   * Check if conditions are suitable for hash join
   * @param conds
   * @return true if all conditions are equality comparisons
   */
  auto CanUseHashJoin(const ConditionVec &conds) -> bool;

  /**
   * Get the required ordering for sort merge join based on join operator
   * @param join_op
   * @param left_desc [out] whether left side should be descending
   * @param right_desc [out] whether right side should be descending
   */
  void GetSortMergeOrdering(CompOp join_op, bool &left_desc, bool &right_desc);

  /**
   * Optimize join using nested loop strategy
   * @param join
   * @param db
   * @return optimized join plan
   */
  auto OptimizeNestedLoopJoin(std::shared_ptr<JoinPlan> join, DatabaseHandle *db) -> std::shared_ptr<AbstractPlan>;

  /**
   * Optimize join using hash join strategy
   * @param join
   * @param db
   * @return optimized join plan
   */
  auto OptimizeHashJoin(std::shared_ptr<JoinPlan> join, DatabaseHandle *db) -> std::shared_ptr<AbstractPlan>;

  /**
   * Optimize join using sort merge strategy
   * @param join
   * @param db
   * @return optimized join plan
   */
  auto OptimizeSortMergeJoin(std::shared_ptr<JoinPlan> join, DatabaseHandle *db) -> std::shared_ptr<AbstractPlan>;
};
}  // namespace njudb

#endif  // NJUDB_OPTIMIZER_H
