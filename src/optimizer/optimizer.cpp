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

#include "optimizer.h"
namespace wsdb {
auto Optimizer::Optimize(std::shared_ptr<AbstractPlan> plan, DatabaseHandle *db) -> std::shared_ptr<AbstractPlan>
{
  plan = LogicalOptimize(plan, db);
  plan = PhysicalOptimize(plan, db);
  return plan;
}

auto Optimizer::LogicalOptimize(std::shared_ptr<AbstractPlan> plan, DatabaseHandle *db) -> std::shared_ptr<AbstractPlan>
{
  if (auto upd = std::dynamic_pointer_cast<UpdatePlan>(plan)) {
    upd->child_ = LogicalOptimize(upd->child_, db);
    return upd;
  } else if (auto del = std::dynamic_pointer_cast<DeletePlan>(plan)) {
    del->child_ = LogicalOptimize(del->child_, db);
    return del;
  } else if (auto filter = std::dynamic_pointer_cast<FilterPlan>(plan)) {
    if (auto scan = std::dynamic_pointer_cast<ScanPlan>(filter->child_)) {
      filter->child_ = LogicalOptimizeScan(scan, filter->conds_, db);
    } else {
      filter->child_ = LogicalOptimize(filter->child_, db);
    }
    return filter;
  } else if (auto sort = std::dynamic_pointer_cast<SortPlan>(plan)) {
    sort->child_ = LogicalOptimize(sort->child_, db);
    return sort;
  } else if (auto proj = std::dynamic_pointer_cast<ProjectPlan>(plan)) {
    proj->child_ = LogicalOptimize(proj->child_, db);
    return proj;
  } else if (auto join = std::dynamic_pointer_cast<JoinPlan>(plan)) {
    join->left_  = LogicalOptimize(join->left_, db);
    join->right_ = LogicalOptimize(join->right_, db);
    return LogicalOptimizeJoin(join);
  } else if (auto agg = std::dynamic_pointer_cast<AggregatePlan>(plan)) {
    agg->child_ = LogicalOptimize(agg->child_, db);
    return agg;
  } else if (auto lim = std::dynamic_pointer_cast<LimitPlan>(plan)) {
    lim->child_ = LogicalOptimize(lim->child_, db);
    return lim;
  }
  return plan;
}

auto Optimizer::LogicalOptimizeScan(const std::shared_ptr<ScanPlan> &scan, ConditionVec conds,
    wsdb::DatabaseHandle *db) -> std::shared_ptr<AbstractPlan>
{
  // try to make index scan
  size_t       max_matched_fields = 0;
  ConditionVec index_conds;
  auto         index = CanIndexScan(conds, index_conds, db->GetIndexes(scan->table_name_), max_matched_fields);
  std::shared_ptr<AbstractPlan> new_scan = scan;
  if (index != nullptr) {
    new_scan = std::make_shared<IdxScanPlan>(scan->table_name_, index->GetIndexId(), index_conds, max_matched_fields);
  }
  return new_scan;
}

auto Optimizer::LogicalOptimizeJoin(std::shared_ptr<JoinPlan> join) -> std::shared_ptr<AbstractPlan>
{
  if (join->strategy_ == NESTED_LOOP) {
    return join;
  }
  WSDB_ASSERT(join->strategy_ == SORT_MERGE, "Unknown join strategy");
  // try to generate SortMergeJoin
  // check if all conditions are equality comparison
  auto all_eq =
      std::all_of(join->conds_.begin(), join->conds_.end(), [](const auto &cond) { return cond.GetOp() == OP_EQ; });
  if (!all_eq) {
    join->strategy_ = NESTED_LOOP;
    return join;
  }
  // generate key schema from conditions
  std::vector<RTField> left_key_fields;
  std::vector<RTField> right_key_fields;
  const auto          &conditions = join->conds_;
  left_key_fields.reserve(conditions.size());
  right_key_fields.reserve(conditions.size());
  for (const auto &cond : conditions) {
    left_key_fields.push_back(cond.GetLCol());
    right_key_fields.push_back(cond.GetRCol());
  }
  std::shared_ptr<AbstractPlan> left = std::dynamic_pointer_cast<IdxScanPlan>(join->left_);
  // generate sort plan
  if (left == nullptr) {
    left = std::make_shared<SortPlan>(std::move(join->left_), std::make_unique<RecordSchema>(left_key_fields), false);
  }
  std::shared_ptr<AbstractPlan> right = std::dynamic_pointer_cast<IdxScanPlan>(join->right_);
  if (right == nullptr) {
    right =
        std::make_shared<SortPlan>(std::move(join->right_), std::make_unique<RecordSchema>(right_key_fields), false);
  }
  join->left_             = left;
  join->right_            = right;
  join->left_key_schema_  = std::make_unique<RecordSchema>(left_key_fields);
  join->right_key_schema_ = std::make_unique<RecordSchema>(right_key_fields);
  return join;
}

auto Optimizer::PhysicalOptimize(
    std::shared_ptr<AbstractPlan> plan, DatabaseHandle *db) -> std::shared_ptr<AbstractPlan>
{
  return plan;
}

auto Optimizer::CanIndexScan(ConditionVec &conds, ConditionVec &index_conds, const std::list<IndexHandle *> &indexes,
    size_t &max_matched_fields) -> IndexHandle *
{
  std::vector<int> best_conds_pos;
  max_matched_fields      = 0;
  IndexHandle *best_index = nullptr;
  for (const auto idx : indexes) {
    std::vector<int> tmp_conds_pos;
    for (const auto &field : idx->GetKeySchema().GetFields()) {
      bool matched = false;
      for (int i = 0; i < static_cast<int>(conds.size()); ++i) {
        auto       &cond = conds[i];
        const auto &lcol = cond.GetLCol();
        if (lcol.field_.table_id_ == field.field_.table_id_ && lcol.field_.field_name_ == field.field_.field_name_) {
          matched = true;
          if (cond.GetOp() != OP_EQ) {
            matched = false;
          }
          tmp_conds_pos.push_back(i);
          break;
        }
      }
      if (!matched) {
        break;
      }
    }
    if (tmp_conds_pos.size() > best_conds_pos.size()) {
      best_conds_pos = tmp_conds_pos;
      best_index     = idx;
    }
  }
  max_matched_fields = best_conds_pos.size();
  if (max_matched_fields == 0) {
    return nullptr;
  }
  index_conds.clear();
  // add index conds;
  for (auto pos : best_conds_pos) {
    index_conds.push_back(conds[pos]);
  }
  // erase index conds from conds
  for (auto pos : best_conds_pos) {
    conds.erase(conds.begin() + pos);
  }
  return best_index;
}
}  // namespace wsdb
