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
namespace njudb {
auto Optimizer::Optimize(std::shared_ptr<AbstractPlan> plan, DatabaseHandle *db) -> std::shared_ptr<AbstractPlan>
{
  plan = LogicalOptimize(plan, db);
  plan = PhysicalOptimize(plan, db);
  return plan;
}

auto Optimizer::PhysicalOptimize(std::shared_ptr<AbstractPlan> plan, DatabaseHandle *db)
    -> std::shared_ptr<AbstractPlan>
{
  if (auto upd = std::dynamic_pointer_cast<UpdatePlan>(plan)) {
    upd->child_ = PhysicalOptimize(upd->child_, db);
    return upd;
  } else if (auto del = std::dynamic_pointer_cast<DeletePlan>(plan)) {
    del->child_ = PhysicalOptimize(del->child_, db);
    return del;
  } else if (auto filter = std::dynamic_pointer_cast<FilterPlan>(plan)) {
    if (auto scan = std::dynamic_pointer_cast<ScanPlan>(filter->child_)) {
      filter->child_ = PhysicalOptimizeScan(scan, filter->conds_, db);
    } else {
      filter->child_ = PhysicalOptimize(filter->child_, db);
    }
    // check if filter can be eliminated
    if (filter->conds_.empty()) {
      return filter->child_;
    }
    return filter;
  } else if (auto sort = std::dynamic_pointer_cast<SortPlan>(plan)) {
    sort->child_ = PhysicalOptimize(sort->child_, db);

    // Try to eliminate sort using index scan
    if (auto scan = std::dynamic_pointer_cast<ScanPlan>(sort->child_)) {
      return TryEliminateSortWithIndex(sort, scan, db);
    } else if (auto filter = std::dynamic_pointer_cast<FilterPlan>(sort->child_)) {
      if (auto scan = std::dynamic_pointer_cast<ScanPlan>(filter->child_)) {
        return TryEliminateSortWithIndexAndFilter(sort, filter, scan, db);
      }
    }

    return sort;
  } else if (auto proj = std::dynamic_pointer_cast<ProjectPlan>(plan)) {
    proj->child_ = PhysicalOptimize(proj->child_, db);
    return proj;
  } else if (auto join = std::dynamic_pointer_cast<JoinPlan>(plan)) {
    return PhysicalOptimizeJoin(join, db);
  } else if (auto agg = std::dynamic_pointer_cast<AggregatePlan>(plan)) {
    agg->child_ = PhysicalOptimize(agg->child_, db);
    return agg;
  } else if (auto lim = std::dynamic_pointer_cast<LimitPlan>(plan)) {
    lim->child_ = PhysicalOptimize(lim->child_, db);
    return lim;
  }
  return plan;
}

auto Optimizer::PhysicalOptimizeScan(const std::shared_ptr<ScanPlan> &scan, ConditionVec &conds,
    njudb::DatabaseHandle *db) -> std::shared_ptr<AbstractPlan>
{
  // try to make index scan
  size_t       max_matched_fields = 0;
  ConditionVec index_conds;
  auto         index = CanIndexScan(conds, index_conds, db->GetIndexes(scan->table_name_), max_matched_fields);
  std::shared_ptr<AbstractPlan> new_scan = scan;
  if (index != nullptr) {
    new_scan = std::make_shared<IdxScanPlan>(scan->table_name_, index->GetIndexId(), index_conds, true);
  }
  return new_scan;
}

auto Optimizer::PhysicalOptimizeJoin(std::shared_ptr<JoinPlan> join, DatabaseHandle *db)
    -> std::shared_ptr<AbstractPlan>
{
  // First optimize left and right children
  join->left_  = PhysicalOptimize(join->left_, db);
  join->right_ = PhysicalOptimize(join->right_, db);

  // If conditions are empty, fall back to nested loop join
  if (join->conds_.empty()) {
    join->strategy_ = NESTED_LOOP;
    return OptimizeNestedLoopJoin(join, db);
  }

  // Handle different join strategies based on the requested strategy
  switch (join->strategy_) {
    case NESTED_LOOP: return OptimizeNestedLoopJoin(join, db);
    case HASH_JOIN: return OptimizeHashJoin(join, db);
    case SORT_MERGE: return OptimizeSortMergeJoin(join, db);
    default:
      // Fall back to nested loop join for unknown strategies
      join->strategy_ = NESTED_LOOP;
      return OptimizeNestedLoopJoin(join, db);
  }
}

auto Optimizer::LogicalOptimize(std::shared_ptr<AbstractPlan> plan, DatabaseHandle *db) -> std::shared_ptr<AbstractPlan>
{
  // TODO: https://docs.pingcap.com/tidb/stable/sql-logical-optimization/
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

    // Check index type and apply different logic
    if (idx->GetIndexType() == IndexType::HASH) {
      // Hash index: can only be used if ALL index key fields have equality conditions
      bool all_fields_have_equality = true;

      for (size_t field_idx = 0; field_idx < idx->GetKeySchema().GetFieldCount(); ++field_idx) {
        const auto &field                    = idx->GetKeySchema().GetFieldAt(field_idx);
        bool        found_equality_for_field = false;

        // Find equality condition for this field
        for (int i = 0; i < static_cast<int>(conds.size()); ++i) {
          auto       &cond = conds[i];
          const auto &lcol = cond.GetLCol();

          if (lcol.field_.table_id_ == field.field_.table_id_ && lcol.field_.field_name_ == field.field_.field_name_ &&
              cond.GetOp() == OP_EQ) {
            found_equality_for_field = true;
            tmp_conds_pos.push_back(i);
            break;
          }
        }

        if (!found_equality_for_field) {
          all_fields_have_equality = false;
          break;
        }
      }

      // For hash index, we can only use it if ALL key fields have equality conditions
      if (!all_fields_have_equality) {
        tmp_conds_pos.clear();
      }

    } else if (idx->GetIndexType() == IndexType::BPTREE) {
      // B+ tree index: existing logic for range and equality conditions

      // For each field in the index key schema
      for (size_t field_idx = 0; field_idx < idx->GetKeySchema().GetFieldCount(); ++field_idx) {
        const auto      &field = idx->GetKeySchema().GetFieldAt(field_idx);
        std::vector<int> field_matching_conds;

        // Find all conditions that match this field
        for (int i = 0; i < static_cast<int>(conds.size()); ++i) {
          auto       &cond = conds[i];
          const auto &lcol = cond.GetLCol();

          if (lcol.field_.table_id_ == field.field_.table_id_ && lcol.field_.field_name_ == field.field_.field_name_) {

            // Check if this condition can be used in index scan
            auto op = cond.GetOp();
            if (op == OP_EQ || op == OP_LT || op == OP_LE || op == OP_GT || op == OP_GE) {
              field_matching_conds.push_back(i);
            }
          }
        }

        if (field_matching_conds.empty()) {
          // No conditions for this field, cannot use subsequent fields
          break;
        }

        // For the first field, we can use all matching conditions
        // For subsequent fields, we can only use them if previous fields have equality conditions
        if (field_idx == 0) {
          // First field: add all matching conditions
          tmp_conds_pos.insert(tmp_conds_pos.end(), field_matching_conds.begin(), field_matching_conds.end());
        } else {
          // Subsequent fields: check if all previous field conditions are equality
          bool prev_fields_all_eq = true;
          for (size_t prev_idx = 0; prev_idx < field_idx; ++prev_idx) {
            // Check conditions for previous fields
            const auto &prev_field            = idx->GetKeySchema().GetFieldAt(prev_idx);
            bool        has_eq_for_prev_field = false;

            for (auto pos : tmp_conds_pos) {
              auto       &prev_cond = conds[pos];
              const auto &prev_lcol = prev_cond.GetLCol();

              if (prev_lcol.field_.table_id_ == prev_field.field_.table_id_ &&
                  prev_lcol.field_.field_name_ == prev_field.field_.field_name_ && prev_cond.GetOp() == OP_EQ) {
                has_eq_for_prev_field = true;
                break;
              }
            }

            if (!has_eq_for_prev_field) {
              prev_fields_all_eq = false;
              break;
            }
          }

          if (prev_fields_all_eq) {
            // Can use this field's conditions
            tmp_conds_pos.insert(tmp_conds_pos.end(), field_matching_conds.begin(), field_matching_conds.end());
          } else {
            // Cannot use this field due to range conditions in previous fields
            break;
          }
        }
      }
    }

    // Choose the index with the most matching conditions
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
  // Add index conditions
  for (auto pos : best_conds_pos) {
    index_conds.push_back(conds[pos]);
  }

  // Erase index conditions from conds to avoid duplicate checks in the filter plan
  // Sort positions in descending order to avoid index invalidation
  std::sort(best_conds_pos.rbegin(), best_conds_pos.rend());
  for (auto pos : best_conds_pos) {
    conds.erase(conds.begin() + pos);
  }

  return best_index;
}

auto Optimizer::TryEliminateSortWithIndex(
    std::shared_ptr<SortPlan> sort, std::shared_ptr<ScanPlan> scan, DatabaseHandle *db) -> std::shared_ptr<AbstractPlan>
{
  auto indexes = db->GetIndexes(scan->table_name_);

  // Find the best index that can provide the required ordering
  IndexHandle *best_index = nullptr;
  for (auto index : indexes) {
    if (CanUseIndexForOrderBy(sort->key_schema_.get(), &index->GetKeySchema(), index->GetIndexType(), sort->is_desc_)) {
      best_index = index;
      break;  // Take the first matching index (could be improved to choose the best one)
    }
  }

  if (best_index != nullptr) {
    // Replace SortPlan(ScanPlan) with IdxScanPlan
    return std::make_shared<IdxScanPlan>(scan->table_name_,
        best_index->GetIndexId(),
        ConditionVec{},  // No conditions for pure ordering
        !sort->is_desc_  // Ascending if not desc
    );
  }

  return sort;  // Cannot optimize
}

auto Optimizer::TryEliminateSortWithIndexAndFilter(std::shared_ptr<SortPlan> sort, std::shared_ptr<FilterPlan> filter,
    std::shared_ptr<ScanPlan> scan, DatabaseHandle *db) -> std::shared_ptr<AbstractPlan>
{
  auto indexes = db->GetIndexes(scan->table_name_);

  // Try to find an index that can handle both filtering and ordering
  IndexHandle *best_index         = nullptr;
  ConditionVec index_conds        = filter->conds_;  // Copy conditions for potential modification
  size_t       max_matched_fields = 0;

  for (auto index : indexes) {
    // First check if this index can provide the required ordering
    if (CanUseIndexForOrderBy(sort->key_schema_.get(), &index->GetKeySchema(), index->GetIndexType(), sort->is_desc_)) {
      // Check if this index can also handle some filtering conditions
      ConditionVec temp_conds = filter->conds_;
      ConditionVec temp_index_conds;
      size_t       temp_matched_fields = 0;

      auto temp_index = CanIndexScan(temp_conds, temp_index_conds, {index}, temp_matched_fields);

      if (temp_index != nullptr) {
        // This index can handle both ordering and some filtering
        best_index         = index;
        index_conds        = temp_index_conds;
        max_matched_fields = temp_matched_fields;
        break;  // Take the first matching index
      } else if (best_index == nullptr) {
        // This index can only handle ordering, but it's better than nothing
        best_index = index;
        index_conds.clear();  // No filter conditions can be handled
        max_matched_fields = 0;
      }
    }
  }

  if (best_index != nullptr) {
    // Create IdxScanPlan with the conditions that can be handled by the index
    auto idx_scan =
        std::make_shared<IdxScanPlan>(scan->table_name_, best_index->GetIndexId(), index_conds, !sort->is_desc_);

    // If there are remaining filter conditions, add a FilterPlan on top
    if (max_matched_fields > 0 && max_matched_fields < filter->conds_.size()) {
      // Remove the conditions that are handled by the index
      ConditionVec remaining_conds = filter->conds_;
      ConditionVec temp_index_conds;
      size_t       temp_matched_fields = 0;
      CanIndexScan(remaining_conds, temp_index_conds, {best_index}, temp_matched_fields);

      if (!remaining_conds.empty()) {
        return std::make_shared<FilterPlan>(idx_scan, remaining_conds);
      }
    } else if (max_matched_fields == 0) {
      // Index only handles ordering, all filter conditions remain
      return std::make_shared<FilterPlan>(idx_scan, filter->conds_);
    }

    return idx_scan;  // All conditions handled by index
  }

  return sort;  // Cannot optimize
}

auto Optimizer::CanUseIndexForOrderBy(
    const RecordSchema *order_schema, const RecordSchema *index_schema, IndexType index_type, bool is_desc) -> bool
{
  if (index_type != IndexType::BPTREE) {
    return false;
  }
  // Check if ORDER BY columns are a prefix of index columns
  if (order_schema->GetFieldCount() > index_schema->GetFieldCount()) {
    return false;
  }

  // Check if all ORDER BY fields match the index fields in the same order
  for (size_t i = 0; i < order_schema->GetFieldCount(); ++i) {
    const auto &order_field = order_schema->GetFieldAt(i);
    const auto &index_field = index_schema->GetFieldAt(i);

    if (order_field.field_.table_id_ != index_field.field_.table_id_ ||
        order_field.field_.field_name_ != index_field.field_.field_name_) {
      return false;
    }
  }

  // Index scan supports both ascending and descending order
  return true;
}

auto Optimizer::CanUseSortMergeJoin(const ConditionVec &conds) -> bool
{
  if (conds.empty()) {
    return false;
  }

  // Get the operator from the first condition
  CompOp first_op = conds[0].GetOp();

  // Check if the operator is supported for sort merge join
  if (first_op != OP_EQ && first_op != OP_LT && first_op != OP_LE && first_op != OP_GT && first_op != OP_GE) {
    return false;
  }
  // check if all conditions are equality conditions or only has one inequality condition
  return conds.size() == 1 || std::all_of(
      conds.begin(), conds.end(), [](const Condition &cond) { return cond.GetOp() == OP_EQ; });
}

auto Optimizer::CanUseHashJoin(const ConditionVec &conds) -> bool
{
  if (conds.empty()) {
    return false;
  }
  // Check if there is at least one equality condition
  return std::any_of(conds.begin(), conds.end(), [](const Condition &cond) { return cond.GetOp() == OP_EQ; });
}

void Optimizer::GetSortMergeOrdering(CompOp join_op, bool &left_desc, bool &right_desc)
{
  switch (join_op) {
    case OP_EQ:
      // For equality, both sides ascending
      left_desc  = false;
      right_desc = false;
      break;
    case OP_GT:
    case OP_GE:
      // For > and >=: left descending, right ascending
      left_desc  = true;
      right_desc = false;
      break;
    case OP_LT:
    case OP_LE:
      // For < and <=: left ascending, right descending
      left_desc  = false;
      right_desc = true;
      break;
    default:
      // Should not reach here for valid sort merge join operators
      left_desc  = false;
      right_desc = false;
      break;
  }
}

auto Optimizer::OptimizeNestedLoopJoin(std::shared_ptr<JoinPlan> join, DatabaseHandle *db)
    -> std::shared_ptr<AbstractPlan>
{
  // Nested loop join requires no special optimization
  // Just return the join plan as-is
  return join;
}

auto Optimizer::OptimizeHashJoin(std::shared_ptr<JoinPlan> join, DatabaseHandle *db) -> std::shared_ptr<AbstractPlan>
{
  // Check if conditions are suitable for hash join
  if (!CanUseHashJoin(join->conds_)) {
    // Fall back to nested loop join
    join->strategy_ = NESTED_LOOP;
    return OptimizeNestedLoopJoin(join, db);
  }

  // Set up hash join
  std::vector<RTField> left_key_fields;
  std::vector<RTField> right_key_fields;

  for (const auto &cond : join->conds_) {
    if (cond.GetOp() == OP_EQ) {
      left_key_fields.push_back(cond.GetLCol());
      right_key_fields.push_back(cond.GetRCol());
    }
  }
  // remove all equality conditions
  join->conds_.erase(
      std::remove_if(
          join->conds_.begin(), join->conds_.end(), [](const Condition &cond) { return cond.GetOp() == OP_EQ; }),
      join->conds_.end());

  join->left_key_schema_  = std::make_unique<RecordSchema>(left_key_fields);
  join->right_key_schema_ = std::make_unique<RecordSchema>(right_key_fields);
  join->join_op_          = OP_EQ;  // Hash join only supports equality

  return join;
}

auto Optimizer::OptimizeSortMergeJoin(std::shared_ptr<JoinPlan> join, DatabaseHandle *db)
    -> std::shared_ptr<AbstractPlan>
{
  // Check if conditions are suitable for sort merge join
  if (!CanUseSortMergeJoin(join->conds_)) {
    // Fall back to nested loop join
    join->strategy_ = NESTED_LOOP;
    return OptimizeNestedLoopJoin(join, db);
  }

  // All conditions have the same operator, get it from the first condition
  CompOp join_op = join->conds_[0].GetOp();

  // Generate key schema from conditions
  std::vector<RTField> left_key_fields;
  std::vector<RTField> right_key_fields;

  for (const auto &cond : join->conds_) {
    left_key_fields.push_back(cond.GetLCol());
    right_key_fields.push_back(cond.GetRCol());
  }

  // Determine required ordering based on join operator
  bool left_desc, right_desc;
  GetSortMergeOrdering(join_op, left_desc, right_desc);

  std::shared_ptr<AbstractPlan> left  = join->left_;
  std::shared_ptr<AbstractPlan> right = join->right_;

  // Check if left child can provide the required ordering
  bool left_has_index_order = false;
  if (auto idx_scan = std::dynamic_pointer_cast<IdxScanPlan>(left)) {
    // Direct IdxScanPlan - check if it provides the required ordering
    auto index           = db->GetIndex(idx_scan->idx_id_);
    left_has_index_order = CanUseIndexForOrderBy(std::make_unique<RecordSchema>(left_key_fields).get(),
        &index->GetKeySchema(),
        index->GetIndexType(),
        left_desc);
    if (left_has_index_order) {
      idx_scan->is_ascending_ = !left_desc;  // Set the order direction (is_ascending is opposite of is_desc)
    }
  } else if (auto filter = std::dynamic_pointer_cast<FilterPlan>(left)) {
    if (auto idx_scan = std::dynamic_pointer_cast<IdxScanPlan>(filter->child_)) {
      // FilterPlan(IdxScanPlan) - check if the index provides the required ordering
      auto index           = db->GetIndex(idx_scan->idx_id_);
      left_has_index_order = CanUseIndexForOrderBy(std::make_unique<RecordSchema>(left_key_fields).get(),
          &index->GetKeySchema(),
          index->GetIndexType(),
          left_desc);
      if (left_has_index_order) {
        idx_scan->is_ascending_ = !left_desc;  // Set the order direction (is_ascending is opposite of is_desc)
      }
    }
  }

  // Check if right child can provide the required ordering
  bool right_has_index_order = false;
  if (auto idx_scan = std::dynamic_pointer_cast<IdxScanPlan>(right)) {
    // Direct IdxScanPlan - check if it provides the required ordering
    auto index            = db->GetIndex(idx_scan->idx_id_);
    right_has_index_order = CanUseIndexForOrderBy(std::make_unique<RecordSchema>(right_key_fields).get(),
        &index->GetKeySchema(),
        index->GetIndexType(),
        right_desc);
    if (right_has_index_order) {
      idx_scan->is_ascending_ = !right_desc;  // Set the order direction (is_ascending is opposite of is_desc)
    }
  } else if (auto filter = std::dynamic_pointer_cast<FilterPlan>(right)) {
    if (auto idx_scan = std::dynamic_pointer_cast<IdxScanPlan>(filter->child_)) {
      // FilterPlan(IdxScanPlan) - check if the index provides the required ordering
      auto index = db->GetIndex(idx_scan->idx_id_);
      if (index != nullptr) {
        right_has_index_order = CanUseIndexForOrderBy(std::make_unique<RecordSchema>(right_key_fields).get(),
            &index->GetKeySchema(),
            index->GetIndexType(),
            right_desc);
      }
      if (right_has_index_order) {
        idx_scan->is_ascending_ = !right_desc;  // Set the order direction (is_ascending is opposite of is_desc)
      }
    }
  }

  // Generate sort plan only if the child doesn't already provide the required ordering
  if (!left_has_index_order) {
    left = std::make_shared<SortPlan>(std::move(left), std::make_unique<RecordSchema>(left_key_fields), left_desc);
  }
  if (!right_has_index_order) {
    right = std::make_shared<SortPlan>(std::move(right), std::make_unique<RecordSchema>(right_key_fields), right_desc);
  }

  join->left_             = left;
  join->right_            = right;
  join->left_key_schema_  = std::make_unique<RecordSchema>(left_key_fields);
  join->right_key_schema_ = std::make_unique<RecordSchema>(right_key_fields);
  join->join_op_          = join_op;

  return join;
}
}  // namespace njudb
