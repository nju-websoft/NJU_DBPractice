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

#include "executor.h"
#include "executor_defs.h"

#include "expr/condition_expr.h"

namespace njudb {

// translate the plan to executor
auto Executor::Translate(const std::shared_ptr<AbstractPlan> &plan, DatabaseHandle *db) -> AbstractExecutorUptr
{
  if (db == nullptr) {
    NJUDB_THROW(NJUDB_DB_NOT_OPEN, "");
  }
  // translate
  if (const auto create_table = std::dynamic_pointer_cast<CreateTablePlan>(plan)) {
    return std::make_unique<CreateTableExecutor>(
        create_table->table_name_, std::move(create_table->schema_), db, create_table->storage_);
  } else if (const auto drop_table = std::dynamic_pointer_cast<DropTablePlan>(plan)) {
    return std::make_unique<DropTableExecutor>(drop_table->table_name_, db);
  } else if (const auto desc_table = std::dynamic_pointer_cast<DescTablePlan>(plan)) {
    return std::make_unique<DescTableExecutor>(db->GetTable(desc_table->table_name_));
  } else if (const auto show_table = std::dynamic_pointer_cast<ShowTablesPlan>(plan)) {
    return std::make_unique<ShowTablesExecutor>(db);
  } else if (const auto create_index = std::dynamic_pointer_cast<CreateIndexPlan>(plan)) {
    return std::make_unique<CreateIndexExecutor>(create_index->index_name_,
        create_index->table_name_,
        std::move(create_index->key_schema_),
        create_index->index_type_,
        db);
  } else if (const auto drop_index = std::dynamic_pointer_cast<DropIndexPlan>(plan)) {
    return std::make_unique<DropIndexExecutor>(drop_index->table_name_, drop_index->index_name_, db);
  } else if (const auto show_index = std::dynamic_pointer_cast<ShowIndexesPlan>(plan)) {
    if (show_index->table_name_.empty()) {
      return std::make_unique<ShowIndexesExecutor>(db);
    } else {
      return std::make_unique<ShowIndexesExecutor>(show_index->table_name_, db);
    }
  } else if (const auto insert = std::dynamic_pointer_cast<InsertPlan>(plan)) {
    if (db->GetTable(insert->table_name_) == nullptr) {
      NJUDB_THROW(NJUDB_TABLE_MISS, insert->table_name_);
    }
    auto                    tab = db->GetTable(insert->table_name_);
    std::vector<RecordUptr> inserts;
    inserts.emplace_back(std::make_unique<Record>(&tab->GetSchema(), insert->values_, INVALID_RID));
    return std::make_unique<InsertExecutor>(tab, db->GetIndexes(insert->table_name_), std::move(inserts));
  } else if (const auto update = std::dynamic_pointer_cast<UpdatePlan>(plan)) {
    auto tab = db->GetTable(update->table_name_);
    if (tab == nullptr) {
      NJUDB_THROW(NJUDB_TABLE_MISS, update->table_name_);
    }
    return std::make_unique<UpdateExecutor>(
        Translate(update->child_, db), tab, db->GetIndexes(update->table_name_), std::move(update->updates_));
  } else if (const auto del = std::dynamic_pointer_cast<DeletePlan>(plan)) {
    auto tab = db->GetTable(del->table_name_);
    if (tab == nullptr) {
      NJUDB_THROW(NJUDB_TABLE_MISS, del->table_name_);
    }
    return std::make_unique<DeleteExecutor>(Translate(del->child_, db), tab, db->GetIndexes(del->table_name_));
  } else if (const auto filter = std::dynamic_pointer_cast<FilterPlan>(plan)) {
    std::function<bool(const Record &)> filter_func = [filter](const Record &record) {
      return ConditionExpr::Eval(filter->conds_, record);
    };
    return std::make_unique<FilterExecutor>(Translate(filter->child_, db), std::move(filter_func));
  } else if (const auto scan = std::dynamic_pointer_cast<ScanPlan>(plan)) {
    auto tab = db->GetTable(scan->table_name_);
    if (tab == nullptr) {
      NJUDB_THROW(NJUDB_TABLE_MISS, scan->table_name_);
    }
    return std::make_unique<SeqScanExecutor>(tab);
  } else if (const auto idx_scan = std::dynamic_pointer_cast<IdxScanPlan>(plan)) {
    return std::make_unique<IdxScanExecutor>(db->GetTable(idx_scan->table_name_),
        db->GetIndex(idx_scan->idx_id_),
        idx_scan->conds_,
        true);  // Default to ascending order
  } else if (const auto sort_plan = std::dynamic_pointer_cast<SortPlan>(plan)) {
    return std::make_unique<SortExecutor>(
        Translate(sort_plan->child_, db), std::move(sort_plan->key_schema_), sort_plan->is_desc_);
  } else if (const auto proj_plan = std::dynamic_pointer_cast<ProjectPlan>(plan)) {
    return std::make_unique<ProjectionExecutor>(Translate(proj_plan->child_, db), std::move(proj_plan->schema_));
  } else if (const auto join_plan = std::dynamic_pointer_cast<JoinPlan>(plan)) {
    if (join_plan->strategy_ == NESTED_LOOP) {
      return std::make_unique<NestedLoopJoinExecutor>(
          join_plan->type_, Translate(join_plan->left_, db), Translate(join_plan->right_, db), join_plan->conds_);
    } else if (join_plan->strategy_ == HASH_JOIN) {
      return std::make_unique<HashJoinExecutor>(join_plan->type_,
          Translate(join_plan->left_, db),
          Translate(join_plan->right_, db),
          std::move(join_plan->left_key_schema_),
          std::move(join_plan->right_key_schema_),
          std::move(join_plan->conds_));
    } else if (join_plan->strategy_ == SORT_MERGE) {
      return std::make_unique<SortMergeJoinExecutor>(join_plan->type_,
          Translate(join_plan->left_, db),
          Translate(join_plan->right_, db),
          std::move(join_plan->left_key_schema_),
          std::move(join_plan->right_key_schema_),
          join_plan->join_op_);
    }
  } else if (const auto agg_plan = std::dynamic_pointer_cast<AggregatePlan>(plan)) {
    auto agg_schema   = std::make_unique<RecordSchema>(agg_plan->agg_fields);
    auto group_schema = std::make_unique<RecordSchema>(agg_plan->group_fields_);
    return std::make_unique<AggregateExecutor>(
        Translate(agg_plan->child_, db), std::move(agg_schema), std::move(group_schema));
  } else if (const auto lim = std::dynamic_pointer_cast<LimitPlan>(plan)) {
    return std::make_unique<LimitExecutor>(Translate(lim->child_, db), lim->limit_);

  } else {
    NJUDB_FATAL("Unknown plan type");
  }
  return nullptr;
}

void Executor::Execute(const AbstractExecutorUptr &executor, Context *ctx)
{
  if (executor->GetType() == TXN) {
    // do transaction executor if implemented
  } else {
    auto header = executor->GetOutSchema();
    ctx->nt_ctl_->SendRecHeader(ctx->client_fd_, header);
    for (executor->Init(); !executor->IsEnd(); executor->Next()) {
      auto rec = executor->GetRecord();
      NJUDB_ASSERT(rec != nullptr, "");
      ctx->nt_ctl_->SendRec(ctx->client_fd_, rec.get());
    }
    ctx->nt_ctl_->SendRecFinish(ctx->client_fd_);
  }
}

}  // namespace njudb