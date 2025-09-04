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

#include "planner.h"

#include <utility>

namespace njudb {

auto Planner::PlanAST(const std::shared_ptr<ast::TreeNode> &ast, DatabaseHandle *db) -> std::shared_ptr<AbstractPlan>
{

  /// database related
  if (ast == nullptr) {
    return nullptr;
  } else if (const auto cdb = std::dynamic_pointer_cast<ast::CreateDatabase>(ast)) {
    return std::make_shared<CreateDBPlan>(cdb->db_name_);
  } else if (const auto odb = std::dynamic_pointer_cast<ast::OpenDatabase>(ast)) {
    return std::make_shared<OpenDBPlan>(odb->db_name_);
  } else if (const auto exp = std::dynamic_pointer_cast<ast::Explain>(ast)) {
    return std::make_shared<ExplainPlan>(PlanAST(exp->stmt, db));
  }
  if (db == nullptr) {
    NJUDB_THROW(NJUDB_DB_NOT_OPEN, "");
  }
  /// select
  if (const auto sel = std::dynamic_pointer_cast<ast::SelectStmt>(ast)) {
    std::vector<std::string> tabs;
    auto                     plan = AnalyseSelect(sel, db, tabs);
    if (sel->limit >= 0) {
      plan = std::make_shared<LimitPlan>(plan, sel->limit);
    }
    return plan;
  }
  /// insert
  if (const auto ins = std::dynamic_pointer_cast<ast::InsertStmt>(ast)) {
    std::vector<ValueSptr> values;
    values.reserve(ins->vals.size());
    for (const auto &v : ins->vals) {
      values.push_back(TransformValue(v));
    }
    return std::make_shared<InsertPlan>(ins->tab_name, values);
  }
  /// update
  if (const auto upd = std::dynamic_pointer_cast<ast::UpdateStmt>(ast)) {
    std::vector<std::pair<RTField, ValueSptr>> updates;
    for (const auto &u : upd->set_clauses) {
      CheckFieldTabName(upd->tab_name, u->col_name, db, {upd->tab_name});
      auto  tbl   = db->GetTable(upd->tab_name);
      auto &l_col = tbl->GetSchema().GetFieldByName(tbl->GetTableId(), u->col_name);
      updates.emplace_back(l_col, TransformValue(u->val));
    }
    auto conds = MakeConditionVec(upd->conds, db, {upd->tab_name});
    // ScanPlan
    auto scan_plan   = std::make_shared<ScanPlan>(upd->tab_name);
    auto filter_plan = std::make_shared<FilterPlan>(scan_plan, conds);
    return std::make_shared<UpdatePlan>(filter_plan, upd->tab_name, updates);
  }
  /// delete
  if (const auto del = std::dynamic_pointer_cast<ast::DeleteStmt>(ast)) {
    auto conds = MakeConditionVec(del->conds, db, {del->tab_name});
    // ScanPlan
    auto scan_plan   = std::make_shared<ScanPlan>(del->tab_name);
    auto filter_plan = std::make_shared<FilterPlan>(scan_plan, conds);
    return std::make_shared<DeletePlan>(filter_plan, del->tab_name);
  }
  /// create table
  if (const auto ctab = std::dynamic_pointer_cast<ast::CreateTable>(ast)) {
    auto schema = CreateRecordSchema(ctab->fields_, ctab->tab_name_, db);
    return std::make_shared<CreateTablePlan>(ctab->tab_name_, std::move(schema), ctab->model_);
  }
  /// drop table
  if (const auto dtab = std::dynamic_pointer_cast<ast::DropTable>(ast)) {
    return std::make_shared<DropTablePlan>(dtab->tab_name_);
  }
  /// describe table
  if (const auto desc = std::dynamic_pointer_cast<ast::DescTable>(ast)) {
    return std::make_shared<DescTablePlan>(desc->tab_name_);
  }
  /// show tables
  if (const auto stab = std::dynamic_pointer_cast<ast::ShowTables>(ast)) {
    return std::make_shared<ShowTablesPlan>();
  }
  /// index related
  if (const auto cidx = std::dynamic_pointer_cast<ast::CreateIndex>(ast)) {
    auto schema = CreateIndexKeySchema(cidx->tab_name_, cidx->col_names_, db);
    return std::make_shared<CreateIndexPlan>(cidx->index_name_, cidx->tab_name_, std::move(schema), cidx->index_type_);
  } else if (const auto didx = std::dynamic_pointer_cast<ast::DropIndex>(ast)) {
    return std::make_shared<DropIndexPlan>(didx->tab_name_, didx->index_name_);
  } else if (const auto sidx = std::dynamic_pointer_cast<ast::ShowIndexes>(ast)) {
    return std::make_shared<ShowIndexesPlan>(sidx->tab_name_);
  }
  /// transaction related
  if (const auto txnbeg = std::dynamic_pointer_cast<ast::TxnBegin>(ast)) {

  } else if (const auto txncom = std::dynamic_pointer_cast<ast::TxnCommit>(ast)) {

  } else if (const auto txnabt = std::dynamic_pointer_cast<ast::TxnAbort>(ast)) {

  } else if (const auto txnback = std::dynamic_pointer_cast<ast::TxnRollback>(ast)) {

  } else {
    NJUDB_FATAL("Invalid AST node");
  }
  // never reach here
  return nullptr;
}

auto Planner::AnalyseSelect(const std::shared_ptr<ast::SelectStmt> &sel, njudb::DatabaseHandle *db,
    std::vector<std::string> &tabs) -> std::shared_ptr<AbstractPlan>
{
  std::vector<RTField>          sel_fields;
  bool                          is_agg   = false;
  std::shared_ptr<AbstractPlan> sub_plan = nullptr;
  /// analyse tables, tables are of three types: tabname, tabname (JOIN_TYPE) tabname, (SELECT_STMT)
  for (const auto &t : sel->tabs) {
    if (const auto tab = std::dynamic_pointer_cast<ast::ExplicitTable>(t)) {
      tabs.push_back(tab->tab_name);
    } else if (const auto join = std::dynamic_pointer_cast<ast::JoinExpr>(t)) {
      if (sel->tabs.size() != 1) {
        NJUDB_THROW(NJUDB_GRAMMAR_ERROR, "Explicit join only support two named tables");
      }
      tabs.push_back(join->left);
      tabs.push_back(join->right);
    } else if (const auto subq = std::dynamic_pointer_cast<ast::SelectStmt>(t)) {
      if (sel->tabs.size() != 1) {
        NJUDB_THROW(NJUDB_GRAMMAR_ERROR, "only support single sub query now");
      }
      sub_plan = AnalyseSelect(subq, db, tabs);
    }
  }
  /// analyse projection fields
  // analyse fields， fields can be of three types: *, tabname.*, colname, agg(colname)
  if (sel->cols.empty()) {
    sel_fields = GetAllFields(tabs, db);
  }
  // else cols are not empty, analyse cols and check if there is aggregation
  for (const auto &col : sel->cols) {
    RTField rt;
    if (col->col_name != "*") {
      CheckFieldTabName(col->tab_name, col->col_name, db, tabs);
      auto tab = db->GetTable(col->tab_name);
      rt       = tab->GetSchema().GetFieldByName(tab->GetTableId(), col->col_name);
    }
    rt.alias_ = col->alias;
    if (const auto agg = std::dynamic_pointer_cast<ast::AggCol>(col)) {
      rt.is_agg_   = true;
      rt.agg_type_ = agg->agg_type;
      // change type and size of the field, the aggregate executor only check table id and field name
      if (rt.agg_type_ == AGG_COUNT_STAR || rt.agg_type_ == AGG_COUNT) {
        rt.field_.field_type_ = TYPE_INT;
        rt.field_.field_size_ = sizeof(int);
      }
      is_agg = true;
    }
    sel_fields.push_back(rt);
  }
  /// analyse conditions
  // get where and having conditions
  auto where  = MakeConditionVec(sel->conds, db, tabs);
  auto having = MakeConditionVec(sel->having, db, tabs);
  // check order by cols
  std::vector<RTField> order_fields =
      sel->has_sort ? TransformCols(sel->order->cols_, db, tabs) : std::vector<RTField>{};
  bool is_desc = sel->has_sort && sel->order->orderby_dir_ == OrderBy_DESC;
  // check group by cols
  std::vector<RTField> group_fields =
      sel->has_groupby ? TransformCols(sel->groupby->cols, db, tabs) : std::vector<RTField>{};
  is_agg = is_agg || !having.empty() || !group_fields.empty();
  /// analyse and generate plans
  if (sub_plan != nullptr) {
    /// select with sub query
    auto plan = std::move(sub_plan);
    if(!where.empty()){
      // if there are conditions, we need to add a filter plan
      plan = std::make_shared<FilterPlan>(plan, where);
    }
    if (is_agg) {
      plan = MakeAggregatePlan(plan, group_fields, sel_fields, having);
    }
    return MakeProjSortPlan(plan, sel_fields, order_fields, is_desc);
  } else if (tabs.size() == 1) {
    /// single table without sub query or joins
    auto plan = MakeFilterScanPlan(tabs[0], where);
    if (is_agg) {
      plan = MakeAggregatePlan(plan, group_fields, sel_fields, having);
    }
    return MakeProjSortPlan(plan, sel_fields, order_fields, is_desc);
  } else {
    /// analyse joins
    if (sel->tabs.size() == 1) {
      // explicit join
      NJUDB_ASSERT(tabs.size() == 2, "table size should be 2");
      auto                          join_expr  = std::dynamic_pointer_cast<ast::JoinExpr>(sel->tabs[0]);
      auto                          join_cond  = GetConditionsForJoin(join_expr->left, join_expr->right, where, db);
      auto                          left_cond  = GetConditionsForTable(join_expr->left, where, db);
      auto                          right_cond = GetConditionsForTable(join_expr->right, where, db);
      std::shared_ptr<AbstractPlan> left_plan  = MakeFilterScanPlan(join_expr->left, left_cond);
      std::shared_ptr<AbstractPlan> right_plan = MakeFilterScanPlan(join_expr->right, right_cond);
      std::shared_ptr<AbstractPlan> sum_plan   = std::make_shared<JoinPlan>(
          std::move(left_plan), std::move(right_plan), join_cond, join_expr->type, sel->join_strategy);
      if (is_agg) {
        sum_plan = MakeAggregatePlan(sum_plan, group_fields, sel_fields, having);
      }
      return MakeProjSortPlan(sum_plan, sel_fields, order_fields, is_desc);
    } else {
      // implicit join, join conditions are in where clause
      auto join_tabs      = tabs;
      auto right_tab_name = join_tabs.back();
      auto right_cond     = GetConditionsForTable(right_tab_name, where, db);
      auto right_plan     = MakeFilterScanPlan(right_tab_name, right_cond);
      join_tabs.pop_back();
      std::vector<std::string> right_tree_tables{right_tab_name};
      // NOTE: the generated join tree is not balanced
      while (!join_tabs.empty()) {
        auto left_tab_name = join_tabs.back();
        auto join_cond     = ConditionVec{};
        for (auto &tab : right_tree_tables) {
          // append join cond with newly added table
          auto join_cond_tmp = GetConditionsForJoin(left_tab_name, tab, where, db);
          join_cond.insert(join_cond.end(), join_cond_tmp.begin(), join_cond_tmp.end());
        }
        auto left_cond = GetConditionsForTable(left_tab_name, where, db);
        auto left_plan = MakeFilterScanPlan(left_tab_name, left_cond);
        right_plan     = std::make_shared<JoinPlan>(
            std::move(left_plan), std::move(right_plan), join_cond, INNER_JOIN, sel->join_strategy);
        join_tabs.pop_back();
        right_tree_tables.push_back(left_tab_name);
      }
      if (is_agg) {
        right_plan = MakeAggregatePlan(right_plan, group_fields, sel_fields, having);
      }
      return MakeProjSortPlan(right_plan, sel_fields, order_fields, is_desc);
    }
  }  // end of join analysis
  // never reach here
  return nullptr;
}

auto Planner::TransformValue(const std::shared_ptr<ast::Value> &val) -> ValueSptr
{
  if (const auto i = std::dynamic_pointer_cast<ast::IntLit>(val)) {
    return ValueFactory::CreateIntValue(i->val_);
  } else if (const auto f = std::dynamic_pointer_cast<ast::FloatLit>(val)) {
    return ValueFactory::CreateFloatValue(f->val_);
  } else if (const auto s = std::dynamic_pointer_cast<ast::StringLit>(val)) {
    return ValueFactory::CreateStringValue(s->val_.c_str(), s->val_.size());
  } else if (const auto n = std::dynamic_pointer_cast<ast::NullLit>(val)) {
    // as we do not know the type or size of the null value, we use int type and 0 size, should
    // handle carefully in executors
    return ValueFactory::CreateNullValue(TYPE_INT);
  } else {
    NJUDB_FATAL("Invalid value type");
  }
}

auto Planner::TransformCols(const std::vector<std::shared_ptr<ast::Col>> &cols, njudb::DatabaseHandle *db,
    const std::vector<std::string> &tabs) -> std::vector<RTField>
{
  std::vector<RTField> fields;
  fields.reserve(cols.size());
  for (auto &c : cols) {
    CheckFieldTabName(c->tab_name, c->col_name, db, tabs);
    if (const auto agg_col = std::dynamic_pointer_cast<ast::AggCol>(c)) {
      NJUDB_THROW(NJUDB_GRAMMAR_ERROR,
          fmt::format(
              "Aggregation in wrong place, type: {}, col: {}", AggTypeToString(agg_col->agg_type), agg_col->col_name));
    }
    auto tab = db->GetTable(c->tab_name);
    auto rt  = tab->GetSchema().GetFieldByName(tab->GetTableId(), c->col_name);
    fields.push_back(rt);
  }
  return fields;
}

auto Planner::MakeConditionVec(const std::vector<std::shared_ptr<ast::BinaryExpr>> &exprs, DatabaseHandle *db,
    const std::vector<std::string> &tabs) -> ConditionVec
{
  ConditionVec conds;
  conds.reserve(exprs.size());
  for (const auto &e : exprs) {
    auto    lhs = e->lhs_;
    RTField l_rt;
    // count(*) or sum(*) situation
    if (lhs->col_name != "*") {
      CheckFieldTabName(lhs->tab_name, lhs->col_name, db, tabs);
      auto ltab = db->GetTable(lhs->tab_name);
      l_rt      = ltab->GetSchema().GetFieldByName(ltab->GetTableId(), lhs->col_name);
    }
    l_rt.alias_ = lhs->alias;
    if (const auto col = std::dynamic_pointer_cast<ast::AggCol>(lhs)) {
      l_rt.is_agg_   = true;
      l_rt.agg_type_ = col->agg_type;
      if (l_rt.agg_type_ == AGG_COUNT_STAR || l_rt.agg_type_ == AGG_COUNT) {
        l_rt.field_.field_type_ = TYPE_INT;
        l_rt.field_.field_size_ = sizeof(int);
      }
    }
    auto rhs = e->rhs_;
    if (const auto col = std::dynamic_pointer_cast<ast::Col>(rhs)) {
      if (const auto agg = std::dynamic_pointer_cast<ast::AggCol>(rhs)) {
        NJUDB_THROW(NJUDB_GRAMMAR_ERROR, "Aggregation function in right hand side");
      }
      CheckFieldTabName(col->tab_name, col->col_name, db, tabs);
      auto rtab   = db->GetTable(col->tab_name);
      auto r_rt   = rtab->GetSchema().GetFieldByName(rtab->GetTableId(), col->col_name);
      r_rt.alias_ = col->alias;
      conds.emplace_back(e->op_, l_rt, r_rt);
    } else if (const auto val = std::dynamic_pointer_cast<ast::Value>(rhs)) {
      auto v = TransformValue(val);
      v      = ValueFactory::CastTo(v, l_rt.field_.field_type_);
      conds.emplace_back(e->op_, l_rt, v);
    } else if (const auto sel = std::dynamic_pointer_cast<ast::SelectStmt>(rhs)) {
      // TODO: subquery in condition
      NJUDB_THROW(NJUDB_NOT_IMPLEMENTED, "Subquery in condition is not implemented yet");
    } else {
      NJUDB_THROW(NJUDB_GRAMMAR_ERROR, "Invalid right hand side");
    }
  }
  return conds;
}

auto Planner::CreateRecordSchema(const std::vector<std::shared_ptr<ast::Field>> &fields, std::string &tab_name,
    DatabaseHandle *db) -> RecordSchemaUptr
{
  std::vector<RTField> rt_fields;
  rt_fields.reserve(fields.size());
  for (const auto &f : fields) {
    const auto col_def = std::dynamic_pointer_cast<ast::ColDef>(f);
    if (col_def == nullptr) {
      NJUDB_FATAL("Invalid field definition");
    }
    FieldSchema fs;
    fs.field_name_ = col_def->col_name_;
    fs.field_type_ = col_def->type_len_->type_;
    fs.field_size_ = col_def->type_len_->len_;
    auto tbl       = db->GetTable(tab_name);
    fs.table_id_   = tbl != nullptr ? tbl->GetTableId() : INVALID_TABLE_ID;
    if (fs.field_size_ == 0) {
      NJUDB_THROW(NJUDB_GRAMMAR_ERROR, "Field size cannot be 0");
    }
    rt_fields.push_back({.field_ = fs});
  }
  return std::make_unique<RecordSchema>(rt_fields);
}

auto Planner::CreateIndexKeySchema(
    const std::string &table_name, const std::vector<std::string> &col_names, DatabaseHandle *db) -> RecordSchemaUptr
{
  std::vector<RTField> rt_fields;
  rt_fields.reserve(col_names.size());
  auto tab = db->GetTable(table_name);
  if (tab == nullptr) {
    NJUDB_THROW(NJUDB_TABLE_MISS, table_name);
  }
  std::string dummy_table_name = table_name;
  for (const auto &col_name : col_names) {
    CheckFieldTabName(dummy_table_name, col_name, db, {dummy_table_name});
    NJUDB_ASSERT(
        dummy_table_name == table_name, fmt::format("Table name mismatch: {}, {}", dummy_table_name, table_name));
    auto fs = tab->GetSchema().GetFieldByName(tab->GetTableId(), col_name);
    rt_fields.push_back(fs);
  }
  return std::make_unique<RecordSchema>(rt_fields);
}

void Planner::CheckFieldTabName(
    std::string &tab_name, const std::string &field_name, DatabaseHandle *db, const std::vector<std::string> &cand_tabs)
{
  if (!tab_name.empty()) {
    // check if the table exists and if the field exists
    if (db->GetTable(tab_name) == nullptr) {
      NJUDB_THROW(NJUDB_TABLE_MISS, tab_name);
    }
    if (field_name.empty()) {
      NJUDB_THROW(NJUDB_GRAMMAR_ERROR, "Field name cannot be empty");
    }
    if (!db->GetTable(tab_name)->HasField(field_name)) {
      NJUDB_THROW(NJUDB_FIELD_MISS, field_name);
    }
  } else {
    auto checkField = [&](TableHandle *tab, const std::string &field) -> void {
      if (tab->HasField(field)) {
        if (!tab_name.empty()) {
          NJUDB_THROW(NJUDB_GRAMMAR_ERROR,
              fmt::format("Ambiguous field:{}, tab1:{}, tab2:{}", field, tab_name, tab->GetTableName()));
        }
        tab_name = tab->GetTableName();
      }
    };
    if (cand_tabs.empty()) {
      for (const auto &tid_tab : db->GetAllTables()) {
        auto tab = tid_tab.second.get();
        checkField(tab, field_name);
      }
    } else {
      for (const auto &cand_tab : cand_tabs) {
        auto tab = db->GetTable(cand_tab);
        if (tab == nullptr) {
          NJUDB_THROW(NJUDB_TABLE_MISS, cand_tab);
        }
        checkField(tab, field_name);
      }
    }
    if (tab_name.empty()) {
      NJUDB_THROW(NJUDB_FIELD_MISS, field_name);
    }
  }
}

auto Planner::GetAllFields(const std::vector<std::string> &tabs, DatabaseHandle *db) -> std::vector<RTField>
{
  std::vector<RTField> fields;
  for (const auto &tab_name : tabs) {
    auto tab = db->GetTable(tab_name);
    if (tab == nullptr) {
      NJUDB_THROW(NJUDB_TABLE_MISS, tab_name);
    }
    for (int i = 0; i < static_cast<int>(tab->GetSchema().GetFieldCount()); ++i) {
      auto &field = tab->GetSchema().GetFieldAt(i);
      fields.push_back(field);
    }
  }
  return fields;
}

auto Planner::GetConditionsForJoin(
    const std::string &left, const std::string &right, ConditionVec &conds, DatabaseHandle *db) -> ConditionVec
{
  ConditionVec ret;
  auto         left_table_id  = db->GetTable(left)->GetTableId();
  auto         right_table_id = db->GetTable(right)->GetTableId();

  // move the condition of the join to ret, should check if the rhs is column
  for (const auto &c : conds) {
    if (c.GetRhsType() == kColumn) {
      auto left_col_table_id  = c.GetLCol().field_.table_id_;
      auto right_col_table_id = c.GetRCol().field_.table_id_;
      if (left_col_table_id == left_table_id && right_col_table_id == right_table_id &&
          left_col_table_id == right_col_table_id) {
        ret.push_back(c);
      }
      // Case 1: left column belongs to left table, right column belongs to right table
      else if (left_col_table_id == left_table_id && right_col_table_id == right_table_id) {
        ret.push_back(c);
      }
      // Case 2: left column belongs to right table, right column belongs to left table
      // Use GetReversedCondition to swap the columns and reverse the operator
      else if (left_col_table_id == right_table_id && right_col_table_id == left_table_id) {
        ret.push_back(c.GetReversedCondition());
      }
    }
  }
  return ret;
}

auto Planner::GetConditionsForTable(const std::string &tab_name, const ConditionVec &conds, DatabaseHandle *db)
    -> ConditionVec
{
  std::vector<Condition> ret;
  for (const auto &c : conds) {
    if (c.GetLCol().field_.table_id_ == db->GetTable(tab_name)->GetTableId()) {
      if (c.GetRhsType() != kColumn || c.GetRCol().field_.table_id_ == db->GetTable(tab_name)->GetTableId()) {
        ret.push_back(c);
      }
    }
  }
  return ret;
}

auto Planner::MakeFilterScanPlan(const std::string &tab_name, const ConditionVec &conds)
    -> std::shared_ptr<AbstractPlan>
{
  auto scan_plan = std::make_shared<ScanPlan>(tab_name);
  if (conds.empty()) {
    return scan_plan;
  }
  return std::make_shared<FilterPlan>(std::move(scan_plan), conds);
}

auto Planner::MakeAggregatePlan(std::shared_ptr<AbstractPlan> &child, const std::vector<RTField> &group_fields,
    const std::vector<RTField> &proj_fields, const ConditionVec &havings) -> std::shared_ptr<AbstractPlan>
{
  std::vector<RTField> agg_fields;
  // do projection field check, i.e. any none projection field should be in group by
  for (const auto &f : proj_fields) {
    if (f.is_agg_) {
      agg_fields.push_back(f);
    } else {
      auto it = std::find_if(group_fields.begin(), group_fields.end(), [&f](const RTField &g) {
        return g.field_.table_id_ == f.field_.table_id_ && g.field_.field_name_ == f.field_.field_name_;
      });
      if (it == group_fields.end()) {
        NJUDB_THROW(
            NJUDB_GRAMMAR_ERROR, fmt::format("field \"{}\" in projection should be in group by", f.field_.field_name_));
      }
    }
  }
  for (auto &c : havings) {
    if (c.GetLCol().is_agg_) {
      agg_fields.push_back(c.GetLCol());
    } else {
      auto it = std::find_if(group_fields.begin(), group_fields.end(), [&c](const RTField &g) {
        return g.field_.table_id_ == c.GetLCol().field_.table_id_ &&
               g.field_.field_name_ == c.GetLCol().field_.field_name_;
      });
      if (it == group_fields.end()) {
        NJUDB_THROW(NJUDB_GRAMMAR_ERROR,
            fmt::format("field \"{}\" in having should be in group by", c.GetLCol().field_.field_name_));
      }
    }
  }
  if (havings.empty()) {
    return std::make_shared<AggregatePlan>(std::move(child), group_fields, agg_fields);
  } else {
    auto agg_plan = std::make_shared<AggregatePlan>(std::move(child), group_fields, agg_fields);
    return std::make_shared<FilterPlan>(std::move(agg_plan), havings);
  }
}

auto Planner::MakeProjSortPlan(std::shared_ptr<AbstractPlan> &child, const std::vector<RTField> &proj_fields,
    const std::vector<RTField> &sort_fields, bool is_desc) -> std::shared_ptr<AbstractPlan>
{
  if (sort_fields.empty()) {
    return std::make_shared<ProjectPlan>(std::move(child), proj_fields);
  }
  auto key_schema = std::make_unique<RecordSchema>(sort_fields);
  auto sort       = std::make_shared<SortPlan>(std::move(child), std::move(key_schema), is_desc);
  return std::make_shared<ProjectPlan>(std::move(sort), proj_fields);
}
}  // namespace njudb
