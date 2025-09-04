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
// Created by ziqi on 2024/7/17.
//

#ifndef NJUDB_AST_H
#define NJUDB_AST_H

#include <utility>
#include <vector>
#include <string>
#include <memory>

#include "common/types.h"

namespace njudb {

namespace ast {

/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

// Base class for tree nodes
struct TreeNode
{
  virtual ~TreeNode() = default;  // enable polymorphism
};

struct Help : public TreeNode
{};

struct Explain : public TreeNode
{
  std::shared_ptr<TreeNode> stmt;

  explicit Explain(std::shared_ptr<TreeNode> stmt_) : stmt(std::move(stmt_)) {}
};

struct ShowTables : public TreeNode
{};

struct TxnBegin : public TreeNode
{};

struct TxnCommit : public TreeNode
{};

struct TxnAbort : public TreeNode
{};

struct TxnRollback : public TreeNode
{};

struct LogStaticCheckpoint : public TreeNode
{};

struct TypeLen : public TreeNode
{
  FieldType type_;
  int       len_;

  TypeLen(FieldType type, int len) : type_(type), len_(len) {}
};

struct Field : public TreeNode
{};

struct ColDef : public Field
{
  std::string              col_name_;
  std::shared_ptr<TypeLen> type_len_;

  ColDef(std::string col_name, std::shared_ptr<TypeLen> &type_len) : col_name_(std::move(col_name)), type_len_(type_len)
  {}
};

struct CreateDatabase : public TreeNode
{
  std::string db_name_;

  explicit CreateDatabase(std::string db_name) : db_name_(std::move(db_name)) {}
};

struct OpenDatabase : public TreeNode
{
  std::string db_name_;

  explicit OpenDatabase(std::string db_name) : db_name_(std::move(db_name)) {}
};

struct CreateTable : public TreeNode
{
  std::string                         tab_name_;
  std::vector<std::shared_ptr<Field>> fields_;
  StorageModel                        model_;

  CreateTable(std::string tab_name, std::vector<std::shared_ptr<Field>> fields, StorageModel model)
      : tab_name_(std::move(tab_name)), fields_(std::move(fields)), model_(model)
  {}
};

struct DropTable : public TreeNode
{
  std::string tab_name_;

  explicit DropTable(std::string tab_name) : tab_name_(std::move(tab_name)) {}
};

struct DescTable : public TreeNode
{
  std::string tab_name_;

  DescTable(std::string tab_name) : tab_name_(std::move(tab_name)) {}
};

struct CreateIndex : public TreeNode
{
  std::string              index_name_;
  std::string              tab_name_;
  std::vector<std::string> col_names_;
  IndexType                index_type_;

  CreateIndex(std::string index_name, std::string tab_name, std::vector<std::string> col_names, IndexType index_type)
      : index_name_(std::move(index_name)),
        tab_name_(std::move(tab_name)),
        col_names_(std::move(col_names)),
        index_type_(index_type)
  {}
};

struct DropIndex : public TreeNode
{
  std::string tab_name_;
  std::string index_name_;

  DropIndex(std::string tab_name, std::string index_name)
      : tab_name_(std::move(tab_name)), index_name_(std::move(index_name))
  {}
};

struct ShowIndexes : public TreeNode
{
  std::string tab_name_;

  ShowIndexes(std::string tab_name) : tab_name_(std::move(tab_name)) {}
};

struct Expr : public TreeNode
{};

struct Value : public Expr
{};

struct IntLit : public Value
{
  int val_;

  IntLit(int val) : val_(val) {}
};

struct FloatLit : public Value
{
  float val_;

  FloatLit(float val) : val_(val) {}
};

struct StringLit : public Value
{
  std::string val_;

  StringLit(std::string val) : val_(std::move(val)) {}
};

struct BoolLit : public Value
{
  bool val_;

  BoolLit(bool val) : val_(val) {}
};

struct ArrLit : public Value
{
  std::vector<std::shared_ptr<Value>> val_;

  ArrLit(std::vector<std::shared_ptr<Value>> val) : val_(std::move(val)) {}
};

struct NullLit : public Value
{};

struct Col : public Expr
{
  std::string tab_name;
  std::string col_name;
  std::string alias;

  Col(std::string tab_name_, std::string col_name_, std::string alias_ = "")
      : tab_name(std::move(tab_name_)), col_name(std::move(col_name_)), alias(std::move(alias_))
  {}

  void setAlias(std::string alias_) { alias = std::move(alias_); }
};

struct AggCol : public Col
{
  AggType agg_type;

  AggCol(const std::shared_ptr<Col> &on_col, AggType agg_type_)
      : Col(on_col->tab_name, on_col->col_name), agg_type(agg_type_)
  {}
};

struct SetClause : public TreeNode
{
  std::string            col_name;
  std::shared_ptr<Value> val;

  SetClause(std::string col_name_, std::shared_ptr<Value> val_) : col_name(std::move(col_name_)), val(std::move(val_))
  {}
};

struct BinaryExpr : public TreeNode
{
  std::shared_ptr<Col>      lhs_;
  CompOp                    op_;
  std::shared_ptr<TreeNode> rhs_;

  BinaryExpr(std::shared_ptr<Col> lhs, CompOp op, std::shared_ptr<TreeNode> rhs)
      : lhs_(std::move(lhs)), op_(op), rhs_(std::move(rhs))
  {}
};

struct OrderBy : public TreeNode
{
  OrderByDir                        orderby_dir_;
  std::vector<std::shared_ptr<Col>> cols_;

  OrderBy(OrderByDir orderby_dir, std::vector<std::shared_ptr<Col>> cols)
      : orderby_dir_(std::move(orderby_dir)), cols_(std::move(cols))
  {}
};

struct GroupBy : public TreeNode
{
  std::vector<std::shared_ptr<Col>> cols;

  explicit GroupBy(std::vector<std::shared_ptr<Col>> cols_) : cols(std::move(cols_)) {}
};

struct InsertStmt : public TreeNode
{
  std::string                         tab_name;
  std::vector<std::shared_ptr<Value>> vals;

  InsertStmt(std::string tab_name_, std::vector<std::shared_ptr<Value>> vals_)
      : tab_name(std::move(tab_name_)), vals(std::move(vals_))
  {}
};

struct BulkInsertStmt : public TreeNode
{
  std::string tab_name;
  std::string file_name;
  char        delim;

  BulkInsertStmt(std::string tab_name_, std::string file_name_, char delim_)
      : tab_name(std::move(tab_name_)), file_name(std::move(file_name_)), delim(delim_)
  {}
};

struct DeleteStmt : public TreeNode
{
  std::string                              tab_name;
  std::vector<std::shared_ptr<BinaryExpr>> conds;

  DeleteStmt(std::string tab_name_, std::vector<std::shared_ptr<BinaryExpr>> conds_)
      : tab_name(std::move(tab_name_)), conds(std::move(conds_))
  {}
};

struct UpdateStmt : public TreeNode
{
  std::string                              tab_name;
  std::vector<std::shared_ptr<SetClause>>  set_clauses;
  std::vector<std::shared_ptr<BinaryExpr>> conds;

  UpdateStmt(std::string tab_name_, std::vector<std::shared_ptr<SetClause>> set_clauses_,
      std::vector<std::shared_ptr<BinaryExpr>> conds_)
      : tab_name(std::move(tab_name_)), set_clauses(std::move(set_clauses_)), conds(std::move(conds_))
  {}
};

struct JoinExpr : public TreeNode
{
  std::string left;
  std::string right;
  JoinType    type;

  JoinExpr(std::string left_, std::string right_, JoinType type_)
      : left(std::move(left_)), right(std::move(right_)), type(type_)
  {}
};

struct ExplicitTable : public TreeNode
{
  std::string tab_name;

  explicit ExplicitTable(std::string tab_name_) : tab_name(std::move(tab_name_)) {}
};

struct SelectStmt : public TreeNode
{
  std::vector<std::shared_ptr<Col>>        cols;
  std::vector<std::shared_ptr<TreeNode>>   tabs;
  std::vector<std::shared_ptr<BinaryExpr>> conds;
  JoinStrategy                             join_strategy;

  bool has_sort;

  bool                                     has_groupby;
  std::shared_ptr<GroupBy>                 groupby;
  std::vector<std::shared_ptr<BinaryExpr>> having;

  std::shared_ptr<OrderBy> order;
  int limit;

  SelectStmt(std::vector<std::shared_ptr<Col>> cols_, std::vector<std::shared_ptr<TreeNode>> tabs_,
      std::vector<std::shared_ptr<BinaryExpr>> conds_, std::shared_ptr<GroupBy> groupby_,
      std::vector<std::shared_ptr<BinaryExpr>> having_, JoinStrategy join_st_, std::shared_ptr<OrderBy> order_,
      int limit_)
      : cols(std::move(cols_)),
        tabs(std::move(tabs_)),
        conds(std::move(conds_)),
        join_strategy(join_st_),
        groupby(std::move(groupby_)),
        having(std::move(having_)),
        order(std::move(order_)),
        limit(limit_)
  {
    has_sort    = (bool)order;
    has_groupby = (bool)groupby;
  }
};

// Semantic value
struct SemValue
{
  int                      sv_int;
  float                    sv_float;
  std::string              sv_str;
  bool                     sv_bool;
  OrderByDir               sv_orderby_dir;
  JoinStrategy             sv_join_strategy;
  IndexType                sv_index_type;
  std::vector<std::string> sv_strs;

  std::shared_ptr<TreeNode> sv_node;

  std::vector<std::shared_ptr<TreeNode>> sv_node_arr;

  std::shared_ptr<SelectStmt> sv_sel;

  CompOp sv_comp_op;

  StorageModel sv_storage_model;

  std::shared_ptr<TypeLen> sv_type_len;

  std::shared_ptr<Field>              sv_field;
  std::vector<std::shared_ptr<Field>> sv_fields;

  std::shared_ptr<Expr> sv_expr;

  std::shared_ptr<Value>              sv_val;
  std::vector<std::shared_ptr<Value>> sv_vals;

  std::shared_ptr<AggCol>           sv_agg_col;
  std::shared_ptr<Col>              sv_col;
  std::vector<std::shared_ptr<Col>> sv_cols;

  std::shared_ptr<SetClause>              sv_set_clause;
  std::vector<std::shared_ptr<SetClause>> sv_set_clauses;

  std::shared_ptr<BinaryExpr>              sv_cond;
  std::vector<std::shared_ptr<BinaryExpr>> sv_conds;

  std::shared_ptr<OrderBy> sv_orderby;

  std::shared_ptr<GroupBy> sv_groupby;
};

extern std::shared_ptr<TreeNode> njudb_ast_;

}  // namespace ast

}  // namespace njudb

#define YYSTYPE njudb::ast::SemValue

#endif  // NJUDB_AST_H
