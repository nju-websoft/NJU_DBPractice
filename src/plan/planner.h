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

#ifndef WSDB_ANALYSER_H
#define WSDB_ANALYSER_H

#include <utility>

#include "parser/ast.h"
#include "plan.h"
#include "system/context.h"

namespace wsdb {

class Planner
{
public:
  Planner() = default;

  DISABLE_COPY_MOVE_AND_ASSIGN(Planner)

  [[nodiscard]] static auto PlanAST(
      const std::shared_ptr<ast::TreeNode> &ast, DatabaseHandle *db) -> std::shared_ptr<AbstractPlan>;

private:
  /// transform value to server defined value
  static auto TransformValue(const std::shared_ptr<ast::Value> &val) -> ValueSptr;

  /// transform non-aggregation cols (like group by and order by) into RTFields
  static auto TransformCols(const std::vector<std::shared_ptr<ast::Col>> &cols, DatabaseHandle *db,
      const std::vector<std::string> &tabs) -> std::vector<RTField>;

  /// make a condition vector form a list of binary expressions
  static auto MakeConditionVec(const std::vector<std::shared_ptr<ast::BinaryExpr>> &exprs, DatabaseHandle *db,
      const std::vector<std::string> &tabs) -> ConditionVec;

  /// make record schema for table definition
  static auto CreateRecordSchema(const std::vector<std::shared_ptr<ast::Field>> &fields, std::string &tab_name,
      DatabaseHandle *db) -> RecordSchemaUptr;

  /// check if the table has the specific field, if tab_name is empty string, fulfill tab_name by checking all tables in
  /// the database
  static void CheckFieldTabName(std::string &tab_name, const std::string &field_name, DatabaseHandle *db,
      const std::vector<std::string> &cand_tabs);

  /// Get All fields of tabs
  static auto GetAllFields(const std::vector<std::string> &tabs, DatabaseHandle *db) -> std::vector<RTField>;

  /// Analyse select statement, tabs will be filled
  static auto AnalyseSelect(const std::shared_ptr<ast::SelectStmt> &stmt, DatabaseHandle *db,
      std::vector<std::string> &tabs) -> std::shared_ptr<AbstractPlan>;

  /// Get join conditions from where clause
  static auto GetConditionsForJoin(
      const std::string &left, const std::string &right, ConditionVec &conds, DatabaseHandle *db) -> ConditionVec;

  /// Get conditions whose lval field belongs to tab_name
  static auto GetConditionsForTable(
      const std::string &tab_name, const ConditionVec &conds, DatabaseHandle *db) -> ConditionVec;

  static auto MakeFilterScanPlan(
      const std::string &tab_name, const ConditionVec &conds) -> std::shared_ptr<AbstractPlan>;

  static auto MakeAggregatePlan(std::shared_ptr<AbstractPlan> &child, const std::vector<RTField> &group_fields,
      const std::vector<RTField> &proj_fields, const ConditionVec &havings) -> std::shared_ptr<AbstractPlan>;

  static auto MakeProjSortPlan(std::shared_ptr<AbstractPlan> &child, const std::vector<RTField> &proj_fields,
      const std::vector<RTField> &sort_fields, bool is_desc) -> std::shared_ptr<AbstractPlan>;
};
}  // namespace wsdb

#endif  // WSDB_ANALYSER_H
