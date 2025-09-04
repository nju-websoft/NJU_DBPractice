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

#ifndef NJUDB_PLAN_H
#define NJUDB_PLAN_H
#include <utility>

#include "common/record.h"
#include "common/condition.h"

#define TAB_STR(level) std::string(2 * level, ' ')

namespace njudb {
class AbstractPlan
{
public:
  virtual ~AbstractPlan() = default;

  virtual auto ToString(int level) const -> std::string { NJUDB_FATAL("Unimplemented ToString(level) in AbstractPlan"); }
};

class ExplainPlan : public AbstractPlan
{
public:
  explicit ExplainPlan(std::shared_ptr<AbstractPlan> plan) : logical_plan_(std::move(plan)) {}

  std::shared_ptr<AbstractPlan> logical_plan_;
};

class CreateDBPlan : public AbstractPlan
{
public:
  explicit CreateDBPlan(std::string db_name) : db_name_(std::move(db_name)) {}
  auto ToString(int level) const -> std::string override
  {
    return fmt::format("{}CreateDBPlan [{}]", TAB_STR(level), db_name_);
  }
  std::string db_name_;
};

class OpenDBPlan : public AbstractPlan
{
public:
  explicit OpenDBPlan(std::string db_name) : db_name_(std::move(db_name)) {}
  auto ToString(int level) const -> std::string override
  {
    return fmt::format("{}OpenDBPlan [{}]", TAB_STR(level), db_name_);
  }
  std::string db_name_;
};

class CreateTablePlan : public AbstractPlan
{
public:
  CreateTablePlan(std::string table_name, RecordSchemaUptr schema, StorageModel storage)
      : table_name_(std::move(table_name)), schema_(std::move(schema)), storage_(storage)
  {}

  auto ToString(int level) const -> std::string override
  {
    return fmt::format("{}CreateTablePlan [{}] <{}>", TAB_STR(level), table_name_, schema_->ToString());
  }

  std::string      table_name_;
  RecordSchemaUptr schema_;
  StorageModel     storage_;
};

class DropTablePlan : public AbstractPlan
{
public:
  explicit DropTablePlan(std::string table_name) : table_name_(std::move(table_name)) {}

  auto ToString(int level) const -> std::string override
  {
    return fmt::format("{}DropTablePlan [{}]", TAB_STR(level), table_name_);
  }

  std::string table_name_;
};

class DescTablePlan : public AbstractPlan
{
public:
  explicit DescTablePlan(std::string table_name) : table_name_(std::move(table_name)) {}
  auto ToString(int level) const -> std::string override
  {
    return fmt::format("{}DescTablePlan [{}]", TAB_STR(level), table_name_);
  }
  std::string table_name_;
};

class ShowTablesPlan : public AbstractPlan
{
  auto ToString(int level) const -> std::string override { return fmt::format("{}ShowTablesPlan", TAB_STR(level)); }
};

class CreateIndexPlan : public AbstractPlan
{
public:
  CreateIndexPlan(std::string index_name, std::string table_name, RecordSchemaUptr key_schema, IndexType index_type)
      : index_type_(index_type),
        index_name_(std::move(index_name)),
        table_name_(std::move(table_name)),
        key_schema_(std::move(key_schema))
  {}

  auto ToString(int level) const -> std::string override
  {
    return fmt::format("{}CreateIndexPlan [{}] <{}> [{}] [{}]",
        TAB_STR(level),
        index_name_,
        IndexTypeToString(index_type_),
        table_name_,
        key_schema_->ToString());
  }
  IndexType        index_type_;
  std::string      index_name_;
  std::string      table_name_;
  RecordSchemaUptr key_schema_;
};

class DropIndexPlan : public AbstractPlan
{
public:
  DropIndexPlan(std::string table_name, std::string index_name)
      : table_name_(std::move(table_name)), index_name_(std::move(index_name))
  {}
  auto ToString(int level) const -> std::string override
  {
    return fmt::format("{}DropIndexPlan [{}] <{}>", TAB_STR(level), table_name_, index_name_);
  }
  std::string table_name_;
  std::string index_name_;
};

class ShowIndexesPlan : public AbstractPlan
{
public:
  ShowIndexesPlan() : table_name_("") {}  // Show all indexes
  explicit ShowIndexesPlan(std::string table_name) : table_name_(std::move(table_name)) {}
  auto ToString(int level) const -> std::string override
  {
    if (table_name_.empty()) {
      return fmt::format("{}ShowIndexesPlan [ALL]", TAB_STR(level));
    }
    return fmt::format("{}ShowIndexesPlan [{}]", TAB_STR(level), table_name_);
  }
  std::string table_name_;
};

class InsertPlan : public AbstractPlan
{
public:
  InsertPlan(std::string table_name, std::vector<ValueSptr> values)
      : table_name_(std::move(table_name)), values_(std::move(values))
  {}
  auto ToString(int level) const -> std::string override
  {
    std::string value_str;
    for (const auto &value : values_) {
      value_str += value->ToString() + ", ";
    }
    value_str.back() = ')';
    value_str        = "(" + value_str;
    return fmt::format("{}InsertPlan [{}] <{}>", TAB_STR(level), table_name_, value_str);
  }
  std::string            table_name_;
  std::vector<ValueSptr> values_;
};

class UpdatePlan : public AbstractPlan
{
public:
  UpdatePlan(
      std::shared_ptr<AbstractPlan> child, std::string table_name, std::vector<std::pair<RTField, ValueSptr>> updates)
      : child_(std::move(child)), table_name_(std::move(table_name)), updates_(std::move(updates))
  {}
  auto ToString(int level) const -> std::string override
  {
    std::string value_str;
    for (const auto &update : updates_) {
      value_str += update.first.ToString() + " = " + update.second->ToString() + ", ";
    }
    return fmt::format(
        "{}UpdatePlan [{}] <{}>\n{}", TAB_STR(level), table_name_, value_str, child_->ToString(level + 1));
  }
  std::shared_ptr<AbstractPlan>              child_;
  std::string                                table_name_;
  std::vector<std::pair<RTField, ValueSptr>> updates_;
};

class DeletePlan : public AbstractPlan
{
public:
  DeletePlan(std::shared_ptr<AbstractPlan> child, std::string table_name)
      : child_(std::move(child)), table_name_(std::move(table_name))
  {}
  auto ToString(int level) const -> std::string override
  {
    return fmt::format("{}DeletePlan [{}]\n{}", TAB_STR(level), table_name_, child_->ToString(level + 1));
  }
  std::shared_ptr<AbstractPlan> child_;
  std::string                   table_name_;
};

class FilterPlan : public AbstractPlan
{
public:
  FilterPlan(std::shared_ptr<AbstractPlan> child, ConditionVec conds)
      : child_(std::move(child)), conds_(std::move(conds))
  {}
  auto ToString(int level) const -> std::string override
  {
    std::string cond_str;
    if (!conds_.empty()) {
      cond_str += conds_.front().ToString();
      for (size_t i = 1; i < conds_.size(); i++) {
        cond_str += " AND " + conds_[i].ToString();
      }
    }
    return fmt::format("{}FilterPlan <{}>\n{}", TAB_STR(level), cond_str, child_->ToString(level + 1));
  }
  std::shared_ptr<AbstractPlan> child_;
  ConditionVec                  conds_;
};

class ScanPlan : public AbstractPlan
{
public:
  explicit ScanPlan(std::string table_name) : table_name_(std::move(table_name)) {}
  auto ToString(int level) const -> std::string override
  {
    return fmt::format("{}ScanPlan [{}]", TAB_STR(level), table_name_);
  }
  std::string table_name_;
};

class IdxScanPlan : public AbstractPlan
{
public:
  IdxScanPlan(std::string table_name, idx_id_t idxId, ConditionVec conds, bool is_ascending)
      : table_name_(std::move(table_name)), idx_id_(idxId), conds_(std::move(conds)), is_ascending_(is_ascending)
  {}
  auto ToString(int level) const -> std::string override
  {
    std::string cond_str;
    if (!conds_.empty()) {
      cond_str += conds_.front().ToString();
      for (size_t i = 1; i < conds_.size(); i++) {
        cond_str += " AND " + conds_[i].ToString();
      }
    }
    return fmt::format("{}IdxScanPlan [{}] <{}> <{}> <{}>",
        TAB_STR(level),
        table_name_,
        idx_id_,
        cond_str,
        is_ascending_ ? "ASC" : "DESC");
  }
  std::string  table_name_;
  idx_id_t     idx_id_;
  ConditionVec conds_;
  bool         is_ascending_{true};  // Default to ascending order
};

class SortPlan : public AbstractPlan
{
public:
  SortPlan(std::shared_ptr<AbstractPlan> child, RecordSchemaUptr key_schema, bool is_desc)
      : child_(std::move(child)), key_schema_(std::move(key_schema)), is_desc_(is_desc)
  {}
  auto ToString(int level) const -> std::string override
  {
    return fmt::format("{}SortPlan <{}> [{}]\n{}",
        TAB_STR(level),
        key_schema_->ToString(),
        is_desc_ ? "DESC" : "ASC",
        child_->ToString(level + 1));
  }
  std::shared_ptr<AbstractPlan> child_;
  RecordSchemaUptr              key_schema_;
  bool                          is_desc_;
};

class ProjectPlan : public AbstractPlan
{
public:
  ProjectPlan(std::shared_ptr<AbstractPlan> child, const std::vector<RTField> &schema)
      : child_(std::move(child)), schema_(std::make_unique<RecordSchema>(schema))
  {}
  auto ToString(int level) const -> std::string override
  {
    return fmt::format("{}ProjectPlan <{}>\n{}", TAB_STR(level), schema_->ToString(), child_->ToString(level + 1));
  }
  std::shared_ptr<AbstractPlan> child_;
  RecordSchemaUptr              schema_;
};

class JoinPlan : public AbstractPlan
{
public:
  JoinPlan(std::shared_ptr<AbstractPlan> left, std::shared_ptr<AbstractPlan> right, ConditionVec &conds, JoinType type,
      JoinStrategy strategy)
      : left_(std::move(left)), right_(std::move(right)), conds_(std::move(conds)), type_(type), strategy_(strategy)
  {}
  auto ToString(int level) const -> std::string override
  {
    std::string cond_str;
    if (!conds_.empty()) {
      cond_str += conds_.front().ToString();
      for (size_t i = 1; i < conds_.size(); i++) {
        cond_str += " AND " + conds_[i].ToString();
      }
    }
    return fmt::format("{}JoinPlan <conds: {}, type: {}, strategy: {}>\n{}\n{}",
        TAB_STR(level),
        cond_str,
        JoinTypeToString(type_),
        JoinStrategyToString(strategy_),
        left_->ToString(level + 1),
        right_->ToString(level + 1));
  }
  std::shared_ptr<AbstractPlan> left_;
  std::shared_ptr<AbstractPlan> right_;
  ConditionVec                  conds_;
  JoinType                      type_;
  JoinStrategy                  strategy_;
  // below is available when strategy == SortMerge or HashJoin
  RecordSchemaUptr left_key_schema_;
  RecordSchemaUptr right_key_schema_;
  CompOp           join_op_{OP_EQ};  // used in SortMergeJoin and HashJoin
};

class AggregatePlan : public AbstractPlan
{
public:
  AggregatePlan(std::shared_ptr<AbstractPlan> child, std::vector<RTField> group_fields, std::vector<RTField> agg_fields)
      : child_(std::move(child)), group_fields_(std::move(group_fields)), agg_fields(std::move(agg_fields))
  {}
  auto ToString(int level) const -> std::string override
  {
    std::string group_fields_str = "group fields: ";
    std::string agg_fields_str   = "agg fields: ";
    for (const auto &field : group_fields_) {
      group_fields_str += field.ToString() + ", ";
    }
    if (group_fields_.empty())
      group_fields_str += "No group fields";
    else {
      group_fields_str.pop_back();
      group_fields_str.pop_back();
    }
    for (const auto &field : agg_fields) {
      agg_fields_str += field.ToString() + ", ";
    }
    if (agg_fields.empty())
      agg_fields_str += "No agg fields";
    else {
      agg_fields_str.pop_back();
      agg_fields_str.pop_back();
    }
    return fmt::format(
        "{}AggregatePlan <{}> <{}>\n{}", TAB_STR(level), group_fields_str, agg_fields_str, child_->ToString(level + 1));
  }
  std::shared_ptr<AbstractPlan> child_;
  std::vector<RTField>          group_fields_;
  std::vector<RTField>          agg_fields;
};

class LimitPlan : public AbstractPlan
{
public:
  LimitPlan(std::shared_ptr<AbstractPlan> child, size_t limit) : child_(std::move(child)), limit_(limit) {}
  auto ToString(int level) const -> std::string override
  {
    return fmt::format(
        "{}LimitPlan <{}>\n{}", TAB_STR(level), fmt::format("limit to {}", limit_), child_->ToString(level + 1));
  }
  std::shared_ptr<AbstractPlan> child_;
  size_t                        limit_;
};

}  // namespace njudb

#endif  // NJUDB_PLAN_H
