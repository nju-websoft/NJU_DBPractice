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

#ifndef WSDB_PLAN_H
#define WSDB_PLAN_H
#include <utility>

#include "system/handle/record_handle.h"
#include "common/condition.h"

namespace wsdb {
class AbstractPlan
{
public:
  virtual ~AbstractPlan() = default;
};

class CreateDBPlan : public AbstractPlan
{
public:
  explicit CreateDBPlan(std::string db_name) : db_name_(std::move(db_name)) {}

  std::string db_name_;
};

class OpenDBPlan : public AbstractPlan
{
public:
  explicit OpenDBPlan(std::string db_name) : db_name_(std::move(db_name)) {}

  std::string db_name_;
};

class CreateTablePlan : public AbstractPlan
{
public:
  CreateTablePlan(std::string table_name, RecordSchemaUptr schema, StorageModel storage)
      : table_name_(std::move(table_name)), schema_(std::move(schema)), storage_(storage)
  {}

  std::string      table_name_;
  RecordSchemaUptr schema_;
  StorageModel     storage_;
};

class DropTablePlan : public AbstractPlan
{
public:
  explicit DropTablePlan(std::string table_name) : table_name_(std::move(table_name)) {}

  std::string table_name_;
};

class DescTablePlan : public AbstractPlan
{
public:
  explicit DescTablePlan(std::string table_name) : table_name_(std::move(table_name)) {}

  std::string table_name_;
};

class ShowTablesPlan : public AbstractPlan
{};

class InsertPlan : public AbstractPlan
{
public:
  InsertPlan(std::string table_name, std::vector<ValueSptr> values)
      : table_name_(std::move(table_name)), values_(std::move(values))
  {}
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

  std::shared_ptr<AbstractPlan> child_;
  std::string                   table_name_;
};

class FilterPlan : public AbstractPlan
{
public:
  FilterPlan(std::shared_ptr<AbstractPlan> child, ConditionVec conds)
      : child_(std::move(child)), conds_(std::move(conds))
  {}

  std::shared_ptr<AbstractPlan> child_;
  ConditionVec                  conds_;
};

class ScanPlan : public AbstractPlan
{
public:
  explicit ScanPlan(std::string table_name) : table_name_(std::move(table_name)) {}

  std::string table_name_;
};

class IdxScanPlan : public AbstractPlan
{
public:
  IdxScanPlan(std::string table_name, idx_id_t idxId, ConditionVec conds, int matched_fields)
      : table_name_(std::move(table_name)), idx_id_(idxId), conds_(std::move(conds)), matched_fields_(matched_fields)
  {}

  std::string  table_name_;
  idx_id_t     idx_id_;
  ConditionVec conds_;
  int          matched_fields_;
};

class SortPlan : public AbstractPlan
{
public:
  SortPlan(std::shared_ptr<AbstractPlan> child, RecordSchemaUptr key_schema, bool is_desc)
      : child_(std::move(child)), key_schema_(std::move(key_schema)), is_desc_(is_desc)
  {}

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

  std::shared_ptr<AbstractPlan> left_;
  std::shared_ptr<AbstractPlan> right_;
  ConditionVec                  conds_;
  JoinType                      type_;
  JoinStrategy                  strategy_;
  // below is available when strategy == SortMerge
  RecordSchemaUptr left_key_schema_;
  RecordSchemaUptr right_key_schema_;
};

class AggregatePlan : public AbstractPlan
{
public:
  AggregatePlan(std::shared_ptr<AbstractPlan> child, std::vector<RTField> group_fields, std::vector<RTField> agg_fields)
      : child_(std::move(child)), group_fields_(std::move(group_fields)), agg_fields(std::move(agg_fields))
  {}
  std::shared_ptr<AbstractPlan> child_;
  std::vector<RTField>          group_fields_;
  std::vector<RTField>          agg_fields;
};

class LimitPlan : public AbstractPlan
{
public:
  LimitPlan(std::shared_ptr<AbstractPlan> child, size_t limit) : child_(std::move(child)), limit_(limit) {}

  std::shared_ptr<AbstractPlan> child_;
  size_t                        limit_;
};

}  // namespace wsdb

#endif  // WSDB_PLAN_H
