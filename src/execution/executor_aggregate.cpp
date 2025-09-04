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
// Created by ziqi on 2024/8/5.
//

#include "executor_aggregate.h"

namespace njudb {

AggregateExecutor::AggregateValue::AggregateValue(RecordSchema *schema) : schema_(schema) { NJUDB_STUDENT_TODO(l3, t3); }
AggregateExecutor::AggregateValue::AggregateValue(RecordSchema *schema, const Record &record)
{
  NJUDB_STUDENT_TODO(l3, t3);
}
void AggregateExecutor::AggregateValue::CombineWith(const AggregateExecutor::AggregateValue &other)
{
  NJUDB_STUDENT_TODO(l3, t3);
}

auto AggregateExecutor::AggregateValue::Values() const -> const std::vector<ValueSptr> & { return values_; }

void AggregateExecutor::AggregateValue::Finalize() { NJUDB_STUDENT_TODO(l3, t3); }

AggregateExecutor::AggregateExecutor(
    AbstractExecutorUptr child, RecordSchemaUptr agg_schema, RecordSchemaUptr group_schema)
    : AbstractExecutor(Basic),
      child_(std::move(child)),
      agg_schema_(std::move(agg_schema)),
      group_schema_(std::move(group_schema))
{
  std::vector<RTField> fields;
  for (const auto &field : group_schema_->GetFields()) {
    fields.push_back(field);
  }
  for (const auto &field : agg_schema_->GetFields()) {
    fields.push_back(field);
  }
  out_schema_ = std::make_unique<RecordSchema>(fields);
}

void AggregateExecutor::Init() { NJUDB_STUDENT_TODO(l3, t3); }

void AggregateExecutor::Next() { NJUDB_STUDENT_TODO(l3, t3); }

auto AggregateExecutor::IsEnd() const -> bool { NJUDB_STUDENT_TODO(l3, t3); }

}  // namespace njudb