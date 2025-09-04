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

#ifndef NJUDB_EXECUTOR_AGGREGATE_H
#define NJUDB_EXECUTOR_AGGREGATE_H
#include <unordered_map>
#include "executor_abstract.h"

namespace njudb {

class AggregateExecutor : public AbstractExecutor
{
public:
  AggregateExecutor(AbstractExecutorUptr child, RecordSchemaUptr agg_schema, RecordSchemaUptr group_schema);

  void Init() override;

  void Next() override;

  [[nodiscard]] auto IsEnd() const -> bool override;

private:
  // aggregate value behaves like a writable record
  class AggregateValue
  {
  public:
    AggregateValue() : schema_(nullptr) {}

    /**
     * create aggregate initial value according to schema
     * @param schema
     */
    explicit AggregateValue(RecordSchema *schema);

    /**
     * create aggregate value according to schema and record
     * @param schema
     * @param record
     */
    AggregateValue(RecordSchema *schema, const Record &record);

    void CombineWith(const AggregateValue &other);

    [[nodiscard]] auto Values() const -> const std::vector<ValueSptr> &;

    void Finalize();

    virtual ~AggregateValue() = default;

  private:
    RecordSchema          *schema_;
    std::vector<ValueSptr> values_;
    bool                   summarized_ = false;
    // count the number of non-null values for avg calculation to avoid adding repeated count(field)
    // maybe redundant when count(field) exists in the schema, but better than reconstructing the schema
    std::unordered_map<size_t, int> avg_count_map_;
  };

private:
  AbstractExecutorUptr                                 child_;
  RecordSchemaUptr                                     agg_schema_;
  RecordSchemaUptr                                     group_schema_;
  std::unordered_map<Record, AggregateValue>           group_map_;
  std::unordered_map<Record, AggregateValue>::iterator group_iter_;
};

}  // namespace njudb

#endif  // NJUDB_EXECUTOR_AGGREGATE_H
