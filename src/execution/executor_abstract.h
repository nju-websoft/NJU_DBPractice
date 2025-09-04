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
// Created by ziqi on 2024/7/31.
//

#ifndef NJUDB_EXECUTOR_ABSTRACT_H
#define NJUDB_EXECUTOR_ABSTRACT_H

#include "../../common/error.h"
#include "../../common/micro.h"
#include "common/record.h"

namespace njudb {

enum ExecutorType
{
  DDL = 1,  // Data Definition Language
  DML,      // Data Manipulation Language
  Basic,    // basic executors
  TXN,      // Transaction
};

class AbstractExecutor
{
public:
  AbstractExecutor() = delete;

  /**
   * Constructor should implement out schema definition and some basic initialization
   * @param type
   */
  explicit AbstractExecutor(ExecutorType type) : type_(type) {}

  virtual ~AbstractExecutor() = default;

  virtual void Init() = 0;

  virtual void Next() = 0;

  [[nodiscard]] virtual auto IsEnd() const -> bool = 0;

  [[nodiscard]] virtual auto GetOutSchema() const -> const RecordSchema *
  {
    NJUDB_ASSERT(out_schema_ != nullptr, "out_schema_ is nullptr");
    return out_schema_.get();
  };

  [[nodiscard]] auto GetType() const -> ExecutorType { return type_; }

  [[nodiscard]] auto GetRecord() -> RecordUptr
  {
    if (record_ == nullptr) {
      return nullptr;
    }
    return std::make_unique<Record>(*record_);
  };

protected:
  RecordSchemaUptr out_schema_;
  RecordUptr       record_;

private:
  ExecutorType type_;
};

DEFINE_UNIQUE_PTR(AbstractExecutor);

}  // namespace njudb

#endif  // NJUDB_EXECUTOR_ABSTRACT_H
