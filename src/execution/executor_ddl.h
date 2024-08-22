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
// Created by ziqi on 2024/8/3.
//

#ifndef WSDB_EXECUTOR_DDL_H
#define WSDB_EXECUTOR_DDL_H

#include <utility>

#include "system/handle/database_handle.h"
#include "executor_abstract.h"

namespace wsdb {
class CreateTableExecutor : public AbstractExecutor
{
public:
  CreateTableExecutor(std::string table_name, RecordSchemaUptr schema, DatabaseHandle *db, StorageModel storage);

  void Init() override;

  void Next() override;

  [[nodiscard]] auto IsEnd() const -> bool override;

private:
  std::string      tab_name_;
  RecordSchemaUptr schema_;
  StorageModel     storage_;
  DatabaseHandle  *db_;

private:
  bool is_end_;
};

class DropTableExecutor : public AbstractExecutor
{
public:
  explicit DropTableExecutor(std::string table_name, DatabaseHandle *db);

  void Init() override;

  void Next() override;

  [[nodiscard]] auto IsEnd() const -> bool override;

private:
  std::string     tab_name_;
  DatabaseHandle *db_;

private:
  bool is_end_;
};

class DescTableExecutor : public AbstractExecutor
{
public:
  explicit DescTableExecutor(TableHandle *tbl_hdl);

  void Init() override;

  void Next() override;

  [[nodiscard]] auto IsEnd() const -> bool override;

private:
  TableHandle    *tab_hdl_;

private:
  bool is_end_;
  size_t cursor_;
};

class ShowTablesExecutor : public AbstractExecutor
{
public:
  explicit ShowTablesExecutor(DatabaseHandle *db);

  void Init() override;

  void Next() override;

  [[nodiscard]] auto IsEnd() const -> bool override;

private:
  DatabaseHandle *db_;

private:
  bool   is_end_;
  size_t cursor_;
};

}  // namespace wsdb

#endif  // WSDB_EXECUTOR_DDL_H
