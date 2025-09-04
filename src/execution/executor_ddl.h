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

#ifndef NJUDB_EXECUTOR_DDL_H
#define NJUDB_EXECUTOR_DDL_H

#include <utility>

#include "system/handle/database_handle.h"
#include "executor_abstract.h"

namespace njudb {
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
  TableHandle *tab_hdl_;
  
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
  bool   is_end_;
  size_t cursor_;
};

class CreateIndexExecutor : public AbstractExecutor
{
public:
  CreateIndexExecutor(const std::string &index_name, const std::string &table_name, RecordSchemaUptr key_schema,
      IndexType index_type, DatabaseHandle *db);
  void Init() override;

  void Next() override;

  [[nodiscard]] auto IsEnd() const -> bool override;

private:
  std::string      index_name_;
  std::string      table_name_;
  RecordSchemaUptr key_schema_;
  IndexType        index_type_;
  DatabaseHandle  *db_;

  bool is_end_;
};

class DropIndexExecutor : public AbstractExecutor
{
public:
  DropIndexExecutor(std::string table_name, std::string index_name, DatabaseHandle *db);

  void Init() override;

  void Next() override;

  [[nodiscard]] auto IsEnd() const -> bool override;

private:
  std::string     table_name_;
  std::string     index_name_;
  DatabaseHandle *db_;

  bool is_end_;
};

class ShowIndexesExecutor : public AbstractExecutor
{
public:
  explicit ShowIndexesExecutor(DatabaseHandle *db);
  explicit ShowIndexesExecutor(const std::string &table_name, DatabaseHandle *db);

  void Init() override;

  void Next() override;

  [[nodiscard]] auto IsEnd() const -> bool override;

private:
  // if table name is empty, show all indexes in the database
  std::string table_name_;
  DatabaseHandle *db_;

  std::vector<IndexHandle*> indexes_to_show_;
  bool   is_end_;
  size_t cursor_;
};

}  // namespace njudb

#endif  // NJUDB_EXECUTOR_DDL_H
