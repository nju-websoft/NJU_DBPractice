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

#define MAX_TABNAME_LEN 128

#include "executor_ddl.h"
namespace wsdb {

static auto MakeTableDescOutSchema(size_t sz_db_name, size_t sz_tb_name) -> std::unique_ptr<RecordSchema>
{
  std::vector<RTField> fields(6);
  // 5 fields, db name, table name, field num, record length, storage model, index num
  fields[0] = RTField{.field_ = {.table_id_ = INVALID_TABLE_ID,
                          .field_name_      = "Database",
                          .field_size_      = sz_db_name,
                          .field_type_      = TYPE_STRING}};
  fields[1] = RTField{.field_ = {.table_id_ = INVALID_TABLE_ID,
                          .field_name_      = "Table",
                          .field_size_      = sz_tb_name,
                          .field_type_      = TYPE_STRING}};
  fields[2] = RTField{.field_ = {.table_id_ = INVALID_TABLE_ID,
                          .field_name_      = "FieldNum",
                          .field_size_      = sizeof(size_t),
                          .field_type_      = TYPE_INT}};
  fields[3] = RTField{.field_ = {.table_id_ = INVALID_TABLE_ID,
                          .field_name_      = "RecordLength",
                          .field_size_      = sizeof(size_t),
                          .field_type_      = TYPE_INT}};
  fields[4] = RTField{
      .field_ = {
          .table_id_ = INVALID_TABLE_ID, .field_name_ = "StorageModel", .field_size_ = 10, .field_type_ = TYPE_STRING}};

  fields[5] = RTField{.field_ = {.table_id_ = INVALID_TABLE_ID,
                          .field_name_      = "IndexNum",
                          .field_size_      = sizeof(size_t),
                          .field_type_      = TYPE_INT}};
  return std::make_unique<RecordSchema>(fields);
}

static auto MakeTableDescValue(const std::string &db_name, const std::string &tb_name, size_t field_num,
    size_t record_length, const char *storage_model, size_t idx_num) -> std::vector<ValueSptr>
{
  std::vector<ValueSptr> values(6);
  values[0] = ValueFactory::CreateStringValue(db_name.c_str(), db_name.size());
  values[1] = ValueFactory::CreateStringValue(tb_name.c_str(), tb_name.size());
  values[2] = ValueFactory::CreateIntValue(static_cast<int>(field_num));
  values[3] = ValueFactory::CreateIntValue(static_cast<int>(record_length));
  values[4] = ValueFactory::CreateStringValue(storage_model, strlen(storage_model));
  values[5] = ValueFactory::CreateIntValue(static_cast<int>(idx_num));
  return values;
}

/// CreateTableExecutor
CreateTableExecutor::CreateTableExecutor(
    std::string table_name, wsdb::RecordSchemaUptr schema, wsdb::DatabaseHandle *db, StorageModel storage)
    : AbstractExecutor(DDL),
      tab_name_(std::move(table_name)),
      schema_(std::move(schema)),
      storage_(storage),
      db_(db),
      is_end_(false)
{
  out_schema_ = MakeTableDescOutSchema(db_->GetName().size(), tab_name_.size());
}

void CreateTableExecutor::Init() { WSDB_FETAL("CreateTableExecutor does not support Init"); }
void CreateTableExecutor::Next()
{
  if (is_end_) {
    WSDB_FETAL("CreateTableExecutor is end");
  }
  if (db_->GetTable(tab_name_) != nullptr) {
    WSDB_THROW(WSDB_TABLE_EXIST, tab_name_);
  }
  db_->CreateTable(tab_name_, *schema_, storage_);
  auto values = MakeTableDescValue(db_->GetName(),
      tab_name_,
      schema_->GetFieldCount(),
      schema_->GetRecordLength(),
      StorageModelToString(storage_),
      0);
  record_     = std::make_unique<Record>(out_schema_.get(), values, INVALID_RID);
  is_end_     = true;
}
auto CreateTableExecutor::IsEnd() const -> bool { return is_end_; }

/// DropTable Executor
DropTableExecutor::DropTableExecutor(std::string table_name, wsdb::DatabaseHandle *db)
    : AbstractExecutor(DDL), tab_name_(std::move(table_name)), db_(db), is_end_(false)
{
  out_schema_ = MakeTableDescOutSchema(db_->GetName().size(), tab_name_.size());
}

void DropTableExecutor::Init() { WSDB_FETAL("DropTableExecutor does not support Init"); }
void DropTableExecutor::Next()
{
  if (is_end_) {
    WSDB_FETAL("DropTableExecutor is end");
  }
  auto tab = db_->GetTable(tab_name_);
  if (tab == nullptr) {
    WSDB_THROW(WSDB_TABLE_MISS, tab_name_);
  }
  auto values = MakeTableDescValue(db_->GetName(),
      tab_name_,
      tab->GetSchema().GetFieldCount(),
      tab->GetSchema().GetRecordLength(),
      StorageModelToString(tab->GetStorageModel()),
      db_->GetIndexNum(tab->GetTableId()));
  record_     = std::make_unique<Record>(out_schema_.get(), values, INVALID_RID);
  db_->DropTable(tab_name_);
  is_end_ = true;
}
auto DropTableExecutor::IsEnd() const -> bool { return is_end_; }

/// DescTable Executor

DescTableExecutor::DescTableExecutor(wsdb::TableHandle *tbl_hdl)
    : AbstractExecutor(DDL), tab_hdl_(tbl_hdl), is_end_(false), cursor_(0)
{
  // desc table header is | Field | type | Null
  std::vector<RTField> fields(3);
  fields[0] = RTField{
      .field_ = {
          .table_id_ = INVALID_TABLE_ID, .field_name_ = "Field", .field_size_ = 128, .field_type_ = TYPE_STRING}};
  fields[1] = RTField{
      .field_ = {.table_id_ = INVALID_TABLE_ID, .field_name_ = "Type", .field_size_ = 20, .field_type_ = TYPE_STRING}};
  fields[2] = RTField{
      .field_ = {.table_id_ = INVALID_TABLE_ID, .field_name_ = "Null", .field_size_ = 10, .field_type_ = TYPE_STRING}};
  out_schema_ = std::make_unique<RecordSchema>(fields);
}

void DescTableExecutor::Init() { WSDB_FETAL("DescTableExecutor does not support Init"); }
void DescTableExecutor::Next()
{
  if (IsEnd()) {
    WSDB_FETAL("DescTableExecutor is end");
  }
  if (tab_hdl_ == nullptr) {
    WSDB_THROW(WSDB_TABLE_MISS, "TableHandle is null");
  }
  if (cursor_ >= tab_hdl_->GetSchema().GetFieldCount()) {
    is_end_ = true;
    return;
  }
  std::vector<ValueSptr> values;
  values.reserve(out_schema_->GetFieldCount());
  auto field = tab_hdl_->GetSchema().GetFieldAt(cursor_);
  values.push_back(ValueFactory::CreateStringValue(field.field_.field_name_.c_str(), field.field_.field_name_.size()));
  values.push_back(ValueFactory::CreateStringValue(
      fmt::format("{} ({})", FieldTypeToString(field.field_.field_type_), std::to_string(field.field_.field_size_))
          .c_str(),
      20));
  values.push_back(ValueFactory::CreateStringValue(field.field_.nullable_ ? "YES" : "NO", 10));
  WSDB_ASSERT(values.size() == out_schema_->GetFieldCount(), "Value size not match");
  record_ = std::make_unique<Record>(out_schema_.get(), values, INVALID_RID);
  cursor_++;
}
auto DescTableExecutor::IsEnd() const -> bool { return is_end_; }

/// ShowTables Executor
ShowTablesExecutor::ShowTablesExecutor(wsdb::DatabaseHandle *db)
    : AbstractExecutor(DDL), db_(db), is_end_(false), cursor_(0)
{
  out_schema_ = MakeTableDescOutSchema(db_->GetName().size(), MAX_TABNAME_LEN);
}

void ShowTablesExecutor::Init() { WSDB_FETAL("ShowTablesExecutor does not support Init"); }
void ShowTablesExecutor::Next()
{
  if (is_end_) {
    WSDB_FETAL("ShowTablesExecutor is end");
  }
  auto &tables = db_->GetAllTables();
  if (cursor_ >= tables.size()) {
    is_end_ = true;
    return;
  }
  auto it = tables.begin();
  std::advance(it, cursor_);
  auto tab_hdl = it->second.get();
  auto values  = MakeTableDescValue(db_->GetName(),
      tab_hdl->GetTableName(),
      tab_hdl->GetSchema().GetFieldCount(),
      tab_hdl->GetSchema().GetRecordLength(),
      StorageModelToString(tab_hdl->GetStorageModel()),
      db_->GetIndexNum(tab_hdl->GetTableId()));
  record_      = std::make_unique<Record>(out_schema_.get(), values, INVALID_RID);
  cursor_++;
}
auto ShowTablesExecutor::IsEnd() const -> bool { return is_end_; }

}  // namespace wsdb
