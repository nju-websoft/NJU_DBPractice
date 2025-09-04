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
namespace njudb {

static auto MakeTableDescOutSchema(size_t sz_db_name, size_t sz_tb_name, bool include_table_id)
    -> std::unique_ptr<RecordSchema>
{
  std::vector<RTField> fields;

  // Always include: db name, table name
  fields.push_back(RTField{.field_ = {.table_id_ = INVALID_TABLE_ID,
                               .field_name_      = "Database",
                               .field_size_      = sz_db_name,
                               .field_type_      = TYPE_STRING}});
  fields.push_back(RTField{.field_ = {.table_id_ = INVALID_TABLE_ID,
                               .field_name_      = "Table",
                               .field_size_      = sz_tb_name,
                               .field_type_      = TYPE_STRING}});

  // Conditionally include table id
  if (include_table_id) {
    fields.push_back(RTField{.field_ = {.table_id_ = INVALID_TABLE_ID,
                                 .field_name_      = "TableID",
                                 .field_size_      = sizeof(size_t),
                                 .field_type_      = TYPE_INT}});
  }

  // Always include: field num, record length, storage model, index num
  fields.push_back(RTField{.field_ = {.table_id_ = INVALID_TABLE_ID,
                               .field_name_      = "FieldNum",
                               .field_size_      = sizeof(size_t),
                               .field_type_      = TYPE_INT}});
  fields.push_back(RTField{.field_ = {.table_id_ = INVALID_TABLE_ID,
                               .field_name_      = "RecordLength",
                               .field_size_      = sizeof(size_t),
                               .field_type_      = TYPE_INT}});
  fields.push_back(RTField{.field_ = {.table_id_ = INVALID_TABLE_ID,
                               .field_name_      = "StorageModel",
                               .field_size_      = 10,
                               .field_type_      = TYPE_STRING}});
  fields.push_back(RTField{.field_ = {.table_id_ = INVALID_TABLE_ID,
                               .field_name_      = "IndexNum",
                               .field_size_      = sizeof(size_t),
                               .field_type_      = TYPE_INT}});

  return std::make_unique<RecordSchema>(fields);
}

static auto MakeTableDescValue(const std::string &db_name, const std::string &tb_name, table_id_t table_id,
    size_t field_num, size_t record_length, const char *storage_model, size_t idx_num, bool include_table_id)
    -> std::vector<ValueSptr>
{
  std::vector<ValueSptr> values;

  // Always include: db name, table name
  values.push_back(ValueFactory::CreateStringValue(db_name.c_str(), db_name.size()));
  values.push_back(ValueFactory::CreateStringValue(tb_name.c_str(), tb_name.size()));

  // Conditionally include table id
  if (include_table_id) {
    values.push_back(ValueFactory::CreateIntValue(static_cast<int>(table_id)));
  }

  // Always include: field num, record length, storage model, index num
  values.push_back(ValueFactory::CreateIntValue(static_cast<int>(field_num)));
  values.push_back(ValueFactory::CreateIntValue(static_cast<int>(record_length)));
  values.push_back(ValueFactory::CreateStringValue(storage_model, strlen(storage_model)));
  values.push_back(ValueFactory::CreateIntValue(static_cast<int>(idx_num)));

  return values;
}

/// CreateTableExecutor
CreateTableExecutor::CreateTableExecutor(
    std::string table_name, njudb::RecordSchemaUptr schema, njudb::DatabaseHandle *db, StorageModel storage)
    : AbstractExecutor(DDL),
      tab_name_(std::move(table_name)),
      schema_(std::move(schema)),
      storage_(storage),
      db_(db),
      is_end_(false)
{
  out_schema_ =
      MakeTableDescOutSchema(db_->GetName().size(), tab_name_.size(), false);  // Don't show table ID for CREATE TABLE
}

void CreateTableExecutor::Init()
{
  if (db_->GetTable(tab_name_) != nullptr) {
    NJUDB_THROW(NJUDB_TABLE_EXIST, tab_name_);
  }
  db_->CreateTable(tab_name_, *schema_, storage_);
  auto values = MakeTableDescValue(db_->GetName(),
      tab_name_,
      INVALID_TABLE_ID,  // Will be filled after creation
      schema_->GetFieldCount(),
      schema_->GetRecordLength(),
      StorageModelToString(storage_),
      0,
      false);  // Don't include table ID for CREATE TABLE
  record_     = std::make_unique<Record>(out_schema_.get(), values, INVALID_RID);
}
void CreateTableExecutor::Next() { is_end_ = true; }
auto CreateTableExecutor::IsEnd() const -> bool { return is_end_; }

/// DropTable Executor
DropTableExecutor::DropTableExecutor(std::string table_name, njudb::DatabaseHandle *db)
    : AbstractExecutor(DDL), tab_name_(std::move(table_name)), db_(db), is_end_(false)
{
  out_schema_ =
      MakeTableDescOutSchema(db_->GetName().size(), tab_name_.size(), false);  // Don't show table ID for DROP TABLE
}

void DropTableExecutor::Init()
{
  auto tab = db_->GetTable(tab_name_);
  if (tab == nullptr) {
    NJUDB_THROW(NJUDB_TABLE_MISS, tab_name_);
  }
  auto values = MakeTableDescValue(db_->GetName(),
      tab_name_,
      tab->GetTableId(),
      tab->GetSchema().GetFieldCount(),
      tab->GetSchema().GetRecordLength(),
      StorageModelToString(tab->GetStorageModel()),
      db_->GetIndexNum(tab->GetTableId()),
      false);  // Don't include table ID for DROP TABLE
  record_     = std::make_unique<Record>(out_schema_.get(), values, INVALID_RID);
  db_->DropTable(tab_name_);
}
void DropTableExecutor::Next() { is_end_ = true; }
auto DropTableExecutor::IsEnd() const -> bool { return is_end_; }

/// DescTable Executor

DescTableExecutor::DescTableExecutor(njudb::TableHandle *tbl_hdl) : AbstractExecutor(DDL), tab_hdl_(tbl_hdl), cursor_(0)
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

void DescTableExecutor::Init()
{
  if (tab_hdl_ == nullptr) {
    NJUDB_THROW(NJUDB_TABLE_MISS, "TableHandle is null");
  }
  if (IsEnd()) {
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
  NJUDB_ASSERT(values.size() == out_schema_->GetFieldCount(), "Value size not match");
  record_ = std::make_unique<Record>(out_schema_.get(), values, INVALID_RID);
  cursor_++;
}
void DescTableExecutor::Next()
{
  if (IsEnd()) {
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
  NJUDB_ASSERT(values.size() == out_schema_->GetFieldCount(), "Value size not match");
  record_ = std::make_unique<Record>(out_schema_.get(), values, INVALID_RID);
  cursor_++;
}
auto DescTableExecutor::IsEnd() const -> bool { return cursor_ >= tab_hdl_->GetSchema().GetFieldCount(); }

/// ShowTables Executor
ShowTablesExecutor::ShowTablesExecutor(njudb::DatabaseHandle *db)
    : AbstractExecutor(DDL), db_(db), is_end_(false), cursor_(0)
{
  out_schema_ = MakeTableDescOutSchema(db_->GetName().size(), MAX_TABNAME_LEN, true);  // Show table ID for SHOW TABLES
}

void ShowTablesExecutor::Init()
{
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
      tab_hdl->GetTableId(),
      tab_hdl->GetSchema().GetFieldCount(),
      tab_hdl->GetSchema().GetRecordLength(),
      StorageModelToString(tab_hdl->GetStorageModel()),
      db_->GetIndexNum(tab_hdl->GetTableId()),
      true);  // Include table ID for SHOW TABLES
  record_      = std::make_unique<Record>(out_schema_.get(), values, INVALID_RID);
  cursor_++;
}
void ShowTablesExecutor::Next()
{
  if (is_end_) {
    NJUDB_FATAL("ShowTablesExecutor is end");
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
      tab_hdl->GetTableId(),
      tab_hdl->GetSchema().GetFieldCount(),
      tab_hdl->GetSchema().GetRecordLength(),
      StorageModelToString(tab_hdl->GetStorageModel()),
      db_->GetIndexNum(tab_hdl->GetTableId()),
      true);  // Include table ID for SHOW TABLES
  record_      = std::make_unique<Record>(out_schema_.get(), values, INVALID_RID);
  cursor_++;
}
auto ShowTablesExecutor::IsEnd() const -> bool { return is_end_; }

/// Helper functions for index executors
static auto MakeIndexDescOutSchema(size_t sz_db_name, size_t sz_table_name, size_t sz_index_name, bool include_index_id)
    -> std::unique_ptr<RecordSchema>
{
  std::vector<RTField> fields;

  // Always include: database name, table name, index name
  fields.push_back(RTField{.field_ = {.table_id_ = INVALID_TABLE_ID,
                               .field_name_      = "Database",
                               .field_size_      = sz_db_name,
                               .field_type_      = TYPE_STRING}});
  fields.push_back(RTField{.field_ = {.table_id_ = INVALID_TABLE_ID,
                               .field_name_      = "Table",
                               .field_size_      = sz_table_name,
                               .field_type_      = TYPE_STRING}});
  fields.push_back(RTField{.field_ = {.table_id_ = INVALID_TABLE_ID,
                               .field_name_      = "Index",
                               .field_size_      = sz_index_name,
                               .field_type_      = TYPE_STRING}});

  // Conditionally include index id
  if (include_index_id) {
    fields.push_back(RTField{.field_ = {.table_id_ = INVALID_TABLE_ID,
                                 .field_name_      = "IndexID",
                                 .field_size_      = sizeof(size_t),
                                 .field_type_      = TYPE_INT}});
  }

  // Always include: index type, key schema description
  fields.push_back(RTField{
      .field_ = {
          .table_id_ = INVALID_TABLE_ID, .field_name_ = "IndexType", .field_size_ = 20, .field_type_ = TYPE_STRING}});
  fields.push_back(RTField{
      .field_ = {
          .table_id_ = INVALID_TABLE_ID, .field_name_ = "KeySchema", .field_size_ = 256, .field_type_ = TYPE_STRING}});

  return std::make_unique<RecordSchema>(fields);
}

static auto MakeIndexDescValue(const std::string &db_name, const std::string &table_name, const std::string &index_name,
    size_t index_id, IndexType index_type, const RecordSchema &key_schema, bool include_index_id)
    -> std::vector<ValueSptr>
{
  std::vector<ValueSptr> values;

  // Always include: db name, table name, index name
  values.push_back(ValueFactory::CreateStringValue(db_name.c_str(), db_name.size()));
  values.push_back(ValueFactory::CreateStringValue(table_name.c_str(), table_name.size()));
  values.push_back(ValueFactory::CreateStringValue(index_name.c_str(), index_name.size()));

  // Conditionally include index id
  if (include_index_id) {
    values.push_back(ValueFactory::CreateIntValue(static_cast<int>(index_id)));
  }

  // Always include: index type, key schema description
  auto index_type_str = IndexTypeToString(index_type);
  values.push_back(ValueFactory::CreateStringValue(index_type_str, strlen(index_type_str)));
  auto key_schema_str = key_schema.ToString();
  values.push_back(ValueFactory::CreateStringValue(key_schema_str.c_str(), key_schema_str.size()));

  return values;
}

/// CreateIndexExecutor
CreateIndexExecutor::CreateIndexExecutor(const std::string &index_name, const std::string &table_name,
    RecordSchemaUptr key_schema, IndexType index_type, DatabaseHandle *db)
    : AbstractExecutor(DDL),
      index_name_(index_name),
      table_name_(table_name),
      key_schema_(std::move(key_schema)),
      index_type_(index_type),
      db_(db),
      is_end_(false)
{
  out_schema_ = MakeIndexDescOutSchema(
      db_->GetName().size(), table_name_.size(), index_name_.size(), false);  // Don't show index ID for CREATE INDEX
}

void CreateIndexExecutor::Init()
{
  // Check if table exists
  auto table = db_->GetTable(table_name_);
  if (table == nullptr) {
    NJUDB_THROW(NJUDB_TABLE_MISS, table_name_);
  }

  // Create the index
  db_->CreateIndex(index_name_, table_name_, *key_schema_, index_type_);

  // Create output record
  auto values = MakeIndexDescValue(
      db_->GetName(), table_name_, index_name_, 0, index_type_, *key_schema_, false);  // Don't include index ID for
                                                                                       // CREATE INDEX
  record_ = std::make_unique<Record>(out_schema_.get(), values, INVALID_RID);
}

void CreateIndexExecutor::Next() { is_end_ = true; }

auto CreateIndexExecutor::IsEnd() const -> bool { return is_end_; }

/// DropIndexExecutor
DropIndexExecutor::DropIndexExecutor(std::string table_name, std::string index_name, DatabaseHandle *db)
    : AbstractExecutor(DDL),
      table_name_(std::move(table_name)),
      index_name_(std::move(index_name)),
      db_(db),
      is_end_(false)
{
  out_schema_ = MakeIndexDescOutSchema(
      db_->GetName().size(), table_name_.size(), index_name_.size(), false);  // Don't show index ID for DROP INDEX
}

void DropIndexExecutor::Init()
{
  // Check if table exists
  auto table = db_->GetTable(table_name_);
  if (table == nullptr) {
    NJUDB_THROW(NJUDB_TABLE_MISS, table_name_);
  }

  // Get index information before dropping it
  auto         indexes      = db_->GetIndexes(table_name_);
  IndexHandle *target_index = nullptr;
  for (auto index : indexes) {
    if (index->GetIndexName() == index_name_) {
      target_index = index;
      break;
    }
  }

  if (target_index == nullptr) {
    NJUDB_THROW(NJUDB_INDEX_MISS, index_name_);
  }

  // Create output record with index information before dropping
  auto values = MakeIndexDescValue(db_->GetName(),
      table_name_,
      index_name_,
      0,
      target_index->GetIndexType(),
      target_index->GetKeySchema(),
      false);  // Don't include index ID for DROP INDEX
  record_     = std::make_unique<Record>(out_schema_.get(), values, INVALID_RID);

  // Drop the index
  db_->DropIndex(index_name_, table_name_);
}

void DropIndexExecutor::Next() { is_end_ = true; }

auto DropIndexExecutor::IsEnd() const -> bool { return is_end_; }

/// ShowIndexesExecutor
ShowIndexesExecutor::ShowIndexesExecutor(DatabaseHandle *db)
    : AbstractExecutor(DDL), table_name_(""), db_(db), is_end_(false), cursor_(0)
{
  out_schema_ =
      MakeIndexDescOutSchema(db_->GetName().size(), MAX_TABNAME_LEN, 128, true);  // Show index ID for SHOW INDEXES
}

ShowIndexesExecutor::ShowIndexesExecutor(const std::string &table_name, DatabaseHandle *db)
    : AbstractExecutor(DDL), table_name_(table_name), db_(db), is_end_(false), cursor_(0)
{
  out_schema_ = MakeIndexDescOutSchema(db_->GetName().size(),
      table_name_.empty() ? MAX_TABNAME_LEN : table_name_.size(),
      128,
      true);  // Show index ID for SHOW INDEXES
}

void ShowIndexesExecutor::Init()
{
  if (table_name_.empty()) {
    // Show all indexes from all tables
    auto &tables = db_->GetAllTables();
    for (auto &table_pair : tables) {
      auto table_id = table_pair.first;
      auto indexes  = db_->GetIndexes(table_id);
      indexes_to_show_.insert(indexes_to_show_.end(), indexes.begin(), indexes.end());
    }
  } else {
    // Show indexes only for the specified table
    auto table = db_->GetTable(table_name_);
    if (table == nullptr) {
      NJUDB_THROW(NJUDB_TABLE_MISS, table_name_);
    }
    auto indexes = db_->GetIndexes(table_name_);
    indexes_to_show_.insert(indexes_to_show_.end(), indexes.begin(), indexes.end());
  }

  if (cursor_ >= indexes_to_show_.size()) {
    is_end_ = true;
    return;
  }

  auto index = indexes_to_show_[cursor_];
  auto table = db_->GetTable(index->GetTableId());
  if (table == nullptr) {
    NJUDB_THROW(NJUDB_TABLE_MISS, "Table not found for index");
  }

  auto values = MakeIndexDescValue(db_->GetName(),
      table->GetTableName(),
      index->GetIndexName(),
      index->GetIndexId(),
      index->GetIndexType(),
      index->GetKeySchema(),
      true);
  record_     = std::make_unique<Record>(out_schema_.get(), values, INVALID_RID);
  cursor_++;
}

void ShowIndexesExecutor::Next()
{
  if (is_end_) {
    NJUDB_FATAL("ShowIndexesExecutor is end");
  }

  if (cursor_ >= indexes_to_show_.size()) {
    is_end_ = true;
    return;
  }

  auto index = indexes_to_show_[cursor_];
  auto table = db_->GetTable(index->GetTableId());
  if (table == nullptr) {
    NJUDB_THROW(NJUDB_TABLE_MISS, "Table not found for index");
  }

  auto values = MakeIndexDescValue(db_->GetName(),
      table->GetTableName(),
      index->GetIndexName(),
      index->GetIndexId(),
      index->GetIndexType(),
      index->GetKeySchema(),
      true);
  record_     = std::make_unique<Record>(out_schema_.get(), values, INVALID_RID);
  cursor_++;
}

auto ShowIndexesExecutor::IsEnd() const -> bool { return is_end_; }

}  // namespace njudb
