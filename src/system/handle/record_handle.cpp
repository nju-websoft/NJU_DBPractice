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
// Created by ziqi on 2024/7/17.
//

#include "record_handle.h"
#include <cstring>
#include <utility>

namespace wsdb {

RecordSchema::RecordSchema(std::vector<RTField> fields) : fields_(std::move(fields))
{
  offsets_.reserve(fields_.size());
  rec_len_ = 0;
  for (auto &field : fields_) {
    offsets_.push_back(rec_len_);
    rec_len_ += field.field_.field_size_;
  }
}

void RecordSchema::SetTableId(table_id_t tid)
{
  for (auto &field : fields_) {
    field.field_.table_id_ = tid;
  }
}

auto RecordSchema::GetFields() const -> const std::vector<RTField> & { return fields_; }

auto RecordSchema::GetFieldAt(size_t index) const -> const RTField &
{
  WSDB_ASSERT(RecordSchema, GetFieldAt, index < fields_.size(), "Index out of range");
  return fields_[index];
}

auto RecordSchema::GetFieldOffset(size_t index) const -> size_t
{
  WSDB_ASSERT(RecordSchema, GetFieldOffset, index < fields_.size(), "Index out of range");
  return offsets_[index];
}

auto RecordSchema::GetFieldIndex(table_id_t tid, const std::string &name) const -> size_t
{
  for (size_t i = 0; i < fields_.size(); ++i) {
    if (fields_[i].field_.table_id_ == tid && fields_[i].field_.field_name_ == name) {
      return i;
    }
  }
  return fields_.size();
}

auto RecordSchema::GetRTFieldIndex(const RTField &rtfield) const -> size_t
{
  for (size_t i = 0; i < fields_.size(); ++i) {
    auto &f = fields_[i];
    // NOTE: we don't check alias
    if (f.is_agg_ == rtfield.is_agg_ && f.agg_type_ == rtfield.agg_type_ && f.field_ == rtfield.field_) {
      return i;
    }
  }
  return fields_.size();
}

auto RecordSchema::GetFieldByName(table_id_t tid, const std::string &name) const -> const RTField &
{
  return fields_[GetFieldIndex(tid, name)];
}

auto RecordSchema::GetFieldOffset(table_id_t tid, const std::string &name) const -> size_t
{
  return offsets_[GetFieldIndex(tid, name)];
}

auto RecordSchema::GetRecordLength() const -> size_t { return rec_len_; }

auto RecordSchema::GetFieldCount() const -> size_t { return fields_.size(); }

auto RecordSchema::HasField(table_id_t tid, const std::string &name) -> bool
{
  return GetFieldIndex(tid, name) != fields_.size();
}

Record::Record(const RecordSchema *schema, const char *null_map_mem, const char *data, RID rid) : schema_(schema)
{
  data_    = new char[schema_->GetRecordLength()];
  nullmap_ = new char[BITMAP_SIZE(schema_->GetFieldCount())];
  std::memcpy(data_, data, schema_->GetRecordLength());
  std::memcpy(nullmap_, null_map_mem, BITMAP_SIZE(schema_->GetFieldCount()));
  rid_ = rid;
}

Record::Record(const RecordSchema *schema, const std::vector<ValueSptr> &values, wsdb::RID rid)
{
  schema_  = schema;
  data_    = new char[schema_->GetRecordLength()];
  nullmap_ = new char[BITMAP_SIZE(schema_->GetFieldCount())];
  bzero(data_, schema_->GetRecordLength());
  bzero(nullmap_, BITMAP_SIZE(schema_->GetFieldCount()));
  size_t cursor = 0;
  for (size_t i = 0; i < schema_->GetFieldCount(); ++i) {
    auto &field = schema_->GetFieldAt(i);
    if (values[i]->IsNull()) {
      BitMap::SetBit(nullmap_, i, true);
    } else {
      switch (field.field_.field_type_) {
        case FieldType::TYPE_BOOL: {
          auto value = std::dynamic_pointer_cast<BoolValue>(values[i]);
          if (value == nullptr)
            throw WSDBException(WSDB_TYPE_MISSMATCH,
                Q(Record),
                Q(Record),
                fmt::format(
                    "{} != {}", FieldTypeToString(field.field_.field_type_), FieldTypeToString(values[i]->GetType())));
          *reinterpret_cast<bool *>(data_ + cursor) = value->Get();
          break;
        }
        case FieldType::TYPE_INT: {
          auto value = std::dynamic_pointer_cast<IntValue>(ValueFactory::CastTo(values[i], FieldType::TYPE_INT));
          // should first try to cast to IntValue
          if (value == nullptr)
            throw WSDBException(WSDB_TYPE_MISSMATCH,
                Q(Record),
                Q(Record),
                fmt::format(
                    "{} != {}", FieldTypeToString(field.field_.field_type_), FieldTypeToString(values[i]->GetType())));

          *reinterpret_cast<int32_t *>(data_ + cursor) = value->Get();
          break;
        }
        case FieldType::TYPE_FLOAT: {
          auto value = std::dynamic_pointer_cast<FloatValue>(ValueFactory::CastTo(values[i], FieldType::TYPE_FLOAT));
          if (value == nullptr)
            throw WSDBException(WSDB_TYPE_MISSMATCH,
                Q(Record),
                Q(Record),
                fmt::format(
                    "{} != {}", FieldTypeToString(field.field_.field_type_), FieldTypeToString(values[i]->GetType())));

          *reinterpret_cast<float *>(data_ + cursor) = value->Get();
          break;
        }
        case FieldType::TYPE_STRING: {
          auto value = std::dynamic_pointer_cast<StringValue>(values[i]);
          if (value == nullptr)
            throw WSDBException(WSDB_TYPE_MISSMATCH,
                Q(Record),
                Q(Record),
                fmt::format(
                    "{} != {}", FieldTypeToString(field.field_.field_type_), FieldTypeToString(values[i]->GetType())));

          if (value->Get().size() > field.field_.field_size_) {
            throw WSDBException(WSDB_STRING_OVERFLOW,
                Q(Record),
                Q(Record),
                fmt::format("field:{}, size:{}, requested:{}",
                    field.field_.field_name_,
                    field.field_.field_size_,
                    value->Get().size()));
          }
          std::memcpy(data_ + cursor, value->Get().c_str(), value->Get().size());
          break;
        }
        default: WSDB_FETAL(Record, Record, "Unsupported field type");
      }
    }
    cursor += field.field_.field_size_;
  }
  rid_ = rid;
}

Record::Record(const RecordSchema *schema, const Record &other) : schema_(schema)
{
  // new can deal with GetRecordLength() == 0
  data_    = new char[schema_->GetRecordLength()];
  nullmap_ = new char[BITMAP_SIZE(schema_->GetFieldCount())];
  bzero(data_, schema_->GetRecordLength());
  bzero(nullmap_, BITMAP_SIZE(schema_->GetFieldCount()));
  for (size_t i = 0; i < schema_->GetFieldCount(); ++i) {
    auto &field     = schema_->GetFieldAt(i);
    auto  other_idx = other.schema_->GetRTFieldIndex(field);
    if (other_idx == other.schema_->GetFieldCount()) {
      WSDB_FETAL(Record, Record, "Field not found in other record");
    }
    auto other_offset = other.schema_->offsets_[other_idx];
    std::memcpy(data_ + schema_->offsets_[i], other.data_ + other_offset, field.field_.field_size_);
    if (BitMap::GetBit(other.nullmap_, other_idx)) {
      BitMap::SetBit(nullmap_, i, true);
    }
  }
  rid_ = INVALID_RID;
}

Record::Record(const RecordSchema *schema, const wsdb::Record &rec1, const wsdb::Record &rec2)
{
  // do some simple asserts
  WSDB_ASSERT(Record,
      Record,
      schema->GetFieldCount() == rec1.schema_->GetFieldCount() + rec2.schema_->GetFieldCount(),
      "Field count mismatch");
  WSDB_ASSERT(Record,
      Record,
      schema->GetRecordLength() == rec1.schema_->GetRecordLength() + rec2.schema_->GetRecordLength(),
      "Record length mismatch");
  schema_  = schema;
  data_    = new char[schema_->GetRecordLength()];
  nullmap_ = new char[BITMAP_SIZE(schema_->GetFieldCount())];
  bzero(nullmap_, BITMAP_SIZE(schema_->GetFieldCount()));
  memcpy(data_, rec1.data_, rec1.schema_->GetRecordLength());
  memcpy(data_ + rec1.schema_->GetRecordLength(), rec2.data_, rec2.schema_->GetRecordLength());
  // null map should not simply be copied, but should be re-calculated
  for (size_t i = 0; i < rec1.schema_->GetFieldCount(); ++i) {
    if (BitMap::GetBit(rec1.nullmap_, i)) {
      BitMap::SetBit(nullmap_, i, true);
    }
  }
  for (size_t i = 0; i < rec2.schema_->GetFieldCount(); ++i) {
    if (BitMap::GetBit(rec2.nullmap_, i)) {
      BitMap::SetBit(nullmap_, i + rec1.schema_->GetFieldCount(), true);
    }
  }
  rid_ = INVALID_RID;
}

Record::Record(const wsdb::RecordSchema *schema)
{
  schema_  = schema;
  data_    = new char[schema_->GetRecordLength()];
  nullmap_ = new char[BITMAP_SIZE(schema_->GetFieldCount())];
  // set nullmap to all 1
  bzero(data_, schema_->GetRecordLength());
  memset(nullmap_, 0xff, BITMAP_SIZE(schema_->GetFieldCount()));
  rid_ = INVALID_RID;
}

Record::~Record()
{
  delete[] data_;
  delete[] nullmap_;
}

Record::Record(const Record &record)
    : schema_(record.schema_),
      data_(new char[schema_->GetRecordLength()]),
      nullmap_(new char[BITMAP_SIZE(schema_->GetFieldCount())]),
      rid_(record.rid_)
{
  std::memcpy(data_, record.data_, schema_->GetRecordLength());
  std::memcpy(nullmap_, record.nullmap_, BITMAP_SIZE(schema_->GetFieldCount()));
}

Record &Record::operator=(const Record &record)
{
  if (this == &record) {
    return *this;
  }
  schema_  = record.schema_;
  data_    = new char[schema_->GetRecordLength()];
  nullmap_ = new char[BITMAP_SIZE(schema_->GetFieldCount())];
  std::memcpy(data_, record.data_, schema_->GetRecordLength());
  std::memcpy(nullmap_, record.nullmap_, BITMAP_SIZE(schema_->GetFieldCount()));
  rid_ = record.rid_;
  return *this;
}

Record::Record(Record &&record) noexcept
    : schema_(record.schema_), data_(record.data_), nullmap_(record.nullmap_), rid_(record.rid_)
{
  record.data_    = nullptr;
  record.schema_  = nullptr;
  record.nullmap_ = nullptr;
}

Record &Record::operator=(Record &&record) noexcept
{
  if (this == &record) {
    return *this;
  }
  delete[] data_;
  delete[] nullmap_;
  schema_         = record.schema_;
  data_           = record.data_;
  nullmap_        = record.nullmap_;
  rid_            = record.rid_;
  record.data_    = nullptr;
  record.schema_  = nullptr;
  record.nullmap_ = nullptr;
  return *this;
}

auto Record::operator==(const Record &other) const -> bool
{
  // check if the two record is defined under the same schema and whether their data are matched，
  // compare schema memory address and data
  return schema_ == other.schema_ && std::memcmp(data_, other.data_, schema_->GetRecordLength()) == 0 &&
         std::memcmp(nullmap_, other.nullmap_, BITMAP_SIZE(schema_->GetFieldCount())) == 0;
}

auto Record::Hash() const -> size_t
{
  // use schema and data_ to generate hash
  size_t hash = 0;
  for (size_t i = 0; i < schema_->GetFieldCount(); ++i) {
    if (BitMap::GetBit(nullmap_, i)) {
      continue;
    }
    auto &field = schema_->GetFieldAt(i);
    switch (field.field_.field_type_) {
      case FieldType::TYPE_BOOL:
        hash ^= std::hash<bool>{}(*reinterpret_cast<const bool *>(data_ + schema_->offsets_[i]));
        break;
      case FieldType::TYPE_INT:
        hash ^= std::hash<int32_t>{}(*reinterpret_cast<const int32_t *>(data_ + schema_->offsets_[i]));
        break;
      case FieldType::TYPE_FLOAT:
        hash ^= std::hash<float>{}(*reinterpret_cast<const float *>(data_ + schema_->offsets_[i]));
        break;
      case FieldType::TYPE_STRING:
        hash ^= std::hash<std::string>{}(std::string(data_ + schema_->offsets_[i], field.field_.field_size_));
        break;
      default: WSDB_FETAL(Record, Hash, "Unsupported field type to hash");
    }
  }
  return hash;
}

auto Record::GetValueAt(size_t index) const -> ValueSptr
{
  WSDB_ASSERT(Record, GetValueAt, index < schema_->GetFieldCount(), "Index out of range");
  auto &field = schema_->GetFieldAt(index);
  if (BitMap::GetBit(nullmap_, index)) {
    return ValueFactory::CreateNullValue(field.field_.field_type_);
  }
  return ValueFactory::CreateValue(
      field.field_.field_type_, data_ + schema_->offsets_[index], field.field_.field_size_);
}

auto Record::Compare(const wsdb::Record &lrec, const wsdb::Record &rrec) -> int
{
  // compare two records,
  //  WSDB_ASSERT(Record, Compare, lrec.GetSchema() == rrec.GetSchema(), "Schema mismatch");
  // more loose assert to support two similar records
  WSDB_ASSERT(Record,
      Compare(),
      lrec.GetSchema()->GetFieldCount() == rrec.GetSchema()->GetFieldCount(),
      "field count mismatch");
  for (size_t i = 0; i < lrec.GetSchema()->GetFieldCount(); ++i) {
    auto lval = lrec.GetValueAt(i);
    auto rval = rrec.GetValueAt(i);
    if (lval->IsNull() && rval->IsNull()) {
      continue;
    }
    if (lval->IsNull() || rval->IsNull()) {
      return lval->IsNull() ? -1 : 1;
    }
    if (*lval < *rval) {
      return -1;
    }
    if (*lval > *rval) {
      return 1;
    }
  }
  return 0;
}

Chunk::Chunk(const RecordSchema *schema, std::vector<ArrayValueSptr> cols) : schema_(schema), cols_(std::move(cols)) {}

Chunk::~Chunk() = default;

Chunk::Chunk(const wsdb::Chunk &chunk) = default;

Chunk::Chunk(wsdb::Chunk &&chunk) noexcept = default;

Chunk &Chunk::operator=(const wsdb::Chunk &chunk) = default;

Chunk &Chunk::operator=(wsdb::Chunk &&chunk) noexcept = default;

auto Chunk::GetCol(int index) -> ArrayValueSptr { return cols_[index]; }
}  // namespace wsdb