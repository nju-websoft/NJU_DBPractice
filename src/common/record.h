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

#ifndef NJUDB_RECORD_MANAGER_H
#define NJUDB_RECORD_MANAGER_H

#include "../../common/micro.h"
#include "meta.h"
#include "rid.h"
#include "value.h"
#include "bitmap.h"

namespace njudb {

class Record;
class Chunk;
class RecordSchema;
DEFINE_UNIQUE_PTR(Record);
DEFINE_UNIQUE_PTR(RecordSchema);
DEFINE_SHARED_PTR(RecordSchema);
DEFINE_UNIQUE_PTR(Chunk);

class RecordSchema
{
  friend Record;

public:
  RecordSchema() = default;

  explicit RecordSchema(std::vector<RTField> fields) : fields_(std::move(fields))
  {
    offsets_.reserve(fields_.size());
    rec_len_ = 0;
    for (auto &field : fields_) {
      offsets_.push_back(rec_len_);
      rec_len_ += field.field_.field_size_;
    }
  }

  ~RecordSchema() = default;

  // important: should not copy or move
  DISABLE_COPY_MOVE_AND_ASSIGN(RecordSchema)

  auto GetFields() const -> const std::vector<RTField> & { return fields_; }

  void SetTableId(table_id_t tid)
  {
    for (auto &field : fields_) {
      field.field_.table_id_ = tid;
    }
  }

  [[nodiscard]] auto GetFieldAt(size_t index) const -> const RTField &
  {
    NJUDB_ASSERT(index < fields_.size(), "Index out of range");
    return fields_[index];
  }

  [[nodiscard]] auto GetFieldOffset(size_t index) const -> size_t
  {
    NJUDB_ASSERT(index < fields_.size(), "Index out of range");
    return offsets_[index];
  }

  [[nodiscard]] auto GetFieldByName(table_id_t tid, const std::string &name) const -> const RTField &
  {
    return fields_[GetFieldIndex(tid, name)];
  }

  [[nodiscard]] auto GetFieldIndex(table_id_t tid, const std::string &name) const -> size_t
  {
    for (size_t i = 0; i < fields_.size(); ++i) {
      if (fields_[i].field_.table_id_ == tid && fields_[i].field_.field_name_ == name) {
        return i;
      }
    }
    return fields_.size();
  }

  /**
   * get field index according to runtime information
   * @return
   */
  [[nodiscard]] auto GetRTFieldIndex(const RTField &rtfield) const -> size_t
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

  [[nodiscard]] auto GetFieldOffset(table_id_t tid, const std::string &name) const -> size_t
  {
    return offsets_[GetFieldIndex(tid, name)];
  }

  [[nodiscard]] auto GetRecordLength() const -> size_t { return rec_len_; }

  [[nodiscard]] auto GetFieldCount() const -> size_t { return fields_.size(); }

  auto HasField(table_id_t tid, const std::string &name) -> bool
  {
    return GetFieldIndex(tid, name) != fields_.size();
  }

  auto Serialize(char *target) const -> size_t
  {
    // field.field_name_, field.field_type_, field.field_size_
    //  first write the number of fields
    size_t offset      = 0;
    size_t field_count = fields_.size();
    std::memcpy(target + offset, &field_count, sizeof(size_t));
    offset += sizeof(size_t);
    // then write each field
    for (const auto &field : fields_) {
      // file_name is stored by terminating with '\0'
      size_t name_length = field.field_.field_name_.size() + 1;  //
      std::memcpy(target + offset, field.field_.field_name_.c_str(), name_length);
      offset += name_length;
      // write field type
      std::memcpy(target + offset, &field.field_.field_type_, sizeof(FieldType));
      offset += sizeof(FieldType);
      // write field size
      std::memcpy(target + offset, &field.field_.field_size_, sizeof(size_t));
      offset += sizeof(size_t);
    }
    return offset;
  }

  auto SerializeSize() const -> size_t
  {
    size_t size = sizeof(size_t);  // for field count
    for (const auto &field : fields_) {
      size += field.field_.field_name_.size() + 1;  // +1 for '\0'
      size += sizeof(FieldType);
      size += sizeof(size_t);
    }
    return size;
  }

  auto Deserialize(const char *source) -> size_t
  {
    size_t offset = 0;
    size_t field_count;
    std::memcpy(&field_count, source + offset, sizeof(size_t));
    offset += sizeof(size_t);
    fields_.clear();
    fields_.reserve(field_count);
    offsets_.clear();
    offsets_.reserve(field_count);
    rec_len_ = 0;
    // read each field
    for (size_t i = 0; i < field_count; ++i) {
      FieldSchema field;
      // read field name
      field.field_name_ = source + offset;
      offset += field.field_name_.size() + 1;  // +1 for '\0'
      // read field type
      std::memcpy(&field.field_type_, source + offset, sizeof(FieldType));
      offset += sizeof(FieldType);
      // read field size
      std::memcpy(&field.field_size_, source + offset, sizeof(size_t));
      offset += sizeof(size_t);
      fields_.emplace_back(RTField{.field_ = field});
      offsets_.push_back(rec_len_);
      rec_len_ += field.field_size_;
    }
    return offset;
  }

  [[nodiscard]] auto ToString() const -> std::string
  {
    std::string str;
    for (const auto &rtfield : fields_) {
      str += rtfield.ToString() + ", ";
    }
    if (!str.empty()) {
      str.pop_back();
      str.pop_back();
    }
    return str;
  }

private:
  size_t               rec_len_;
  std::vector<RTField> fields_;
  std::vector<size_t>  offsets_;
};

/**
 * To prevent unexpected changes to a record, Record class is non-volatile (except rid),
 * if a record-like object is volatile, use RecordSchema + std::vector<ValueSptr> instead
 */
class Record
{
public:
  Record() = delete;

  /**
   * Generate a record from raw data
   * @param schema
   * @param null_map_mem
   * @param data
   * @param rid
   */
  Record(const RecordSchema *schema, const char *null_map_mem, const char *data, RID rid) : schema_(schema)
  {
    data_    = new char[schema_->GetRecordLength()];
    nullmap_ = new char[BITMAP_SIZE(schema_->GetFieldCount())];
    std::memcpy(data_, data, schema_->GetRecordLength());
    if (null_map_mem == nullptr) {
      memset(nullmap_, 0, BITMAP_SIZE(schema_->GetFieldCount()));
    } else {
      std::memcpy(nullmap_, null_map_mem, BITMAP_SIZE(schema_->GetFieldCount()));
    }
    rid_ = rid;
  }

  /**
   * Generate a record from a list of values
   * @param schema
   * @param values
   * @param rid
   */
  Record(const RecordSchema *schema, const std::vector<ValueSptr> &values, RID rid)
  {
    schema_  = schema;
    data_    = new char[schema_->GetRecordLength()];
    nullmap_ = new char[BITMAP_SIZE(schema_->GetFieldCount())];
    memset(data_, 0, schema_->GetRecordLength());
    memset(nullmap_, 0, BITMAP_SIZE(schema_->GetFieldCount()));
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
              NJUDB_THROW(NJUDB_TYPE_MISSMATCH,
                  fmt::format(
                      "{} != {}", FieldTypeToString(field.field_.field_type_), FieldTypeToString(values[i]->GetType())));
            *reinterpret_cast<bool *>(data_ + cursor) = value->Get();
            break;
          }
          case FieldType::TYPE_INT: {
            auto value = std::dynamic_pointer_cast<IntValue>(ValueFactory::CastTo(values[i], FieldType::TYPE_INT));
            // should first try to cast to IntValue
            if (value == nullptr)
              NJUDB_THROW(NJUDB_TYPE_MISSMATCH,
                  fmt::format(
                      "{} != {}", FieldTypeToString(field.field_.field_type_), FieldTypeToString(values[i]->GetType())));

            *reinterpret_cast<int32_t *>(data_ + cursor) = value->Get();
            break;
          }
          case FieldType::TYPE_FLOAT: {
            auto value = std::dynamic_pointer_cast<FloatValue>(ValueFactory::CastTo(values[i], FieldType::TYPE_FLOAT));
            if (value == nullptr)
              NJUDB_THROW(NJUDB_TYPE_MISSMATCH,
                  fmt::format(
                      "{} != {}", FieldTypeToString(field.field_.field_type_), FieldTypeToString(values[i]->GetType())));

            *reinterpret_cast<float *>(data_ + cursor) = value->Get();
            break;
          }
          case FieldType::TYPE_STRING: {
            auto value = std::dynamic_pointer_cast<StringValue>(values[i]);
            if (value == nullptr)
              NJUDB_THROW(NJUDB_TYPE_MISSMATCH,
                  fmt::format(
                      "{} != {}", FieldTypeToString(field.field_.field_type_), FieldTypeToString(values[i]->GetType())));

            if (value->Get().size() > field.field_.field_size_) {
              NJUDB_THROW(NJUDB_STRING_OVERFLOW,
                  fmt::format("field:{}, size:{}, requested:{}",
                      field.field_.field_name_,
                      field.field_.field_size_,
                      value->Get().size()));
            }
            std::memcpy(data_ + cursor, value->Get().c_str(), value->Get().size());
            break;
          }
          default: NJUDB_FATAL("Unsupported field type");
        }
      }
      cursor += field.field_.field_size_;
    }
    rid_ = rid;
  }

  /**
   * Generate a record from another record given the requested schema
   * @param schema should be a subset of the original schema
   * @param other the original record
   */
  Record(const RecordSchema *schema, const Record &other) : schema_(schema)
  {
    // new can deal with GetRecordLength() == 0
    data_    = new char[schema_->GetRecordLength()];
    nullmap_ = new char[BITMAP_SIZE(schema_->GetFieldCount())];
    memset(data_, 0, schema_->GetRecordLength());
    memset(nullmap_, 0, BITMAP_SIZE(schema_->GetFieldCount()));
    for (size_t i = 0; i < schema_->GetFieldCount(); ++i) {
      auto &field     = schema_->GetFieldAt(i);
      auto  other_idx = other.schema_->GetRTFieldIndex(field);
      if (other_idx == other.schema_->GetFieldCount()) {
        NJUDB_FATAL("Field not found in other record");
      }
      auto other_offset = other.schema_->offsets_[other_idx];
      std::memcpy(data_ + schema_->offsets_[i], other.data_ + other_offset, field.field_.field_size_);
      if (BitMap::GetBit(other.nullmap_, other_idx)) {
        BitMap::SetBit(nullmap_, i, true);
      }
    }
    rid_ = INVALID_RID;
  }

  /**
   * Generate a record from two records given the requested schema
   * @param schema should be a combination of the two records' schema
   * @param rec1 the first record
   * @param rec2 the second record
   */
  Record(const RecordSchema *schema, const Record &rec1, const Record &rec2)
  {
    // do some simple asserts
    NJUDB_ASSERT(
        schema->GetFieldCount() == rec1.schema_->GetFieldCount() + rec2.schema_->GetFieldCount(), "Field count mismatch");
    NJUDB_ASSERT(schema->GetRecordLength() == rec1.schema_->GetRecordLength() + rec2.schema_->GetRecordLength(),
        "Record length mismatch");
    schema_  = schema;
    data_    = new char[schema_->GetRecordLength()];
    nullmap_ = new char[BITMAP_SIZE(schema_->GetFieldCount())];
    memset(data_, 0, schema_->GetRecordLength());
    memset(nullmap_, 0, BITMAP_SIZE(schema_->GetFieldCount()));
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

  /**
   * Generate a record with all fields set to null
   * @param schema
   */
  explicit Record(const RecordSchema *schema)
  {
    schema_  = schema;
    data_    = new char[schema_->GetRecordLength()];
    nullmap_ = new char[BITMAP_SIZE(schema_->GetFieldCount())];
    // set nullmap to all 1
    memset(data_, 0, schema_->GetRecordLength());
    memset(nullmap_, 0xff, BITMAP_SIZE(schema_->GetFieldCount()));
    rid_ = INVALID_RID;
  }

  ~Record()
  {
    delete[] data_;
    delete[] nullmap_;
  }

  Record(const Record &record)
      : schema_(record.schema_),
        data_(new char[schema_->GetRecordLength()]),
        nullmap_(new char[BITMAP_SIZE(schema_->GetFieldCount())]),
        rid_(record.rid_)
  {
    std::memcpy(data_, record.data_, schema_->GetRecordLength());
    std::memcpy(nullmap_, record.nullmap_, BITMAP_SIZE(schema_->GetFieldCount()));
  }

  Record &operator=(const Record &record)
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

  Record(Record &&record) noexcept
      : schema_(record.schema_), data_(record.data_), nullmap_(record.nullmap_), rid_(record.rid_)
  {
    record.data_    = nullptr;
    record.schema_  = nullptr;
    record.nullmap_ = nullptr;
  }

  Record &operator=(Record &&record) noexcept
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

  auto operator==(const Record &other) const -> bool
  {
    // check if the two record is defined under the same schema and whether their data are matched，
    // compare schema memory address and data
    return schema_ == other.schema_ && std::memcmp(data_, other.data_, schema_->GetRecordLength()) == 0 &&
           std::memcmp(nullmap_, other.nullmap_, BITMAP_SIZE(schema_->GetFieldCount())) == 0;
  }

  [[nodiscard]] auto Hash() const -> size_t
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
        default: NJUDB_FATAL("Unsupported field type to hash");
      }
    }
    return hash;
  }

  void SetRID(const RID &rid) { rid_ = rid; }

  /// Get the RID of this record
  [[nodiscard]] auto GetRID() const -> RID { return rid_; }

  [[nodiscard]] auto GetValueAt(size_t index) const -> ValueSptr
  {
    NJUDB_ASSERT(index < schema_->GetFieldCount(), "Index out of range");
    auto &field = schema_->GetFieldAt(index);
    if (BitMap::GetBit(nullmap_, index)) {
      return ValueFactory::CreateNullValue(field.field_.field_type_);
    }
    return ValueFactory::CreateValue(
        field.field_.field_type_, data_ + schema_->offsets_[index], field.field_.field_size_);
  }

  [[nodiscard]] auto GetValues() const -> std::vector<ValueSptr>
  {
    std::vector<ValueSptr> values;
    for (size_t i = 0; i < schema_->GetFieldCount(); ++i) {
      values.push_back(GetValueAt(i));
    }
    return values;
  }

  /// Get the schema of this record
  [[nodiscard]] auto GetSchema() const -> const RecordSchema * { return schema_; }

  [[nodiscard]] auto GetData() const -> const char * { return data_; }

  [[nodiscard]] auto GetNullMap() const -> const char * { return nullmap_; }

  static auto Compare(const Record &lrec, const Record &rrec) -> int
  {
    // compare two records,
    //  NJUDB_ASSERT(Record, Compare, lrec.GetSchema() == rrec.GetSchema(), "Schema mismatch");
    // more loose assert to support two similar records
    NJUDB_ASSERT(lrec.GetSchema()->GetFieldCount() == rrec.GetSchema()->GetFieldCount(), "field count mismatch");
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
  
  [[nodiscard]] auto ToString() const -> std::string
  {
    std::string str = "{";
    for (size_t i = 0; i < schema_->GetFieldCount(); ++i) {
      auto val = GetValueAt(i);
      str += val->ToString();
      if (i != schema_->GetFieldCount() - 1) {
        str += ", ";
      }
    }
    str += "}";
    return str;
  }

private:
  const RecordSchema *schema_;
  char               *data_;
  char               *nullmap_;
  RID                 rid_{};
};

class Chunk
{
public:
  Chunk() = delete;

  Chunk(const RecordSchema *schema, std::vector<ArrayValueSptr> cols) : schema_(schema), cols_(std::move(cols))
  {
    NJUDB_ASSERT(schema_->GetFieldCount() == cols_.size(), "Field count mismatch");
  }

  ~Chunk() = default;

  Chunk(const Chunk &chunk) = default;

  Chunk &operator=(const Chunk &chunk) = default;

  Chunk(Chunk &&chunk) noexcept = default;

  Chunk &operator=(Chunk &&chunk) noexcept = default;

  auto GetCol(int index) -> ArrayValueSptr { return cols_[index]; }

  auto GetColCount() -> size_t { return cols_.size(); }

private:
  const RecordSchema         *schema_;
  std::vector<ArrayValueSptr> cols_;
};

}  // namespace njudb

namespace std {
template <>
struct hash<njudb::Record>
{
  auto operator()(const njudb::Record &record) const -> size_t { return record.Hash(); }
};
}  // namespace std



#endif  // NJUDB_RECORD_MANAGER_H
