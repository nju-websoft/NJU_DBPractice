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

#ifndef WSDB_RECORD_MANAGER_H
#define WSDB_RECORD_MANAGER_H

#include "../../../common/micro.h"
#include "common/meta.h"
#include "common/rid.h"
#include "common/value.h"
#include "common/bitmap.h"

namespace wsdb {

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
  RecordSchema() = delete;

  explicit RecordSchema(std::vector<RTField> fields);

  ~RecordSchema() = default;

  // important: should not copy or move
  DISABLE_COPY_MOVE_AND_ASSIGN(RecordSchema)

  auto GetFields() const -> const std::vector<RTField> &;

  void SetTableId(table_id_t tid);

  [[nodiscard]] auto GetFieldAt(size_t index) const -> const RTField &;

  [[nodiscard]] auto GetFieldOffset(size_t index) const -> size_t;

  [[nodiscard]] auto GetFieldByName(table_id_t tid, const std::string &name) const -> const RTField &;

  [[nodiscard]] auto GetFieldIndex(table_id_t tid, const std::string &name) const -> size_t;

  /**
   * get field index according to runtime information
   * @return
   */
  [[nodiscard]] auto GetRTFieldIndex(const RTField &rtfield) const -> size_t;

  [[nodiscard]] auto GetFieldOffset(table_id_t tid, const std::string &name) const -> size_t;

  [[nodiscard]] auto GetRecordLength() const -> size_t;

  [[nodiscard]] auto GetFieldCount() const -> size_t;

  auto HasField(table_id_t tid, const std::string &name) -> bool;

  [[nodiscard]] auto ToString() const -> std::string
  {
    std::string str;
    for (const auto &rtfield : fields_) {
      auto &field = rtfield.field_;
      str += fmt::format("{}:{}({}), ", field.field_name_, static_cast<int>(field.field_type_), field.field_size_);
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
  Record(const RecordSchema *schema, const char *null_map_mem, const char *data, RID rid);

  /**
   * Generate a record from a list of values
   * @param schema
   * @param values
   * @param rid
   */
  Record(const RecordSchema *schema, const std::vector<ValueSptr> &values, RID rid);

  /**
   * Generate a record from another record given the requested schema
   * @param schema should be a subset of the original schema
   * @param other the original record
   */
  Record(const RecordSchema *schema, const Record &other);

  /**
   * Generate a record from two records given the requested schema
   * @param schema should be a combination of the two records' schema
   * @param rec1 the first record
   * @param rec2 the second record
   */
  Record(const RecordSchema *schema, const Record &rec1, const Record &rec2);

  /**
   * Generate a record with all fields set to null
   * @param schema
   */
  explicit Record(const RecordSchema *schema);

  ~Record();

  Record(const Record &record);

  Record &operator=(const Record &record);

  Record(Record &&record) noexcept;

  Record &operator=(Record &&record) noexcept;

  auto operator==(const Record &other) const -> bool;

  [[nodiscard]] auto Hash() const -> size_t;

  void SetRID(const RID &rid) { rid_ = rid; }

  /// Get the RID of this record
  [[nodiscard]] auto GetRID() const -> RID { return rid_; }

  [[nodiscard]] auto GetValueAt(size_t index) const -> ValueSptr;

  /// Get the schema of this record
  [[nodiscard]] auto GetSchema() const -> const RecordSchema * { return schema_; }

  [[nodiscard]] auto GetData() const -> const char * { return data_; }

  [[nodiscard]] auto GetNullMap() const -> const char * { return nullmap_; }

  static auto Compare(const Record &lrec, const Record &rrec) -> int;

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

  Chunk(const RecordSchema *schema, std::vector<ArrayValueSptr> cols);

  ~Chunk();

  Chunk(const Chunk &chunk);

  Chunk &operator=(const Chunk &chunk);

  Chunk(Chunk &&chunk) noexcept;

  Chunk &operator=(Chunk &&chunk) noexcept;

  auto GetCol(int index) -> ArrayValueSptr;

  auto GetColCount() -> size_t;

private:
  const RecordSchema *schema_;
  std::vector<ArrayValueSptr>          cols_;
};

}  // namespace wsdb

namespace std {
template <>
struct hash<wsdb::Record>
{
  auto operator()(const wsdb::Record &record) const -> size_t { return record.Hash(); }
};
}  // namespace std

#endif  // WSDB_RECORD_MANAGER_H
