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
// Created by ziqi on 2024/7/18.
//

#ifndef NJUDB_META_H
#define NJUDB_META_H
#include <string>
#include <vector>
#include <memory>
#include "../../common/micro.h"
#include "types.h"

struct FieldSchema;

// filed schema is the meta information of a field in a table
struct FieldSchema
{
  table_id_t  table_id_{INVALID_TABLE_ID};
  std::string field_name_{};
  size_t      field_size_{};
  FieldType   field_type_{};
  // maybe use in the future
  //  bool        is_primary_key_{};
  //  bool        is_unique_{};
  bool nullable_{true};
  auto operator==(const FieldSchema &rhs) const -> bool
  {
    return table_id_ == rhs.table_id_ && field_name_ == rhs.field_name_ && field_size_ == rhs.field_size_ &&
           field_type_ == rhs.field_type_ && nullable_ == rhs.nullable_;
  }

  auto ToString() const -> std::string
  {
    return fmt::format("#{}.{}:{}({}){}",
        table_id_ == INVALID_TABLE_ID ? "" : std::to_string(table_id_),
        field_name_,
        FieldTypeToString(field_type_),
        field_size_,
        nullable_ ? "" : "<NOT NULL>");
  }
};

// run time field description, used to record the field information in the query and executions
struct RTField
{
  FieldSchema field_{};
  // run time information
  std::string alias_{};
  bool        is_agg_{false};
  AggType     agg_type_{AggType::AGG_NONE};

  auto operator==(const RTField &rhs) const -> bool
  {
    return field_ == rhs.field_ && alias_ == rhs.alias_ && is_agg_ == rhs.is_agg_ && agg_type_ == rhs.agg_type_;
  }

  auto ToString() const -> std::string
  {
    return fmt::format("{}{}{}",
        field_.ToString(),
        alias_.empty() ? "" : fmt::format("\"{}\"", alias_),
        is_agg_ ? fmt::format("[{}]", AggTypeToString(agg_type_)) : "");
  }
};

/**
 * Table header is the first page of a table, it contains the meta information of the table
 */
struct TableHeader
{
  size_t    page_num_{0};
  page_id_t first_free_page_{INVALID_PAGE_ID};
  size_t    rec_num_{0};
  size_t    rec_size_{0};
  size_t    rec_per_page_{0};
  size_t    field_num_{0};
  size_t    bitmap_size_{0};   // bit map size == BITMAP_SIZE(n_rec_per_page)
  size_t    nullmap_size_{0};  // null map size == BITMAP_SIZE(n_field)
};

#endif  // NJUDB_META_H
