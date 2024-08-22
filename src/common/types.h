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

#ifndef WSDB_TYPES_H
#define WSDB_TYPES_H
#include <cstddef>
#include <atomic>

/// basic types definitions
typedef int32_t frame_id_t;
typedef int32_t page_id_t;
typedef int32_t slot_id_t;
typedef int32_t table_id_t;
typedef int32_t idx_id_t;
typedef int32_t file_id_t;
typedef int32_t txn_id_t;
typedef int32_t lsn_t;

typedef size_t timestamp_t;

constexpr int32_t INVALID_PAGE_ID  = -1;
constexpr int32_t INVALID_SLOT_ID  = -1;
constexpr int32_t INVALID_TABLE_ID = -1;
constexpr int32_t INVALID_FRAME_ID = -1;
constexpr int32_t INVALID_TXN_ID   = -1;
constexpr int32_t INVALID_FILE_ID  = -1;

enum StorageModel
{
  NARY_MODEL = 1,
  PAX_MODEL
};
auto StorageModelToString(StorageModel model) -> const char *;

/// enum definitions
enum FieldType
{
  TYPE_NULL = 0,
  TYPE_BOOL,
  TYPE_INT,
  TYPE_FLOAT,
  TYPE_STRING,
  TYPE_ARRAY,
};
auto FieldTypeToString(FieldType type) -> const char *;

enum AggType
{
  AGG_NONE = 0,
  AGG_COUNT_STAR,
  AGG_COUNT,
  AGG_SUM,
  AGG_AVG,
  AGG_MAX,
  AGG_MIN
};
auto AggTypeToString(AggType type) -> const char *;

enum JoinType
{
  INNER_JOIN = 1,
  OUTER_JOIN
};

enum JoinStrategy
{
  NESTED_LOOP = 1,
  SORT_MERGE
};

enum OrderByDir
{
  OrderBy_ASC,
  OrderBy_DESC
};

enum CompOp
{
  OP_EQ = 1,
  OP_NE,
  OP_LT,
  OP_GT,
  OP_LE,
  OP_GE,
  OP_IN,
  OP_RNG
};
auto CompOpToString(CompOp op) -> const char *;

#endif  // WSDB_TYPES_H
