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

#ifndef NJUDB_TYPES_H
#define NJUDB_TYPES_H
#include <cstddef>
#include <atomic>
#include "../common/micro.h"

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
constexpr int32_t INVALID_IDX_ID   = -1;
constexpr int32_t INVALID_FRAME_ID = -1;
constexpr int32_t INVALID_TXN_ID   = -1;
constexpr int32_t INVALID_FILE_ID  = -1;


#define ENUM_ENTITIES \
  ENUM(NARY_MODEL)    \
  ENUM(PAX_MODEL)
#define ENUM(ent) ENUMENTRY(ent)
DECLARE_ENUM(StorageModel)
#undef ENUM
#define ENUM(ent) ENUM2STRING(ent)
ENUM_TO_STRING_BODY(StorageModel)
#undef ENUM
#undef ENUM_ENTITIES

#define ENUM_ENTITIES \
  ENUM(TYPE_NULL)     \
  ENUM(TYPE_BOOL)     \
  ENUM(TYPE_INT)      \
  ENUM(TYPE_FLOAT)    \
  ENUM(TYPE_STRING)   \
  ENUM(TYPE_ARRAY)
#define ENUM(ent) ENUMENTRY(ent)
DECLARE_ENUM(FieldType)
#undef ENUM
#define ENUM(ent) ENUM2STRING(ent)
ENUM_TO_STRING_BODY(FieldType)
#undef ENUM
#undef ENUM_ENTITIES

#define ENUM_ENTITIES \
  ENUM(AGG_NONE)      \
  ENUM(AGG_COUNT)     \
  ENUM(AGG_COUNT_STAR) \
  ENUM(AGG_SUM)       \
  ENUM(AGG_AVG)       \
  ENUM(AGG_MAX)       \
  ENUM(AGG_MIN)
#define ENUM(ent) ENUMENTRY(ent)
DECLARE_ENUM(AggType)
#undef ENUM
#undef ENUM_ENTITIES
inline auto AggTypeToString(AggType type) -> const char *
{
  switch (type) {
    case AGG_MIN: return "MIN";
    case AGG_MAX: return "MAX";
    case AGG_SUM: return "SUM";
    case AGG_AVG: return "AVG";
    case AGG_COUNT: return "COUNT";
    case AGG_COUNT_STAR: return "COUNT(*)";
    default: return "UNKNOWN";
  }
}

#define ENUM_ENTITIES \
  ENUM(INNER_JOIN)    \
  ENUM(OUTER_JOIN)
#define ENUM(ent) ENUMENTRY(ent)
DECLARE_ENUM(JoinType)
#undef ENUM
#define ENUM(ent) ENUM2STRING(ent)
ENUM_TO_STRING_BODY(JoinType)
#undef ENUM
#undef ENUM_ENTITIES

#define ENUM_ENTITIES \
  ENUM(NESTED_LOOP)   \
  ENUM(SORT_MERGE)    \
  ENUM(HASH_JOIN)
#define ENUM(ent) ENUMENTRY(ent)
DECLARE_ENUM(JoinStrategy)
#undef ENUM
#define ENUM(ent) ENUM2STRING(ent)
ENUM_TO_STRING_BODY(JoinStrategy)
#undef ENUM
#undef ENUM_ENTITIES

#define ENUM_ENTITIES \
  ENUM(OrderBy_ASC)   \
  ENUM(OrderBy_DESC)
#define ENUM(ent) ENUMENTRY(ent)
DECLARE_ENUM(OrderByDir)
#undef ENUM
#define ENUM(ent) ENUM2STRING(ent)
ENUM_TO_STRING_BODY(OrderByDir)
#undef ENUM
#undef ENUM_ENTITIES

#define ENUM_ENTITIES \
  ENUM(OP_EQ)         \
  ENUM(OP_NE)         \
  ENUM(OP_LT)         \
  ENUM(OP_GT)         \
  ENUM(OP_LE)         \
  ENUM(OP_GE)         \
  ENUM(OP_IN)         \
  ENUM(OP_RNG)
#define ENUM(ent) ENUMENTRY(ent)
DECLARE_ENUM(CompOp)
#undef ENUM
#undef ENUM_ENTITIES
inline auto CompOpToString(CompOp op) -> const char *{
  switch (op) {
    case OP_EQ: return "=";
    case OP_NE: return "<>";
    case OP_LT: return "<";
    case OP_GT: return ">";
    case OP_LE: return "<=";
    case OP_GE: return ">=";
    case OP_IN: return "IN";
    case OP_RNG: return "RANGE";
    default: return "UNKNOWN";
  }
}

#define ENUM_ENTITIES \
  ENUM(NONE)          \
  ENUM(BPTREE)        \
  ENUM(HASH)
#define ENUM(ent) ENUMENTRY(ent)
DECLARE_ENUM(IndexType)
#undef ENUM
#define ENUM(ent) ENUM2STRING(ent)
ENUM_TO_STRING_BODY(IndexType)
#undef ENUM
#undef ENUM_ENTITIES

#endif  // NJUDB_TYPES_H
