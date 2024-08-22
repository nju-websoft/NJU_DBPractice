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
#include "types.h"

auto StorageModelToString(StorageModel model) -> const char *
{
  switch (model) {
    case NARY_MODEL: return "NARY";
    case PAX_MODEL: return "PAX";
    default: return "UNKNOWN";
  }
}

auto CompOpToString(CompOp op) -> const char *
{
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

auto FieldTypeToString(FieldType type) -> const char *
{
  switch (type) {
    case TYPE_NULL: return "NULL";
    case TYPE_BOOL: return "BOOL";
    case TYPE_INT: return "INT";
    case TYPE_FLOAT: return "FLOAT";
    case TYPE_STRING: return "STRING";
    case TYPE_ARRAY: return "ARRAY";
    default: return "UNKNOWN";
  }
}

auto AggTypeToString(AggType type) -> const char *
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