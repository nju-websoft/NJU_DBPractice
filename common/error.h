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

#ifndef NJUDB_ERROR_H
#define NJUDB_ERROR_H

#include <exception>
#include <cassert>
#include <string>
#include <iostream>
#include <utility>
#include "micro.h"

#include "fmt/format.h"

/// usage example: throw NJUDBException(NJUDB_FILE_EXISTS, Q(Diskmanager), Q(CreateFile), file_name)
/// output: Exception <Diskmanager::CreateFile>[NJUDB_FILE_EXISTS]: file_name

namespace njudb {

/**
 * @brief error definitions for NJUDB
 * @a NJUDB_EXCEPTION_EMPTY: unreachable abstract class method, undetermined method, etc.
 * @a NJUDB_FILE_EXISTS: file already exists, used for file creation check
 * @a NJUDB_FILE_NOT_EXISTS: file not exists, used for file deletion check
 * @a NJUDB_FILE_NOT_OPEN: file not open, disk manager needs to open file before read/write
 * @a NJUDB_FILE_DELETE_ERROR: unix error when failing to unlink file
 * @a NJUDB_FILE_REOPEN: file already opened, disk manager should not open file twice
 * @a NJUDB_NOT_IMPLEMENTED: method not implemented, used for abstract class method
 * @a NJUDB_NO_FREE_FRAME: buffer pool manager cannot find available frame to load page
 * @a NJUDB_RECORD_EXISTS: record already exists, used for table manager for record insertion
 * @a NJUDB_RECORD_MISS: record not exists, used for table manager for record deletion
 * @a NJUDB_RECLEN_ERROR: record length error, used to check if the record length exceeds MAX_RECORD_SIZE
 * @a NJUDB_PAGE_MISS: used for table manager to check if RID.page is valid
 * @a NJUDB_FILE_READ_ERROR: unix error when failing to read file
 * @a NJUDB_FILE_WRITE_ERROR: unix error when failing to write file
 * @a NJUDB_INVALID_SQL: invalid SQL statement, syntax error
 * @a NJUDB_TXN_ABORTED: transaction aborted, used for transaction manager
 * @a NJUDB_DB_EXISTS: database already exists when attempting to create a new database
 * @a NJUDB_DB_MISS: database not exists when attempting to open a database
 * @a NJUDB_DB_NOT_OPEN: database not open, client should open a specific database before operations
 * @a NJUDB_TABLE_MISS: table handler not exists
 * @a NJUDB_TABLE_EXIST: table already exists when attempting to create a new table
 * @a NJUDB_GRAMMAR_ERROR: SQL grammar error, check semantic error
 * @a NJUDB_FIELD_MISS: field not exists in the schema
 * @a NJUDB_STRING_OVERFLOW: string overflow, used for string size check
 * @a NJUDB_TYPE_MISSMATCH: type mismatch in comparison
 * @a NJUDB_UNSUPPORTED_OP: unsupported operation
 * @a NJUDB_UNEXPECTED_NULL: unexpected null value after adequate check
 * @a NJUDB_CLIENT_DOWN: client down, should close the client connection
 */
#define ENUM_ENTITIES          \
  ENUM(NJUDB_EXCEPTION_EMPTY)   \
  ENUM(NJUDB_FILE_EXISTS)       \
  ENUM(NJUDB_FILE_NOT_EXISTS)   \
  ENUM(NJUDB_FILE_NOT_OPEN)     \
  ENUM(NJUDB_FILE_DELETE_ERROR) \
  ENUM(NJUDB_FILE_REOPEN)       \
  ENUM(NJUDB_NOT_IMPLEMENTED)   \
  ENUM(NJUDB_NO_FREE_FRAME)     \
  ENUM(NJUDB_RECORD_EXISTS)     \
  ENUM(NJUDB_RECORD_MISS)       \
  ENUM(NJUDB_RECLEN_ERROR)      \
  ENUM(NJUDB_PAGE_MISS)         \
  ENUM(NJUDB_FILE_READ_ERROR)   \
  ENUM(NJUDB_FILE_WRITE_ERROR)  \
  ENUM(NJUDB_INVALID_SQL)       \
  ENUM(NJUDB_TXN_ABORTED)       \
  ENUM(NJUDB_DB_EXISTS)         \
  ENUM(NJUDB_DB_MISS)           \
  ENUM(NJUDB_DB_NOT_OPEN)       \
  ENUM(NJUDB_TABLE_MISS)        \
  ENUM(NJUDB_TABLE_EXIST)       \
  ENUM(NJUDB_INDEX_FAIL)        \
  ENUM(NJUDB_INDEX_EXISTS)      \
  ENUM(NJUDB_INDEX_MISS)        \
  ENUM(NJUDB_GRAMMAR_ERROR)     \
  ENUM(NJUDB_FIELD_MISS)        \
  ENUM(NJUDB_STRING_OVERFLOW)   \
  ENUM(NJUDB_TYPE_MISSMATCH)    \
  ENUM(NJUDB_UNSUPPORTED_OP)    \
  ENUM(NJUDB_UNEXPECTED_NULL)   \
  ENUM(NJUDB_CLIENT_DOWN)
#define ENUM(ent) ENUMENTRY(ent)
DECLARE_ENUM(NJUDBExceptionType)
#undef ENUM
#define ENUM(ent) ENUM2STRING(ent)
ENUM_TO_STRING_BODY(NJUDBExceptionType)
#undef ENUM
#undef ENUM_ENTITIES

/// usage:
/// throw NJUDBException(NJUDB_FILE_EXISTS, Q(Diskmanager), Q(CreateFile), file_name)

class NJUDBException_ : public std::exception
{
public:
  NJUDBException_() = delete;
  explicit NJUDBException_(NJUDBExceptionType type, std::string cname = {}, std::string fname = {}, std::string msg = {})
      : type_(type), cname_(std::move(cname)), fname_(std::move(fname)), msg_(std::move(msg))
  {
    std::string info_str = msg_.empty() ? "" : ": " + msg_;
    out_ = fmt::format("EXCEPTION <{}::{}>[{}]{}", cname_, fname_, NJUDBExceptionTypeToString(type_), info_str);
  }

  NJUDBExceptionType type_;
  std::string       cname_;
  std::string       fname_;
  std::string       msg_;
  std::string       out_;

  [[nodiscard]] const char *what() const noexcept override { return out_.c_str(); }

  // dismiss class and function name
  [[nodiscard]] auto short_what() const -> std::string
  {
    std::string info_str = msg_.empty() ? "" : ": " + msg_;
    return fmt::format("EXCEPTION [{}]{}", NJUDBExceptionTypeToString(type_), info_str);
  }

private:
};

#define NJUDB_THROW(type, msg) throw njudb::NJUDBException_(type, fmt::format("{}({})", __FILE__, __LINE__), __func__, msg)

#define NJUDB_FATAL(msg)                                                                                 \
  do {                                                                                                  \
    std::cerr << fmt::format("Fetal <{}({})::{}>: {}", __FILE__, __LINE__, __func__, msg) << std::endl; \
    exit(1);                                                                                            \
  } while (0)

#define NJUDB_ASSERT(expr, msg)                                                                                        \
  do {                                                                                                                \
    if (!(expr)) {                                                                                                    \
      std::cerr << fmt::format("Assert <{}({})::{}>[{}]: {}", __FILE__, __LINE__, __func__, #expr, msg) << std::endl; \
      std::abort();                                                                                                   \
    }                                                                                                                 \
  } while (0)

#define NJUDB_STUDENT_TODO(lab, q)                                                                          \
  do {                                                                                                     \
    std::cerr << fmt::format("Student TODO [{}.{}]: <{}({})::{}>", #lab, #q, __FILE__, __LINE__, __func__) \
              << std::endl;                                                                                \
    exit(1);                                                                                               \
  } while (0)

}  // namespace njudb

#endif  // NJUDB_ERROR_H
