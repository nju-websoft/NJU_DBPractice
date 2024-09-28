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

#ifndef WSDB_ERROR_H
#define WSDB_ERROR_H

#include <exception>
#include <cassert>
#include <string>
#include <iostream>
#include <utility>
#include "micro.h"

#include "fmt/format.h"

/// usage example: throw WSDBException(WSDB_FILE_EXISTS, Q(Diskmanager), Q(CreateFile), file_name)
/// output: Exception <Diskmanager::CreateFile>[WSDB_FILE_EXISTS]: file_name

namespace wsdb {

/**
 * @brief error definitions for WSDB
 * @a WSDB_EXCEPTION_EMPTY: unreachable abstract class method, undetermined method, etc.
 * @a WSDB_FILE_EXISTS: file already exists, used for file creation check
 * @a WSDB_FILE_NOT_EXISTS: file not exists, used for file deletion check
 * @a WSDB_FILE_NOT_OPEN: file not open, disk manager needs to open file before read/write
 * @a WSDB_FILE_DELETE_ERROR: unix error when failing to unlink file
 * @a WSDB_FILE_REOPEN: file already opened, disk manager should not open file twice
 * @a WSDB_NOT_IMPLEMENTED: method not implemented, used for abstract class method
 * @a WSDB_NO_FREE_FRAME: buffer pool manager cannot find available frame to load page
 * @a WSDB_RECORD_EXISTS: record already exists, used for table manager for record insertion
 * @a WSDB_RECORD_MISS: record not exists, used for table manager for record deletion
 * @a WSDB_RECLEN_ERROR: record length error, used to check if the record length exceeds MAX_RECORD_SIZE
 * @a WSDB_PAGE_MISS: used for table manager to check if RID.page is valid
 * @a WSDB_FILE_READ_ERROR: unix error when failing to read file
 * @a WSDB_FILE_WRITE_ERROR: unix error when failing to write file
 * @a WSDB_INVALID_SQL: invalid SQL statement, syntax error
 * @a WSDB_TXN_ABORTED: transaction aborted, used for transaction manager
 * @a WSDB_DB_EXISTS: database already exists when attempting to create a new database
 * @a WSDB_DB_MISS: database not exists when attempting to open a database
 * @a WSDB_DB_NOT_OPEN: database not open, client should open a specific database before operations
 * @a WSDB_TABLE_MISS: table handler not exists
 * @a WSDB_TABLE_EXIST: table already exists when attempting to create a new table
 * @a WSDB_GRAMMAR_ERROR: SQL grammar error, check semantic error
 * @a WSDB_FIELD_MISS: field not exists in the schema
 * @a WSDB_STRING_OVERFLOW: string overflow, used for string size check
 * @a WSDB_TYPE_MISSMATCH: type mismatch in comparison
 * @a WSDB_UNSUPPORTED_OP: unsupported operation
 * @a WSDB_UNEXPECTED_NULL: unexpected null value after adequate check
 * @a WSDB_CLIENT_DOWN: client down, should close the client connection
 */
#define ENUM_ENTITIES          \
  ENUM(WSDB_EXCEPTION_EMPTY)   \
  ENUM(WSDB_FILE_EXISTS)       \
  ENUM(WSDB_FILE_NOT_EXISTS)   \
  ENUM(WSDB_FILE_NOT_OPEN)     \
  ENUM(WSDB_FILE_DELETE_ERROR) \
  ENUM(WSDB_FILE_REOPEN)       \
  ENUM(WSDB_NOT_IMPLEMENTED)   \
  ENUM(WSDB_NO_FREE_FRAME)     \
  ENUM(WSDB_RECORD_EXISTS)     \
  ENUM(WSDB_RECORD_MISS)       \
  ENUM(WSDB_RECLEN_ERROR)      \
  ENUM(WSDB_PAGE_MISS)         \
  ENUM(WSDB_FILE_READ_ERROR)   \
  ENUM(WSDB_FILE_WRITE_ERROR)  \
  ENUM(WSDB_INVALID_SQL)       \
  ENUM(WSDB_TXN_ABORTED)       \
  ENUM(WSDB_DB_EXISTS)         \
  ENUM(WSDB_DB_MISS)           \
  ENUM(WSDB_DB_NOT_OPEN)       \
  ENUM(WSDB_TABLE_MISS)        \
  ENUM(WSDB_TABLE_EXIST)       \
  ENUM(WSDB_GRAMMAR_ERROR)     \
  ENUM(WSDB_FIELD_MISS)        \
  ENUM(WSDB_STRING_OVERFLOW)   \
  ENUM(WSDB_TYPE_MISSMATCH)    \
  ENUM(WSDB_UNSUPPORTED_OP)    \
  ENUM(WSDB_UNEXPECTED_NULL)   \
  ENUM(WSDB_CLIENT_DOWN)
#define ENUM(ent) ENUMENTRY(ent)
DECLARE_ENUM(WSDBExceptionType)
#undef ENUM
#define ENUM(ent) ENUM2STRING(ent)
ENUM_TO_STRING_BODY(WSDBExceptionType)
#undef ENUM
#undef ENUM_ENTITIES

/// usage:
/// throw WSDBException(WSDB_FILE_EXISTS, Q(Diskmanager), Q(CreateFile), file_name)

class WSDBException_ : public std::exception
{
public:
  WSDBException_() = delete;
  explicit WSDBException_(WSDBExceptionType type, std::string cname = {}, std::string fname = {}, std::string msg = {})
      : type_(type), cname_(std::move(cname)), fname_(std::move(fname)), msg_(std::move(msg))
  {
    std::string info_str = msg_.empty() ? "" : ": " + msg_;
    out_ = fmt::format("EXCEPTION <{}::{}>[{}]{}", cname_, fname_, WSDBExceptionTypeToString(type_), info_str);
  }

  WSDBExceptionType type_;
  std::string       cname_;
  std::string       fname_;
  std::string       msg_;
  std::string       out_;

  [[nodiscard]] const char *what() const noexcept override { return out_.c_str(); }

  // dismiss class and function name
  [[nodiscard]] auto short_what() const -> std::string
  {
    std::string info_str = msg_.empty() ? "" : ": " + msg_;
    return fmt::format("EXCEPTION [{}]{}", WSDBExceptionTypeToString(type_), info_str);
  }

private:
};

#define WSDB_THROW(type, msg) throw wsdb::WSDBException_(type, fmt::format("{}({})", __FILE__, __LINE__), __func__, msg)

#define WSDB_FETAL(msg)                                                                                 \
  do {                                                                                                  \
    std::cerr << fmt::format("Fetal <{}({})::{}>: {}", __FILE__, __LINE__, __func__, msg) << std::endl; \
    exit(1);                                                                                            \
  } while (0)

#define WSDB_ASSERT(expr, msg)                                                                                        \
  do {                                                                                                                \
    if (!(expr)) {                                                                                                    \
      std::cerr << fmt::format("Assert <{}({})::{}>[{}]: {}", __FILE__, __LINE__, __func__, #expr, msg) << std::endl; \
      std::abort();                                                                                                   \
    }                                                                                                                 \
  } while (0)

#define WSDB_STUDENT_TODO(lab, q)                                                                          \
  do {                                                                                                     \
    std::cerr << fmt::format("Student TODO [{}.{}]: <{}({})::{}>", #lab, #q, __FILE__, __LINE__, __func__) \
              << std::endl;                                                                                \
    exit(1);                                                                                               \
  } while (0)

}  // namespace wsdb

#endif  // WSDB_ERROR_H
