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

#include "fmt/format.h"

/// usage example: throw WSDBException(WSDB_FILE_EXISTS, Q(Diskmanager), Q(CreateFile), file_name)
/// output: Exception <Diskmanager::CreateFile>[WSDB_FILE_EXISTS]: file_name

namespace wsdb {

#define WSDB_ERRORS                  \
  WSDB_ERROR(WSDB_EXCEPTION_EMPTY)   \
  WSDB_ERROR(WSDB_FILE_EXISTS)       \
  WSDB_ERROR(WSDB_FILE_NOT_EXISTS)   \
  WSDB_ERROR(WSDB_FILE_NOT_OPEN)     \
  WSDB_ERROR(WSDB_FILE_DELETE_ERROR) \
  WSDB_ERROR(WSDB_FILE_REOPEN)       \
  WSDB_ERROR(WSDB_NOT_IMPLEMENTED)   \
  WSDB_ERROR(WSDB_INVALID_ARGUMENT)  \
  WSDB_ERROR(WSDB_NO_FREE_FRAME)     \
  WSDB_ERROR(WSDB_RECORD_EXISTS)     \
  WSDB_ERROR(WSDB_RECORD_MISS)       \
  WSDB_ERROR(WSDB_RECLEN_ERROR)      \
  WSDB_ERROR(WSDB_PAGE_MISS)         \
  WSDB_ERROR(WSDB_PAGE_READ_ERROR)   \
  WSDB_ERROR(WSDB_PAGE_WRITE_ERROR)  \
  WSDB_ERROR(WSDB_INVALID_SQL)       \
  WSDB_ERROR(WSDB_TXN_ABORTED)       \
  WSDB_ERROR(WSDB_DB_BUSY)           \
  WSDB_ERROR(WSDB_DB_EXISTS)         \
  WSDB_ERROR(WSDB_DB_MISS)           \
  WSDB_ERROR(WSDB_DB_NOT_OPEN)       \
  WSDB_ERROR(WSDB_TABLE_MISS)        \
  WSDB_ERROR(WSDB_TABLE_EXIST)       \
  WSDB_ERROR(WSDB_GRAMMAR_ERROR)     \
  WSDB_ERROR(WSDB_EMPTY_FIELD_DEF)   \
  WSDB_ERROR(WSDB_FIELD_MISS)        \
  WSDB_ERROR(WSDB_AMBIGUOUS_FIELD)   \
  WSDB_ERROR(WSDB_STRING_OVERFLOW)   \
  WSDB_ERROR(WSDB_TYPE_MISSMATCH)    \
  WSDB_ERROR(WSDB_UNSUPPORTED_OP)    \
  WSDB_ERROR(WSDB_UNEXPECTED_NULL)   \
  WSDB_ERROR(WSDB_CLIENT_DOWN)

enum WSDBExceptionType
{
#define WSDB_ERROR(type) type,
  WSDB_ERRORS
#undef WSDB_ERROR
};

static std::string WSDBExceptionTypeToString(WSDBExceptionType type)
{
#define WSDB_ERROR(type) \
  case type: return #type;
  switch (type) {
    WSDB_ERRORS
  }
#undef WSDB_ERROR
}

/// usage:
/// throw WSDBException(WSDB_FILE_EXISTS, Q(Diskmanager), Q(CreateFile), file_name)

class WSDBException : public std::exception
{
public:
  WSDBException() = delete;
  explicit WSDBException(WSDBExceptionType type, std::string cname = {}, std::string fname = {}, std::string msg = {})
      : type_(type), cname_(std::move(cname)), fname_(std::move(fname)), msg_(std::move(msg))
  {
    std::string info_str = msg_.empty() ? "" : ": " + msg_;
    out_ = fmt::format("EXCEPTION <{}::{}>[{}]: {}", cname_, fname_, WSDBExceptionTypeToString(type_), info_str);
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
    return fmt::format("EXCEPTION [{}]: {}", WSDBExceptionTypeToString(type_), msg_);
  }

private:
};

#define WSDB_FETAL(class_, fun_, msg)                                                 \
  do {                                                                                \
    std::cerr << fmt::format("Fetal <{}::{}>: {}", #class_, #fun_, msg) << std::endl; \
    exit(1);                                                                          \
  } while (0)

#define WSDB_ASSERT(class_, fun_, expr, msg)                                                        \
  do {                                                                                              \
    if (!(expr)) {                                                                                  \
      std::cerr << fmt::format("Assert <{}::{}>[{}]: {}", #class_, #fun_, #expr, msg) << std::endl; \
      assert(0);                                                                                    \
    }                                                                                               \
  } while (0)

#define WSDB_STUDENT_TODO(lab, q, class_, func_)                                                        \
  do {                                                                                                  \
    std::cerr << fmt::format("Student TODO [{}.{}]: <{}::{}>", #lab, #q, #class_, #func_) << std::endl; \
    exit(1);                                                                                            \
  } while (0)

#define Q(x) #x

}  // namespace wsdb

#endif  // WSDB_ERROR_H
