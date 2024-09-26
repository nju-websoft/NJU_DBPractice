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
// Created by ziqi on 2024/7/19.
//

#ifndef WSDB_MICRO_H
#define WSDB_MICRO_H

#include <memory>
#include <filesystem>
#include "fmt/format.h"

#define DISABLE_COPY_AND_ASSIGN(classname)          \
  classname(const classname &)            = delete; \
  classname &operator=(const classname &) = delete;

#define DISABLE_MOVE_AND_ASSIGN(classname)     \
  classname(classname &&)            = delete; \
  classname &operator=(classname &&) = delete;

#define DISABLE_COPY_MOVE_AND_ASSIGN(classname) \
  DISABLE_COPY_AND_ASSIGN(classname)            \
  DISABLE_MOVE_AND_ASSIGN(classname)

#define DEFINE_SHARED_PTR(type) using type##Sptr = std::shared_ptr<type>
#define DEFINE_UNIQUE_PTR(type) using type##Uptr = std::unique_ptr<type>

#define FILE_NAME(db_name, obj_name, suffix) (fmt::format("{}/{}{}", db_name, obj_name, suffix))
#define OBJNAME_FROM_FILENAME(filename) (std::filesystem::path(filename).stem().string())

#define WSDB_LOG(msg) std::cout << fmt::format("\033[32mLOG <{}::{}>: {}\033[0m\n", __func__, __LINE__, msg)

#define DECLARE_ENUM(EnumName, ...) \
  enum EnumName                                    \
  {                                                \
    ENUM_ENTITIES                                  \
  };

// Helper macro to generate case statements
#define ENUM_TO_STRING_BODY(EnumName, ...)                   \
  inline const char *EnumName##ToString(EnumName value) \
  {                                                     \
    switch (value) {                                    \
      ENUM_ENTITIES                                     \
      default: return "UNKNOWN";                        \
    }                                                   \
  }

#endif  // WSDB_MICRO_H
