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

#ifndef NJUDB_LOG_MANAGER_H
#define NJUDB_LOG_MANAGER_H

#include "storage/disk/disk_manager.h"

namespace njudb {
class LogManager
{
public:
  explicit LogManager(DiskManager *disk_manager);
  ~LogManager() = default;

  void FlushLog();
};
}  // namespace njudb

#endif  // NJUDB_LOG_MANAGER_H
