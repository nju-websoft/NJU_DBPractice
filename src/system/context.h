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
// Created by ziqi on 2024/7/27.
//

#ifndef NJUDB_CONTEXT_H
#define NJUDB_CONTEXT_H
#include "concurrency/txn_manager.h"
#include "log/log_manager.h"
#include "handle/database_handle.h"
#include "net/net_controller.h"

namespace njudb {
struct Context
{
  Transaction    *txn;
  LogManager     *log_manager;
  DatabaseHandle *db_;
  NetController  *nt_ctl_;
  int             client_fd_;

  Context(Transaction *txn, LogManager *log_manager, DatabaseHandle *db_hdl, NetController *nt_ctl_, int client_fd)
      : txn(txn), log_manager(log_manager), db_(db_hdl), nt_ctl_(nt_ctl_), client_fd_(client_fd)
  {}
};
}  // namespace njudb

#endif  // NJUDB_CONTEXT_H
