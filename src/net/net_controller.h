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
// Created by ziqi on 2024/8/7.
//

#ifndef NJUDB_NET_CONTROLLER_H
#define NJUDB_NET_CONTROLLER_H
#include <unordered_map>

#include "common/record.h"
#include "../common/net/net.h"

namespace njudb {

class NetController
{
public:
  NetController();
  auto Listen() -> int;

  auto Accept() const -> int;

  void Close() const;

  auto ReadSQL(int fd) -> std::string;

  void SendRecHeader(int fd, const RecordSchema *header);

  /// record will be stored until buffer is full and flush to socket
  void SendRec(int fd, const Record *rec);

  void SendRecFinish(int fd);

  void SendError(int fd, const std::string &error_msg);

  void SendOK(int fd);

  void SendRawString(int fd, const std::string &str);

  void FlushSend(int fd);

  void Remove(int fd);

private:
  // currently receive and send use the same pkg_
  int                                  server_fd_{0};
  int                                  listen_port_{0};
  int                                  max_client_{0};
  std::unordered_map<int, net::NetPkg> client_buffer_;
};

}  // namespace njudb

#endif  // NJUDB_NET_CONTROLLER_H
