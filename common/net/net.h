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
// Created by ziqi on 2024/7/31.
//

#ifndef WSDB_NET_H
#define WSDB_NET_H

#include <unistd.h>

namespace net {

constexpr size_t MAX_CLIENTS     = 10;
constexpr size_t NET_BUFFER_SIZE = 2048;
constexpr int    SERVER_PORT     = 5001;
constexpr int    CLIENT_PORT     = 5002;

enum NetPkgType
{
  NET_PKG_QUERY = 0,
  NET_PKG_REC_HEADER,
  NET_PKG_REC_BODY,
  NET_PKG_REC_END,
  NET_PKG_ERROR,
  NET_PKG_OK,
  NET_PKG_RAW_STRING
};

struct NetPkg
{
  NetPkgType type_{};
  size_t     len_{0};
  char       buf_[NET_BUFFER_SIZE]{0};
};

int ReadNetPkg(int sockfd, NetPkg &pkg);

int WriteNetPkg(int sockfd, NetPkg &pkg);
}  // namespace net

#endif  // WSDB_NET_H
