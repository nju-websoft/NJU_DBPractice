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
#include <sys/socket.h>
#include <netinet/in.h>
#include "net.h"
#include "../error.h"
#include "../micro.h"
namespace net {

static ssize_t readn(int sockfd, char *buf, int len)
{
  ssize_t n;
  ssize_t nleft = len;
  while (nleft > 0) {
    n = read(sockfd, buf, nleft);
    if (n < 0) {
      WSDB_LOG("ERROR reading from socket");
      return -1;
    } else if (n == 0) {
      WSDB_LOG(fmt::format("connection to {} closed", sockfd));
      return -1;
    }
    nleft -= n;
    buf += n;
  }
  return len;
}

static ssize_t writen(int sockfd, char *buf, int len)
{
  ssize_t n;
  ssize_t nleft = len;
  while (nleft > 0) {
    n = write(sockfd, buf, nleft);
    if (n < 0) {
      WSDB_LOG("ERROR writing to socket");
      return -1;
    } else if (n == 0) {
      WSDB_LOG(fmt::format("connection to {} closed", sockfd));
      return -1;
    }
    nleft -= n;
    buf += n;
  }
  return len;
}

int ReadNetPkg(int sockfd, NetPkg &pkg)
{
  // read pkg type
  auto n = readn(sockfd, (char *)&pkg.type_, sizeof(pkg.type_));
  if (n <= 0)
    return static_cast<int>(n);
  // read pkg length
  n = readn(sockfd, (char *)&pkg.len_, sizeof(pkg.len_));
  if (n <= 0)
    return static_cast<int>(n);
  // read pkg data
  if (pkg.len_ == 0) {
    return static_cast<int>(sizeof(pkg.type_) + sizeof(pkg.len_));
  }
  n = readn(sockfd, pkg.buf_, static_cast<int>(pkg.len_));
  if (n <= 0)
    return static_cast<int>(n);
  return static_cast<int>(sizeof(pkg.type_) + sizeof(pkg.len_) + n);
}

int WriteNetPkg(int sockfd, NetPkg &pkg)
{
  // write pkg type
  auto n = writen(sockfd, (char *)&pkg.type_, sizeof(pkg.type_));
  if (n <= 0)
    return static_cast<int>(n);
  // write pkg length
  n = writen(sockfd, (char *)&pkg.len_, sizeof(pkg.len_));
  if (n <= 0)
    return static_cast<int>(n);
  // write pkg data
  if (pkg.len_ == 0) {
    return static_cast<int>(sizeof(pkg.type_) + sizeof(pkg.len_));
  }
  n = writen(sockfd, pkg.buf_, static_cast<int>(pkg.len_));
  if (n <= 0)
    return static_cast<int>(n);
  return static_cast<int>(sizeof(pkg.type_) + sizeof(pkg.len_) + n);
}
}  // namespace net
