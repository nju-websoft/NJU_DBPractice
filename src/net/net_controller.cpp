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

#include "net_controller.h"

#include <sys/socket.h>
#include <netinet/in.h>

namespace njudb {
NetController::NetController()
{
  max_client_  = net::MAX_CLIENTS;
  listen_port_ = net::SERVER_PORT;
}

auto NetController::Listen() -> int
{
  if (server_fd_ != 0) {
    return -1;
  }
  struct sockaddr_in serv_addr
  {};

  server_fd_ = socket(AF_INET, SOCK_STREAM, 0);
  if (server_fd_ < 0) {
    NJUDB_LOG("ERROR opening socket");
    return -1;
  }
  bzero((char *)&serv_addr, sizeof(serv_addr));
  serv_addr.sin_family      = AF_INET;
  serv_addr.sin_addr.s_addr = INADDR_ANY;
  serv_addr.sin_port        = htons(listen_port_);
  if (bind(server_fd_, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
    NJUDB_LOG("ERROR on binding");
    return -1;
  }
  if (listen(server_fd_, max_client_) < 0) {
    NJUDB_LOG("ERROR on listen");
    return -1;
  }
  return 0;
}
auto NetController::Accept() const -> int
{
  int client_sock;
  struct sockaddr_in cli_addr
  {};
  socklen_t clilen = sizeof(cli_addr);
  client_sock      = accept(server_fd_, (struct sockaddr *)&cli_addr, &clilen);
  if (client_sock < 0) {
    NJUDB_LOG("ERROR on accept");
    return -1;
  }
  return client_sock;
}
void NetController::Close() const { close(server_fd_); }
auto NetController::ReadSQL(int fd) -> std::string
{
  auto &pkg_ = client_buffer_[fd];
  auto  err  = net::ReadNetPkg(fd, pkg_);
  if (err <= 0) {
    NJUDB_THROW(NJUDB_CLIENT_DOWN, "");
  }
  if (pkg_.type_ != net::NET_PKG_QUERY) {
    NJUDB_LOG("ERROR: not a query package");
    return "";
  }
  return {pkg_.buf_, pkg_.len_};
}

void NetController::SendRecHeader(int fd, const RecordSchema *header)
{
  // header format: {field_name}\t{field_name}\t ...
  auto &pkg_ = client_buffer_[fd];
  pkg_.type_ = net::NET_PKG_REC_HEADER;
  std::string header_str;
  for (int i = 0; i < static_cast<int>(header->GetFieldCount()); ++i) {
    auto &field = header->GetFieldAt(i);
    // check alias
    if (field.alias_.empty()) {
      if (field.is_agg_) {
        if (field.agg_type_ == AGG_COUNT_STAR) {
          header_str += AggTypeToString(field.agg_type_);
        } else {
          header_str += fmt::format("{}({})", AggTypeToString(field.agg_type_), field.field_.field_name_);
        }
      } else {
        header_str += field.field_.field_name_;
      }
    } else {
      header_str += field.alias_;
    }
    header_str += '\t';
  }
  pkg_.len_ = header_str.size();
  memcpy(pkg_.buf_, header_str.c_str(), pkg_.len_);
  FlushSend(fd);
}
void NetController::SendRec(int fd, const Record *rec)
{
  // append record to buffer and flush if buffer is full
  auto &pkg_ = client_buffer_[fd];
  pkg_.type_ = net::NET_PKG_REC_BODY;
  // record format: {field_value}\t{field_value}\t ...
  std::string rec_str;
  for (int i = 0; i < static_cast<int>(rec->GetSchema()->GetFieldCount()); ++i) {
    auto v = rec->GetValueAt(i);
    rec_str += v->ToString();
    rec_str += '\t';
  }
  // FIXME: accumulate records and send, may cause client waiting sometimes
  //  if (pkg_.len_ + rec_str.size() > net::NET_BUFFER_SIZE) {
  //    FlushSend(fd);
  //  }
  memcpy(pkg_.buf_ + pkg_.len_, rec_str.c_str(), rec_str.size());
  // add '\0' to separate records
  pkg_.len_ += rec_str.size();
  pkg_.buf_[pkg_.len_++] = '\0';
  FlushSend(fd);
}
void NetController::SendRecFinish(int fd)
{
  auto &pkg_ = client_buffer_[fd];
  //  if (pkg_.type_ == net::NET_PKG_REC_BODY) {
  //    FlushSend(fd);
  //  }
  pkg_.type_ = net::NET_PKG_REC_END;
  pkg_.len_  = 0;
  FlushSend(fd);
}
void NetController::SendError(int fd, const std::string &error_msg)
{
  auto &pkg_ = client_buffer_[fd];
  pkg_.type_ = net::NET_PKG_ERROR;
  pkg_.len_  = error_msg.size();
  memcpy(pkg_.buf_, error_msg.c_str(), pkg_.len_);
  FlushSend(fd);
}

void NetController::SendOK(int fd)
{
  auto &pkg_ = client_buffer_[fd];
  pkg_.type_ = net::NET_PKG_OK;
  pkg_.len_  = 0;
  FlushSend(fd);
}

void NetController::SendRawString(int fd, const std::string &str)
{
  auto &pkg_ = client_buffer_[fd];
  pkg_.type_ = net::NET_PKG_RAW_STRING;
  pkg_.len_  = str.size();
  memcpy(pkg_.buf_, str.c_str(), pkg_.len_);
  FlushSend(fd);
}

void NetController::FlushSend(int fd)
{
  auto err = net::WriteNetPkg(fd, client_buffer_[fd]);
  if (err <= 0) {
    NJUDB_THROW(NJUDB_CLIENT_DOWN, "");
  }
  client_buffer_[fd].len_ = 0;
}

void NetController::Remove(int fd)
{
  if (client_buffer_.find(fd) != client_buffer_.end()) {
    client_buffer_.erase(fd);
  }
}

}  // namespace njudb