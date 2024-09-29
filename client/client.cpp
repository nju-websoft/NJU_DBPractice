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
#include <vector>
#include <sstream>
#include <fstream>
#include <readline/history.h>
#include <readline/readline.h>
#include "../common/net/net.h"
#include "../common/error.h"
#include "../common/micro.h"

#include "argparse/argparse.hpp"

// receive and send data with server, show output to stdout, record command
// history to support up and down arrow key

auto ltrim(std::string &s) -> std::string &
{
  s.erase(s.begin(), std::find_if(s.begin(), s.end(), [](unsigned char ch) { return !std::isspace(ch); }));
  return s;
}

auto rtrim(std::string &s) -> std::string &
{
  s.erase(std::find_if(s.rbegin(), s.rend(), [](unsigned char ch) { return !std::isspace(ch); }).base(), s.end());
  return s;
}

auto trim(std::string &s) -> std::string & { return ltrim(rtrim(s)); }

void Split(const std::string &s, char delim, std::vector<std::string> &elems)
{
  std::stringstream ss(s);
  std::string       item;
  while (std::getline(ss, item, delim)) {
    elems.push_back(item);
  }
}

class Client
{

public:
  Client() = default;

  DISABLE_COPY_MOVE_AND_ASSIGN(Client)

  void Init(const std::string &input, const std::string &output)
  {
    if (!input.empty()) {
      input_.open(input);
      if (!input_.is_open()) {
        err_no_ = -1;
        WSDB_LOG("ERROR opening input file");
      }
      is_interactive_ = false;
    } else {
      input_.copyfmt(std::cin);
      input_.clear(std::cin.rdstate());
      input_.basic_ios<char>::rdbuf(std::cin.rdbuf());
    }
    if (!output.empty()) {
      output_file_.open(output);
      if (!output_file_.is_open()) {
        err_no_ = -1;
        WSDB_LOG("ERROR opening output file");
      }
      output_.copyfmt(output_file_);
      output_.clear(output_file_.rdstate());
      output_.rdbuf(output_file_.rdbuf());
    } else {
      output_.copyfmt(std::cout);
      output_.clear(std::cout.rdstate());
      output_.rdbuf(std::cout.rdbuf());
    }
  }

  void Shutdown()
  {
    if (input_.is_open()) {
      input_.close();
    }
    if (output_file_.is_open()) {
      output_file_.close();
    }
  }

  void Run()
  {
    OpenSocket();
    std::string sql;
    while (err_no_ >= 0) {
      // if input is not from file, show prompt
      if (is_interactive_) {
        char *line;
        if (sql.empty())
          line = readline("wsdb> ");
        else
          line = readline(" ... ");
        if (line == nullptr) {
          // EOF encountered
          break;
        }
        add_history(line);
        sql += line;
        sql += '\n';
        free(line);
        if (sql == "exit;") {
          SendSql(sql);
          break;
        } else if (sql.find(';') != std::string::npos) {
          SendSql(sql);
          DoReceive();
          sql.clear();
        }
      } else {
        std::string line;
        while (std::getline(input_, line)) {
          trim(line);
          sql += line;
          if (line.find(';') != std::string::npos) {
            break;
          }
          sql += '\n';
        }
        if (sql.empty()) {
          break;
        }
        if (sql == "exit;") {
          SendSql(sql);
          break;
        }
        SendSql(sql);
        DoReceive();
        sql.clear();
      }
    }
    CloseSocket();
    if (is_interactive_)
      std::cout << "Bye!" << std::endl;
  }

private:
  void OpenSocket()
  {
    sock_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if ((err_no_ = sock_fd_) < 0) {
      WSDB_LOG("ERROR opening socket");
    }
    sockaddr_in serverAddress{};
    serverAddress.sin_family      = AF_INET;
    serverAddress.sin_port        = htons(net::SERVER_PORT);
    serverAddress.sin_addr.s_addr = INADDR_ANY;

    // sending connection request
    if ((err_no_ = connect(sock_fd_, (struct sockaddr *)&serverAddress, sizeof(serverAddress))) < 0) {
      WSDB_LOG("ERROR connecting");
    }
  }

  void CloseSocket() const { close(sock_fd_); }

  void SendSql(const std::string &sql)
  {
    pkg_.type_ = net::NET_PKG_QUERY;
    pkg_.len_  = sql.size();
    memcpy(pkg_.buf_, sql.c_str(), sql.size());
    if ((err_no_ = net::WriteNetPkg(sock_fd_, pkg_)) < 0) {
      WSDB_LOG("ERROR writing to socket");
    }
  }

  void Receive()
  {
    if ((err_no_ = net::ReadNetPkg(sock_fd_, pkg_)) < 0) {
      WSDB_LOG("ERROR reading from socket");
    }
  }

  void DoReceive()
  {
    std::vector<int> col_width;
    size_t           rec_num = 0;
    while (err_no_ >= 0) {
      Receive();
      if (pkg_.type_ == net::NET_PKG_OK) {
        break;
      } else if (pkg_.type_ == net::NET_PKG_RAW_STRING) {
        output_ << std::string(pkg_.buf_, pkg_.len_) << std::endl;
      } else if (pkg_.type_ == net::NET_PKG_ERROR) {
        // show the error on the screen
        output_ << std::string(pkg_.buf_, pkg_.len_) << std::endl;
        break;
      } else if (pkg_.type_ == net::NET_PKG_REC_HEADER) {
        output_ << std::endl;
        PrintRecord(std::string(pkg_.buf_, pkg_.len_), col_width);
      } else if (pkg_.type_ == net::NET_PKG_REC_BODY) {
        // records are seperated by '\0'
        std::vector<std::string> records;
        Split(std::string(pkg_.buf_, pkg_.len_), '\0', records);
        for (auto &rec : records) {
          PrintRecord(rec, col_width);
        }
        rec_num += records.size();
      } else if (pkg_.type_ == net::NET_PKG_REC_END) {
        // print bottom line
        PrintSeperator(col_width);
        output_ << fmt::format("Total tuple(s): {}", rec_num) << std::endl;
        break;
      }
    }
  }

  void PrintSeperator(const std::vector<int> &field_width)
  {
    output_ << '+';
    for (auto &w : field_width) {
      for (int i = 0; i < w; ++i) {
        output_ << '-';
      }
      output_ << '+';
    }
    output_ << std::endl;
  }

  void PrintRecord(const std::string &rec, std::vector<int> &col_width)
  {
    // the header is one line seperated by '\t', should construct a table using '+' and '-'
    std::vector<std::string> fields;
    Split(rec, '\t', fields);
    if (col_width.empty()) {
      for (auto &f : fields) {
        col_width.push_back(std::max(static_cast<int>(f.size()) + 2, 14));
      }
    }
    // print seperators
    PrintSeperator(col_width);
    // print fields
    size_t height = 1;
    // get max height according to field size
    for (size_t i = 0; i < fields.size(); ++i) {
      auto  &f = fields[i];
      size_t h = f.size() / (col_width[i] - 2) + 1;
      height   = std::max(height, h);
    }
    for (size_t i = 0; i < height; ++i) {
      output_ << "| ";
      for (size_t j = 0; j < fields.size(); ++j) {
        auto &f = fields[j];
        if (i < f.size() / (col_width[j] - 2) + 1) {
          auto sub_str = f.substr(i * (col_width[j] - 2), col_width[j] - 2);
          output_ << sub_str;
          // additional space
          output_ << std::string(col_width[j] - sub_str.size() - 2, ' ');
        } else {
          output_ << std::string(col_width[j] - 2, ' ');
        }
        output_ << " | ";
      }
      output_ << std::endl;
    }
  }

private:
  bool                     is_interactive_{true};
  std::ifstream            input_;
  std::ofstream            output_file_;
  std::ostream             output_{std::cout.rdbuf()};
  net::NetPkg              pkg_;
  int                      sock_fd_{-1};
  int                      err_no_ = 0;
  std::vector<std::string> history_{};
};

int main(int argc, char *argv[])
{
  argparse::ArgumentParser program("wsdb client");
  program.add_argument("-v", "--version").help("show version").default_value(false).implicit_value(true);
  program.add_argument("-h", "--help").help("show help").default_value(false).implicit_value(true);
  program.add_argument("-i", "--input").required().help("input file").default_value(std::string());
  program.add_argument("-o", "--output").required().help("output file").default_value(std::string());

  Client client;
  try {
    program.parse_args(argc, argv);
    if (program.get<bool>("--help")) {
      std::cout << program;
      return 0;
    }
    if (program.get<bool>("--version")) {
      std::cout << "wsdb client 0.1" << std::endl;
      return 0;
    }
    client.Init(program.get<std::string>("--input"), program.get<std::string>("--output"));
  } catch (const std::runtime_error &err) {
    std::cerr << err.what() << std::endl;
    return 1;
  }
  client.Run();
  client.Shutdown();
  return 0;
}