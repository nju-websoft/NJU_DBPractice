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

#ifndef NJUDB_SYSTEM_H
#define NJUDB_SYSTEM_H

#include "storage/storage.h"
#include "execution/executor.h"
#include "parser/parser.h"
#include "plan/planner.h"
#include "optimizer/optimizer.h"
#include "log/log_manager.h"
#include "log/recovery.h"
#include "concurrency/txn_manager.h"
#include "handle/database_handle.h"

namespace njudb {

/**
 * @brief SystemManager is the main entry point for the system.
 * It manages all the components of njudb,
 * and is responsible for creating, dropping databases and managing the database handles.
 */
class SystemManager
{
public:
  SystemManager();

  ~SystemManager();

  void CreateDatabase(const std::string &db_name);

  void DropDatabase(const std::string &db_name);

  void Init();

  void Run();

private:
  bool DoDBPlan(const std::shared_ptr<AbstractPlan> &plan, Context *ctx);

  bool DoExplainPlan(const std::shared_ptr<AbstractPlan> &plan, Context *ctx);

  void SIGINTHandler(int sig);

  void ClientHandler(int client_fd);

  void Recover();

public:
  // The only instance of the SystemManager
  static SystemManager *GetInstance()
  {
    static SystemManager instance;
    return &instance;
  }

private:
  std::unique_ptr<DiskManager>       disk_manager_;
  std::unique_ptr<LogManager>        log_manager_;
  std::unique_ptr<BufferPoolManager> buffer_pool_manager_;
  std::unique_ptr<Recovery>          recovery_;
  std::unique_ptr<TableManager>      table_manager_;
  std::unique_ptr<IndexManager>      index_manager_;
  std::unique_ptr<Parser>            parser_;
  std::unique_ptr<Planner>           planner_;
  std::unique_ptr<Executor>          executor_;
  std::unique_ptr<Optimizer>         optimizer_;
  std::unique_ptr<TxnManager>        txn_manager_;
  std::unique_ptr<NetController>     net_controller_;

  bool                  is_running_{false};  // indicates whether the system is running

  std::unordered_map<std::string, std::unique_ptr<DatabaseHandle>> databases_;
};

}  // namespace njudb

#endif  // NJUDB_SYSTEM_H
