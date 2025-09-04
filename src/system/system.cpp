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

#include <iostream>
#include <unistd.h>
#include <regex>
#include <csignal>

#include "system.h"
#include "../common/net/net.h"
#include "context.h"

namespace njudb {
SystemManager::SystemManager() = default;

void SystemManager::Init()
{
  // change working directory to the bin directory
  if (!std::filesystem::exists(DATA_DIR)) {
    std::filesystem::create_directory(DATA_DIR);
  }
  std::filesystem::current_path(DATA_DIR);

  disk_manager_        = std::make_unique<DiskManager>();
  log_manager_         = std::make_unique<LogManager>(disk_manager_.get());
  buffer_pool_manager_ = std::make_unique<BufferPoolManager>(disk_manager_.get(), log_manager_.get(), REPLACER_LRU_K);
  recovery_            = std::make_unique<Recovery>(disk_manager_.get(), buffer_pool_manager_.get());
  table_manager_       = std::make_unique<TableManager>(disk_manager_.get(), buffer_pool_manager_.get());
  index_manager_       = std::make_unique<IndexManager>(disk_manager_.get(), buffer_pool_manager_.get());
  parser_              = std::make_unique<Parser>();
  planner_             = std::make_unique<Planner>();
  executor_            = std::make_unique<Executor>();
  optimizer_           = std::make_unique<Optimizer>();
  txn_manager_         = std::make_unique<TxnManager>(log_manager_.get());
  net_controller_      = std::make_unique<NetController>();

  // first check TMP_DIR
  if (!std::filesystem::exists(TMP_DIR)) {
    std::filesystem::create_directory(TMP_DIR);
  }
  // read the dirs in the WORKING DIR and add database handles
  for (const auto &entry : std::filesystem::directory_iterator(".")) {
    if (entry.is_directory()) {
      auto db_name = entry.path().filename().string();
      if (db_name == TMP_DIR) {
        continue;
      }
      databases_[db_name] =
          std::make_unique<DatabaseHandle>(db_name, disk_manager_.get(), table_manager_.get(), index_manager_.get());
    }
  }
}

SystemManager::~SystemManager() {}

void SystemManager::CreateDatabase(const std::string &db_name)
{
  NJUDB_ASSERT(databases_.find(db_name) == databases_.end(), "Database already exists");
  // 2. create a new directory for the database
  std::filesystem::create_directory(db_name);
  // 3. create a new database handle
  databases_[db_name] =
      std::make_unique<DatabaseHandle>(db_name, disk_manager_.get(), table_manager_.get(), index_manager_.get());
  // 3.1. create .db file
  DiskManager::CreateFile(FILE_NAME(db_name, db_name, DB_SUFFIX));
}

void SystemManager::DropDatabase(const std::string &db_name) { NJUDB_THROW(NJUDB_NOT_IMPLEMENTED, ""); }

void SystemManager::Recover()
{
  for (auto &db : databases_) {
    recovery_->SetDBHandle(db.second.get());
    recovery_->AnalyzeLog();
    recovery_->Redo();
    recovery_->Undo();
  }
}

void SystemManager::SIGINTHandler(int sig)
{
  // flush all the logs into disk
  NJUDB_LOG("Received SIGINT signal, exiting the system...");
  is_running_ = false;
  log_manager_->FlushLog();
  NJUDB_LOG("Log flushed successfully.");
  net_controller_->Close();
  // close all databases
  for (auto &db : databases_) {
    db.second->Close();
  }
}

void SystemManager::Run()
{
  is_running_   = true;
  auto sig_func = [](int sig) { SystemManager::GetInstance()->SIGINTHandler(sig); };
  // register the SIGINT handler
  signal(SIGINT, sig_func);
  signal(SIGKILL, sig_func);
  // recover the system
  Recover();
  // start the server
  if (net_controller_->Listen() < 0) {
    NJUDB_LOG("ERROR on init server socket");
    return;
  }
  NJUDB_LOG("Server listening on port " + std::to_string(net::SERVER_PORT));
  while (is_running_) {
    auto client_sock = net_controller_->Accept();
    if (client_sock < 0) {
      NJUDB_LOG("ERROR on accept");
      continue;
    }
    // create a new thread to handle the client
    std::thread([client_sock]() {
      SystemManager::GetInstance()->ClientHandler(client_sock);
      close(client_sock);
    }).detach();
  }
  // close the server
  net_controller_->Close();
  // wait for the clean-up daemon
  std::this_thread::sleep_for(std::chrono::seconds(1));
  // exit the system
  NJUDB_LOG("Bye!");
}
void SystemManager::ClientHandler(int client_fd)
{
  // handle the client
  // 1. read the request
  // 2. parse the request
  // 3. execute the request
  // 4. send the response
  // 5. close the connection
  NJUDB_LOG(fmt::format("Client {} connected", client_fd));
  // 1. read the request
  Transaction txn{};
  Context     context(&txn, log_manager_.get(), nullptr, net_controller_.get(), client_fd);
  while (is_running_) {
    try {
      auto sql = net_controller_->ReadSQL(client_fd);
      NJUDB_LOG(fmt::format("Client {} sent: {}", client_fd, sql));
      if (sql == "exit;") {
        break;
      } else if (sql == "shutdown;") {
        is_running_ = false;
        break;
      }
      txn_manager_->SetTransaction(&txn);
      auto gm_tree = parser_->Parse(sql);
      auto plan    = planner_->PlanAST(gm_tree, context.db_);
      if (plan == nullptr || DoDBPlan(plan, &context) || DoExplainPlan(plan, &context)) {
        net_controller_->SendOK(client_fd);
      } else {
        /// plan is not a db plan
        plan           = optimizer_->Optimize(plan, context.db_);
        auto exec_tree = executor_->Translate(plan, context.db_);
        executor_->Execute(exec_tree, &context);
      }
      // commit transaction if this is a single sql statement
      if (!txn.IsExplicit()) {
        txn_manager_->Commit(txn.GetTxnId());
      }
    } catch (NJUDBException_ &e) {
      if (e.type_ == NJUDB_CLIENT_DOWN) {
        NJUDB_LOG(fmt::format("Client {} disconnected", client_fd));
        break;
      } else if (e.type_ == NJUDB_TXN_ABORTED) {
        txn_manager_->Abort(txn.GetTxnId());
      } else {
        NJUDB_LOG_ERROR(e.what());
        net_controller_->SendError(client_fd, e.short_what());
      }
    }
  }  // end of client while loop
  net_controller_->Remove(client_fd);
  if (context.db_ != nullptr) {
    context.db_->Close();
  }
}

bool SystemManager::DoDBPlan(const std::shared_ptr<AbstractPlan> &plan, Context *ctx)
{
  if (const auto cdb = std::dynamic_pointer_cast<CreateDBPlan>(plan)) {
    if (cdb->db_name_ == TMP_DIR) {
      NJUDB_THROW(NJUDB_INVALID_SQL, fmt::format("invalid db name: {}", cdb->db_name_));
    }
    if (databases_.find(cdb->db_name_) == databases_.end()) {
      CreateDatabase(cdb->db_name_);
    } else {
      NJUDB_THROW(NJUDB_DB_EXISTS, fmt::format("{}", cdb->db_name_));
    }
    return true;
  } else if (const auto odb = std::dynamic_pointer_cast<OpenDBPlan>(plan)) {
    if (databases_.find(odb->db_name_) == databases_.end()) {
      NJUDB_THROW(NJUDB_DB_MISS, fmt::format("{}", odb->db_name_));
    } else {
      ctx->db_ = databases_[odb->db_name_].get();
      ctx->db_->ref_cnt_++;
      if (ctx->db_->ref_cnt_ == 1) {
        ctx->db_->Open();
      }
    }
    return true;
  }
  return false;
}

bool SystemManager::DoExplainPlan(const std::shared_ptr<AbstractPlan> &plan, Context *ctx)
{
  if (const auto exp = std::dynamic_pointer_cast<ExplainPlan>(plan)) {
    auto logical_str   = fmt::format("---\nLogical Plan:\n{}", exp->logical_plan_->ToString(0));
    auto physical_plan = optimizer_->Optimize(exp->logical_plan_, ctx->db_);
    auto physical_str  = fmt::format("---\nPhysical Plan:\n{}", physical_plan->ToString(0));
    net_controller_->SendRawString(ctx->client_fd_, logical_str);
    net_controller_->SendRawString(ctx->client_fd_, physical_str);
    return true;
  }
  return false;
}

}  // namespace njudb