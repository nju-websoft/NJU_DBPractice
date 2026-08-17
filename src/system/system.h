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
#include "log/checkpoint_manager.h"
#include "concurrency/txn_manager.h"
#include "concurrency/version_manager.h"
#include "concurrency/timestamp_oracle.h"
#include "concurrency/garbage_collector.h"
#include "concurrency/concurrency_protocol.h"
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

  void CreateDatabase(
      const std::string &db_name, ConcurrencyProtocol protocol = ConcurrencyProtocol::TWO_PHASE_LOCKING);

  void DropDatabase(const std::string &db_name);

  void Init();

  void Run();

  // Public accessors for transaction executors
  auto GetTxnManager() -> TxnManager* { return txn_manager_.get(); }
  
private:
  auto LoadDatabaseProtocol(const std::string &db_name) const -> ConcurrencyProtocol;

  void PersistDatabaseProtocol(const std::string &db_name, ConcurrencyProtocol protocol) const;

  bool HandleTxnCommand(const std::shared_ptr<ast::TreeNode> &ast, Context *ctx, Transaction *&txn, txn_id_t &txn_id,
      int client_fd);

  bool DoDBPlan(
      const std::shared_ptr<AbstractPlan> &plan, Context *ctx, Transaction *&txn, txn_id_t &txn_id);

  bool DoExplainPlan(const std::shared_ptr<AbstractPlan> &plan, Context *ctx);

  void SIGINTHandler(int sig);

  void ClientHandler(int client_fd);
  
  /**
   * @brief Abort a transaction without starting a new one
   * @param txn Transaction to abort
   * @param db_handle DatabaseHandle for accessing tables
   * 
   * Used when the client is disconnecting or exiting, so no new transaction is needed.
   * Delegates to TxnManager::Abort() which handles protocol-specific rollback.
   */
  void AbortTransaction(Transaction *txn, DatabaseHandle *db_handle);
  
  /**
   * @brief Abort a transaction and start a new one
   * @param txn Reference to transaction pointer (will be updated with new transaction)
   * @param txn_id Reference to transaction ID (will be updated with new ID)
   * @param ctx Context containing database handle and other state
   * 
   * Used when the client continues executing more commands after the abort.
   * This helper method consolidates the common abort logic:
   *  - Rollback modifications (for 2PL protocol)
   *  - Abort the transaction
   *  - Start a new transaction to replace the aborted one
   */
  void AbortTransactionAndStartNew(Transaction *&txn, txn_id_t &txn_id, Context *ctx);

  void RestartTransactionForDatabase(Transaction *&txn, txn_id_t &txn_id, Context *ctx);

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
  std::unique_ptr<CheckpointManager> checkpoint_manager_;
  std::unique_ptr<TableManager>      table_manager_;
  std::unique_ptr<IndexManager>      index_manager_;
  std::unique_ptr<Parser>            parser_;
  std::unique_ptr<Planner>           planner_;
  std::unique_ptr<Executor>          executor_;
  std::unique_ptr<Optimizer>         optimizer_;
  std::unique_ptr<LockManager>       lock_manager_;
  std::unique_ptr<TimestampOracle>   timestamp_oracle_;
  std::unique_ptr<TxnManager>        txn_manager_;
  std::unique_ptr<VersionManager>    version_manager_;
  std::unique_ptr<GarbageCollector>  garbage_collector_;
  std::unique_ptr<NetController>     net_controller_;

  bool                  is_running_{false};  // indicates whether the system is running

  std::unordered_map<std::string, std::unique_ptr<DatabaseHandle>> databases_;
};

}  // namespace njudb

#endif  // NJUDB_SYSTEM_H
