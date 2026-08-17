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
#include <fstream>
#include <unistd.h>
#include <regex>
#include <csignal>

#include "system.h"
#include "../common/net/net.h"
#include "context.h"
#include "concurrency/lock_manager.h"
#include "handle/table_handle.h"

namespace njudb {
SystemManager::SystemManager() = default;

void SystemManager::PersistDatabaseProtocol(const std::string &db_name, ConcurrencyProtocol protocol) const
{
  std::ofstream protocol_file(std::filesystem::path(db_name) / ".concurrency_protocol", std::ios::trunc);
  NJUDB_ASSERT(protocol_file.is_open(), fmt::format("Cannot persist concurrency protocol for database {}", db_name));
  protocol_file << (protocol == ConcurrencyProtocol::MVCC ? "mvcc" : "2pl");
}

auto SystemManager::LoadDatabaseProtocol(const std::string &db_name) const -> ConcurrencyProtocol
{
  const auto protocol_path = std::filesystem::path(db_name) / ".concurrency_protocol";
  if (!std::filesystem::exists(protocol_path)) {
    PersistDatabaseProtocol(db_name, ConcurrencyProtocol::TWO_PHASE_LOCKING);
    return ConcurrencyProtocol::TWO_PHASE_LOCKING;
  }

  std::ifstream protocol_file(protocol_path);
  std::string protocol_name;
  protocol_file >> protocol_name;
  if (protocol_name == "mvcc") {
#if NJUDB_ENABLE_LAB05_MVCC
    return ConcurrencyProtocol::MVCC;
#else
    NJUDB_THROW(NJUDB_INVALID_SQL,
        fmt::format("Database {} uses MVCC; rebuild with -DENABLE_LAB05_MVCC=ON", db_name));
#endif
  }
  if (protocol_name == "2pl") {
    return ConcurrencyProtocol::TWO_PHASE_LOCKING;
  }
  NJUDB_THROW(NJUDB_INVALID_SQL, fmt::format("Invalid concurrency protocol metadata in {}", protocol_path.string()));
}

void SystemManager::Init()
{
  // change working directory to the bin directory
  if (!std::filesystem::exists(DATA_DIR)) {
    std::filesystem::create_directory(DATA_DIR);
  }
  std::filesystem::current_path(DATA_DIR);

  disk_manager_        = std::make_unique<DiskManager>();
#if NJUDB_ENABLE_LAB06_WAL
  log_manager_         = std::make_unique<LogManager>(disk_manager_.get());
#endif
  buffer_pool_manager_ = std::make_unique<BufferPoolManager>(disk_manager_.get(), log_manager_.get(), REPLACER_LRU_K);
  table_manager_       = std::make_unique<TableManager>(disk_manager_.get(), buffer_pool_manager_.get());
  index_manager_       = std::make_unique<IndexManager>(disk_manager_.get(), buffer_pool_manager_.get());
  parser_              = std::make_unique<Parser>();
  planner_             = std::make_unique<Planner>();
  executor_            = std::make_unique<Executor>();
  optimizer_           = std::make_unique<Optimizer>();
  lock_manager_        = std::make_unique<LockManager>();
  timestamp_oracle_    = std::make_unique<TimestampOracle>();
  txn_manager_ = std::make_unique<TxnManager>(log_manager_.get(), lock_manager_.get(), timestamp_oracle_.get());
  lock_manager_->SetTxnManager(txn_manager_.get());
  version_manager_     = std::make_unique<VersionManager>();
  garbage_collector_   = std::make_unique<GarbageCollector>(version_manager_.get(), timestamp_oracle_.get());
  net_controller_      = std::make_unique<NetController>();

#if NJUDB_ENABLE_LAB06_WAL
  // Create recovery manager (with all required components)
  recovery_ = std::make_unique<Recovery>(
      disk_manager_.get(), buffer_pool_manager_.get(), log_manager_.get(), txn_manager_.get());

  // Create checkpoint manager (for periodic checkpointing)
  checkpoint_manager_ = std::make_unique<CheckpointManager>(
      log_manager_.get(), txn_manager_.get(), buffer_pool_manager_.get(), version_manager_.get());
#endif

  // first check TMP_DIR
  if (!std::filesystem::exists(TMP_DIR)) {
    std::filesystem::create_directory(TMP_DIR);
  }
  // read the dirs in the WORKING DIR and add database handles
  // CRITICAL FIX: Load databases BEFORE recovery so table handles are available for undo
  for (const auto &entry : std::filesystem::directory_iterator(".")) {
    if (entry.is_directory()) {
      auto db_name = entry.path().filename().string();
      if (db_name == TMP_DIR) {
        continue;
      }
      auto db_handle =
          std::make_unique<DatabaseHandle>(db_name, disk_manager_.get(), table_manager_.get(), index_manager_.get(),
              LoadDatabaseProtocol(db_name));
      db_handle->SetLockManager(lock_manager_.get());
      db_handle->SetVersionManager(version_manager_.get());
      db_handle->SetLogManager(log_manager_.get());
      db_handle->SetKeepOpenWhenIdle(true);
      db_handle->Open();
      databases_[db_name] = std::move(db_handle);
    }
  }

#if NJUDB_ENABLE_LAB06_WAL
  // Recovery routes each log record by its persistent database/table path.
  recovery_->SetDBHandles(databases_);
  recovery_->Recover();
  for (auto &[db_name, db_handle] : databases_) {
    (void)db_name;
    db_handle->RebuildIndexes();
  }

  // Background work starts only after tables and derived indexes are consistent.
  if (CHECKPOINT_INTERVAL_SECONDS > 0) {
    checkpoint_manager_->StartPeriodicCheckpointing(CHECKPOINT_INTERVAL_SECONDS);
  }
#endif

#if NJUDB_ENABLE_LAB05_MVCC
  garbage_collector_->Start();
#endif
}

SystemManager::~SystemManager()
{
  if (garbage_collector_) {
    garbage_collector_->Stop();
  }
  // Create checkpoint before shutdown to minimize recovery time on next startup
  if (checkpoint_manager_) {
    checkpoint_manager_->CreateCheckpoint();
  }
  // Flush all logs to ensure durability
  if (log_manager_) {
    log_manager_->FlushLog();
  }
}

void SystemManager::CreateDatabase(const std::string &db_name, ConcurrencyProtocol protocol)
{
#if !NJUDB_ENABLE_LAB05_MVCC
  if (protocol == ConcurrencyProtocol::MVCC) {
    NJUDB_THROW(NJUDB_INVALID_SQL, "MVCC bonus is disabled; rebuild with -DENABLE_LAB05_MVCC=ON");
  }
#endif
  NJUDB_ASSERT(databases_.find(db_name) == databases_.end(), "Database already exists");

  // 2. create a new directory for the database
  std::filesystem::create_directory(db_name);
  PersistDatabaseProtocol(db_name, protocol);
  // 3. create a new database handle
  auto db_handle = std::make_unique<DatabaseHandle>(
      db_name, disk_manager_.get(), table_manager_.get(), index_manager_.get(), protocol);
  db_handle->SetLockManager(lock_manager_.get());
  db_handle->SetVersionManager(version_manager_.get());
  db_handle->SetLogManager(log_manager_.get());
  db_handle->SetKeepOpenWhenIdle(true);
  // 3.1. create .db file
  DiskManager::CreateFile(FILE_NAME(db_name, db_name, DB_SUFFIX));
  db_handle->FlushMeta();
  databases_[db_name] = std::move(db_handle);
}

void SystemManager::DropDatabase(const std::string &db_name) { NJUDB_THROW(NJUDB_NOT_IMPLEMENTED, ""); }

void SystemManager::SIGINTHandler(int sig)
{
  if (garbage_collector_) {
    garbage_collector_->Stop();
  }
  // Create checkpoint before shutdown
  NJUDB_LOG("Received SIGINT signal, creating checkpoint...");
  if (checkpoint_manager_) {
    checkpoint_manager_->CreateCheckpoint();
  }

  // flush all the logs into disk
  NJUDB_LOG("Flushing logs...");
  is_running_ = false;
  if (log_manager_ != nullptr) {
    log_manager_->FlushLog();
  }
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
  // Initialize transaction with valid ID and register with TxnManager
  auto txn_id = txn_manager_->GetNextTxnId();
  txn_manager_->Begin(txn_id, IsolationLevel::READ_COMMITTED, ConcurrencyProtocol::TWO_PHASE_LOCKING);
  auto   *txn = txn_manager_->GetTransaction(txn_id);
  Context context(txn,
      log_manager_.get(),
      nullptr,
      net_controller_.get(),
      client_fd,
      lock_manager_.get(),
      version_manager_.get(),
      timestamp_oracle_.get());
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
      txn_manager_->SetTransaction(txn);
      auto gm_tree = parser_->Parse(sql);

      // PostgreSQL-style error handling: Check if transaction is in aborted state
      // If an error occurred in an explicit transaction, all commands are rejected
      // until user issues ROLLBACK or connection closes
      if (txn != nullptr && txn->GetState() == TxnState::ABORTED) {
        // Allow only ROLLBACK or COMMIT (which will act as ROLLBACK) to clear state
        auto is_txn_end_command = std::dynamic_pointer_cast<ast::TxnAbort>(gm_tree) ||
                                   std::dynamic_pointer_cast<ast::TxnCommit>(gm_tree);
        
        if (!is_txn_end_command) {
          net_controller_->SendError(client_fd, 
            "ERROR: current transaction is aborted, commands ignored until end of transaction block");
          continue;  // Skip this command, wait for ROLLBACK/COMMIT
        }
        // If it's ROLLBACK or COMMIT, let it proceed to HandleTxnCommand
      }

      // Handle transaction-related commands directly (before planning)
      if (HandleTxnCommand(gm_tree, &context, txn, txn_id, client_fd)) {
        continue;  // Transaction command handled
      }

      auto plan = planner_->PlanAST(gm_tree, context.db_);
      if (plan == nullptr || DoDBPlan(plan, &context, txn, txn_id) || DoExplainPlan(plan, &context)) {
        net_controller_->SendOK(client_fd);
      } else {
        /// plan is not a db plan
        plan           = optimizer_->Optimize(plan, context.db_);
        auto exec_tree = executor_->Translate(plan, context.db_, &context);
        executor_->Execute(exec_tree, &context);
      }
      // commit transaction if this is a single sql statement
      if (!txn->IsExplicit()) {
        txn_manager_->Commit(txn->GetTxnId());
        // Start a new transaction for the next statement
        txn_id = txn_manager_->GetNextTxnId();
        const auto protocol = context.db_ == nullptr ? context.database_creation_protocol_
                                                     : context.db_->GetConcurrencyProtocol();
        txn_manager_->Begin(txn_id, IsolationLevel::READ_COMMITTED, protocol);
        txn         = txn_manager_->GetTransaction(txn_id);
        context.txn = txn;
      }
    } catch (NJUDBException_ &e) {
      if (e.type_ == NJUDB_CLIENT_DOWN) {
        NJUDB_LOG(fmt::format("Client {} disconnected", client_fd));
        // CRITICAL FIX: Abort any active transaction when client disconnects
        // This ensures ACID atomicity - uncommitted transactions must be rolled back
        // No need to start a new transaction since the client handler is exiting
        if (txn != nullptr && context.db_ != nullptr) {
          try {
            AbortTransaction(txn, context.db_);
            txn = nullptr;  // CRITICAL: Nullify immediately after abort
          } catch (const std::exception &abort_error) {
            NJUDB_LOG_ERROR(fmt::format("Error aborting transaction on client disconnect: {}", abort_error.what()));
            txn = nullptr;  // Still nullify to prevent use-after-free
            // Continue to ensure client disconnection is handled
          }
        }
        break;
      } else if (e.type_ == NJUDB_TXN_ABORTED) {
        // Transaction was aborted (e.g., due to deadlock detection)
        // Need to start a new transaction since the client continues executing
        if (txn != nullptr && context.db_ != nullptr) {
          try {
            AbortTransactionAndStartNew(txn, txn_id, &context);
          } catch (const std::exception &abort_error) {
            NJUDB_LOG_ERROR(fmt::format("Error during abort and restart: {}", abort_error.what()));
            // Try to at least start a new transaction
            txn_id = txn_manager_->GetNextTxnId();
            const auto protocol = context.db_ == nullptr ? context.database_creation_protocol_
                                                         : context.db_->GetConcurrencyProtocol();
            txn_manager_->Begin(txn_id, IsolationLevel::READ_COMMITTED, protocol);
            txn         = txn_manager_->GetTransaction(txn_id);
            context.txn = txn;
          }
        }
        net_controller_->SendError(client_fd, "Transaction aborted");
      } else {
        NJUDB_LOG_ERROR(e.what());
        net_controller_->SendError(client_fd, e.short_what());

        // Real-world DBMS behavior for exceptions in transactions:
        // - PostgreSQL: Mark transaction as aborted, all subsequent commands fail
        // - MySQL/Oracle: Statement-level rollback, transaction continues
        // We implement PostgreSQL-style for safety and consistency
        
        if (txn != nullptr && context.db_ != nullptr) {
          if (!txn->IsExplicit()) {
            // AUTOCOMMIT MODE: Abort transaction and start new one
            // This matches all major DBMSs in autocommit mode
            try {
              AbortTransaction(txn, context.db_);
              txn = nullptr;

              // Start a new transaction for next statement
              txn_id = txn_manager_->GetNextTxnId();
              const auto protocol = context.db_ == nullptr ? context.database_creation_protocol_
                                                           : context.db_->GetConcurrencyProtocol();
              txn_manager_->Begin(txn_id, IsolationLevel::READ_COMMITTED, protocol);
              txn         = txn_manager_->GetTransaction(txn_id);
              context.txn = txn;
            } catch (const std::exception &abort_error) {
              NJUDB_LOG_ERROR(fmt::format("Error aborting failed transaction: {}", abort_error.what()));
              txn         = nullptr;
              context.txn = nullptr;
            }
          } else {
            // EXPLICIT TRANSACTION: Mark as aborted but don't roll back yet
            // This matches PostgreSQL behavior:
            // - Transaction enters "aborted" state
            // - All subsequent commands fail immediately
            // - User must explicitly ROLLBACK or let connection close auto-rollback
            txn->SetState(TxnState::ABORTED);
            net_controller_->SendError(client_fd, 
              "ERROR: current transaction is aborted, commands ignored until end of transaction block");
          }
        }
      }
    }
  }  // end of client while loop

  // CRITICAL FIX: Abort any uncommitted transaction on exit
  // Check for both GROWING and SHIRNKING states
  if (txn != nullptr && txn->IsExplicit() && context.db_ != nullptr &&
      (txn->GetState() == TxnState::GROWING || txn->GetState() == TxnState::SHIRNKING)) {
    NJUDB_LOG(
        fmt::format("Client {} exiting with uncommitted transaction, aborting txn {}", client_fd, txn->GetTxnId()));
    try {
      AbortTransaction(txn, context.db_);
      // CRITICAL: txn pointer is now INVALID after AbortTransaction (erased from tid_to_ts_)
      txn = nullptr;  // Prevent dangling pointer usage
    } catch (const std::exception &abort_error) {
      NJUDB_LOG_ERROR(fmt::format("Error aborting transaction on exit: {}", abort_error.what()));
      txn = nullptr;  // Still nullify to prevent use-after-free
    }
  }

  net_controller_->Remove(client_fd);

  // CRITICAL FIX: Flush all logs before closing database to ensure durability
  // This prevents race condition where database closes before log flush completes
  if (context.db_ != nullptr) {
    if (log_manager_ != nullptr) {
      log_manager_->FlushLog();
    }
    context.db_->Release();
  }
}

bool SystemManager::DoDBPlan(
    const std::shared_ptr<AbstractPlan> &plan, Context *ctx, Transaction *&txn, txn_id_t &txn_id)
{
  if (const auto cdb = std::dynamic_pointer_cast<CreateDBPlan>(plan)) {
    if (cdb->db_name_ == TMP_DIR) {
      NJUDB_THROW(NJUDB_INVALID_SQL, fmt::format("invalid db name: {}", cdb->db_name_));
    }
    if (databases_.find(cdb->db_name_) == databases_.end()) {
      CreateDatabase(cdb->db_name_, ctx->database_creation_protocol_);
    } else {
      NJUDB_THROW(NJUDB_DB_EXISTS, fmt::format("{}", cdb->db_name_));
    }
    return true;
  } else if (const auto odb = std::dynamic_pointer_cast<OpenDBPlan>(plan)) {
    if (databases_.find(odb->db_name_) == databases_.end()) {
      NJUDB_THROW(NJUDB_DB_MISS, fmt::format("{}", odb->db_name_));
    } else {
      auto *target_db = databases_[odb->db_name_].get();
      if (ctx->db_ == target_db) {
        return true;
      }
      if (txn != nullptr && txn->IsExplicit()) {
        NJUDB_THROW(NJUDB_INVALID_SQL, "Cannot open a database inside an explicit transaction");
      }
      if (ctx->db_ != nullptr) {
        ctx->db_->Release();
      }
      ctx->db_ = target_db;
      ctx->db_->Acquire();
      RestartTransactionForDatabase(txn, txn_id, ctx);
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

void SystemManager::AbortTransaction(Transaction *txn, DatabaseHandle *db_handle)
{
  assert(txn != nullptr && db_handle != nullptr);

  // TxnManager::Abort() handles all rollback logic (including CLR logging for 2PL)
  // No need to call RollbackTransaction() here - it would be a duplicate!
  txn_manager_->Abort(txn->GetTxnId(), db_handle);
}

void SystemManager::AbortTransactionAndStartNew(Transaction *&txn, txn_id_t &txn_id, Context *ctx)
{
  assert(txn != nullptr && ctx != nullptr && ctx->db_ != nullptr);
  AbortTransaction(txn, ctx->db_);

  // Start a new transaction (client continues executing)
  txn_id = txn_manager_->GetNextTxnId();
  txn_manager_->Begin(txn_id, IsolationLevel::READ_COMMITTED, ctx->db_->GetConcurrencyProtocol());
  txn = txn_manager_->GetTransaction(txn_id);

  // CRITICAL FIX: Validate new transaction was created successfully
  if (txn == nullptr) {
    NJUDB_LOG_ERROR(fmt::format("Failed to create new transaction after abort (txn_id {})", txn_id));
    NJUDB_THROW(NJUDB_TXN_ABORTED, "Transaction creation failed after abort");
  }

  ctx->txn = txn;
}

void SystemManager::RestartTransactionForDatabase(Transaction *&txn, txn_id_t &txn_id, Context *ctx)
{
  assert(ctx != nullptr && ctx->db_ != nullptr);
  const auto protocol = ctx->db_->GetConcurrencyProtocol();
  if (txn != nullptr) {
    txn_manager_->Commit(txn->GetTxnId());
  }
  txn_id = txn_manager_->GetNextTxnId();
  txn_manager_->Begin(txn_id, IsolationLevel::READ_COMMITTED, protocol);
  txn = txn_manager_->GetTransaction(txn_id);
  ctx->txn = txn;
}

bool SystemManager::HandleTxnCommand(
    const std::shared_ptr<ast::TreeNode> &ast, Context *ctx, Transaction *&txn, txn_id_t &txn_id, int client_fd)
{
  // Handle BEGIN
  if (const auto txn_begin = std::dynamic_pointer_cast<ast::TxnBegin>(ast)) {
    if (txn != nullptr && txn->IsExplicit()) {
      net_controller_->SendError(client_fd, "Transaction already in progress");
      return true;
    }
    // Mark transaction as explicit (user-initiated)
    if (txn != nullptr) {
      txn->SetExplicit(true);
    }
    net_controller_->SendOK(client_fd);
    return true;
  }

  // Handle COMMIT
  if (const auto txn_commit = std::dynamic_pointer_cast<ast::TxnCommit>(ast)) {
    if (txn == nullptr) {
      net_controller_->SendError(client_fd, "No active transaction to commit");
      return true;
    }
    if (!txn->IsExplicit()) {
      net_controller_->SendError(client_fd, "Cannot commit auto-commit transaction explicitly");
      return true;
    }

    // PostgreSQL-style: If transaction is ABORTED due to error, COMMIT acts as ROLLBACK
    if (txn->GetState() == TxnState::ABORTED) {
      NJUDB_LOG(fmt::format("Transaction {} is in ABORTED state, COMMIT acts as ROLLBACK", txn_id));
      
      // Perform rollback instead of commit
      AbortTransactionAndStartNew(txn, txn_id, ctx);
      
      net_controller_->SendError(client_fd, "ROLLBACK (transaction was aborted due to previous error)");
      return true;
    }

    // Normal commit path for successful transactions
    txn_manager_->Commit(txn->GetTxnId());

    // Start a new transaction
    txn_id = txn_manager_->GetNextTxnId();
    const auto protocol =
        ctx->db_ == nullptr ? ctx->database_creation_protocol_ : ctx->db_->GetConcurrencyProtocol();
    txn_manager_->Begin(txn_id, IsolationLevel::READ_COMMITTED, protocol);
    txn      = txn_manager_->GetTransaction(txn_id);
    ctx->txn = txn;

    net_controller_->SendOK(client_fd);
    return true;
  }

  // Handle ABORT
  if (const auto txn_abort = std::dynamic_pointer_cast<ast::TxnAbort>(ast)) {
    if (txn == nullptr) {
      net_controller_->SendError(client_fd, "No active transaction to abort");
      return true;
    }
    if (!txn->IsExplicit()) {
      net_controller_->SendError(client_fd, "Cannot abort auto-commit transaction explicitly");
      return true;
    }

    // Abort the transaction and start a new one
    AbortTransactionAndStartNew(txn, txn_id, ctx);

    net_controller_->SendOK(client_fd);
    return true;
  }

  // Handle SET CONCURRENCY_PROTOCOL
  if (const auto set_protocol = std::dynamic_pointer_cast<ast::SetConcurrencyProtocol>(ast)) {
#if !NJUDB_ENABLE_LAB05_MVCC
    if (set_protocol->protocol_ == ConcurrencyProtocol::MVCC) {
      net_controller_->SendError(
          client_fd, "MVCC bonus is disabled; rebuild with -DENABLE_LAB05_MVCC=ON");
      return true;
    }
#endif
    if (ctx->db_ != nullptr) {
      net_controller_->SendError(
          client_fd, "Concurrency protocol is immutable after a database has been created or opened");
      return true;
    }
    if (txn != nullptr && txn->IsExplicit()) {
      net_controller_->SendError(client_fd, "Cannot select a database protocol inside an explicit transaction");
      return true;
    }
    ctx->database_creation_protocol_ = set_protocol->protocol_;
    net_controller_->SendOK(client_fd);
    return true;
  }
  
  // Handle CHECKPOINT
  if (const auto checkpoint_cmd = std::dynamic_pointer_cast<ast::LogStaticCheckpoint>(ast)) {
    try {
      if (checkpoint_manager_ != nullptr) {
        checkpoint_manager_->CreateCheckpoint();
        net_controller_->SendRawString(client_fd, "Checkpoint created successfully\n");
        net_controller_->SendOK(client_fd);
      } else {
        net_controller_->SendError(client_fd, "Checkpoint manager not available");
      }
    } catch (const std::exception &e) {
      net_controller_->SendError(client_fd, 
          fmt::format("Failed to create checkpoint: {}", e.what()));
    }
    return true;
  }

  return false;  // Not a transaction command
}

}  // namespace njudb
