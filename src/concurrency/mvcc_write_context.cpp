#include "concurrency/mvcc_write_context.h"

namespace njudb {

thread_local Transaction *MVCCWriteContext::active_txn_ = nullptr;

MVCCWriteContext::Scope::Scope(Transaction *txn) : previous_(active_txn_)
{
  active_txn_ = txn;
}

MVCCWriteContext::Scope::~Scope()
{
  active_txn_ = previous_;
}

auto MVCCWriteContext::IsAuthorized(Transaction *txn) -> bool
{
  return txn != nullptr && active_txn_ == txn;
}

}  // namespace njudb
