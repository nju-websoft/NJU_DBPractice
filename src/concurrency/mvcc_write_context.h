#ifndef NJUDB_MVCC_WRITE_CONTEXT_H
#define NJUDB_MVCC_WRITE_CONTEXT_H

namespace njudb {

class Transaction;

/**
 * Marks the narrow call scope in which the MVCC coordinator may mutate pages.
 *
 * TableHandle rejects direct MVCC transaction writes outside this scope so a
 * caller cannot bypass version creation and index coordination.
 */
class MVCCWriteContext
{
public:
  class Scope
  {
  public:
    explicit Scope(Transaction *txn);
    ~Scope();

    Scope(const Scope &) = delete;
    Scope &operator=(const Scope &) = delete;

  private:
    Transaction *previous_;
  };

  static auto IsAuthorized(Transaction *txn) -> bool;

private:
  static thread_local Transaction *active_txn_;
};

}  // namespace njudb

#endif  // NJUDB_MVCC_WRITE_CONTEXT_H
