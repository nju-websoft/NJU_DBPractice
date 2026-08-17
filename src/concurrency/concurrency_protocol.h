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
// Created for Lab05 - Dual Protocol Architecture
//

#ifndef NJUDB_CONCURRENCY_PROTOCOL_H
#define NJUDB_CONCURRENCY_PROTOCOL_H

#include <string>

namespace njudb {

/**
 * @brief Concurrency control protocols supported by NJUDB
 *
 * TWO_PHASE_LOCKING: Traditional 2PL with lock-based concurrency control
 * MVCC: Multi-version concurrency control for high read concurrency
 */
enum class ConcurrencyProtocol
{
  TWO_PHASE_LOCKING = 0,  // 2PL with lock-based concurrency control
  MVCC              = 1   // Multi-version concurrency control
};

/**
 * @brief Isolation levels (applicable to both 2PL and MVCC)
 *
 * These isolation levels have different semantics for each protocol:
 *
 * 2PL:
 *  - READ_UNCOMMITTED: No read locks
 *  - READ_COMMITTED: Release read locks early
 *  - REPEATABLE_READ: Hold read locks until commit
 *  - SERIALIZABLE: Full serializability with predicate locks
 *
 * MVCC:
 *  - READ_UNCOMMITTED: Read latest version (rare)
 *  - READ_COMMITTED: Read latest committed version
 *  - REPEATABLE_READ: Consistent snapshot (snapshot isolation)
 *  - SERIALIZABLE: SSI with validation
 */
enum class IsolationLevel
{
  READ_UNCOMMITTED = 0,
  READ_COMMITTED   = 1,
  REPEATABLE_READ  = 2,
  SERIALIZABLE     = 3
};

/**
 * @brief Convert protocol to string for logging/debugging
 */
inline auto ProtocolToString(ConcurrencyProtocol protocol) -> std::string
{
  switch (protocol) {
    case ConcurrencyProtocol::TWO_PHASE_LOCKING: return "2PL";
    case ConcurrencyProtocol::MVCC: return "MVCC";
  }
  return "UNKNOWN";
}

/**
 * @brief Convert isolation level to string
 */
inline auto IsolationLevelToString(IsolationLevel level) -> std::string
{
  switch (level) {
    case IsolationLevel::READ_UNCOMMITTED: return "READ_UNCOMMITTED";
    case IsolationLevel::READ_COMMITTED: return "READ_COMMITTED";
    case IsolationLevel::REPEATABLE_READ: return "REPEATABLE_READ";
    case IsolationLevel::SERIALIZABLE: return "SERIALIZABLE";
  }
  return "UNKNOWN";
}

}  // namespace njudb

#endif  // NJUDB_CONCURRENCY_PROTOCOL_H
