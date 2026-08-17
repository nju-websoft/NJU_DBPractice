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
// Created for Lab05 Phase 4.1 - Version Chain Infrastructure
//

#ifndef NJUDB_VERSION_TABLE_H
#define NJUDB_VERSION_TABLE_H

#include <unordered_map>
#include <shared_mutex>
#include <vector>
#include <memory>
#include "concurrency/version_chain.h"
#include "common/rid.h"

namespace njudb {

struct VersionKey {
  table_id_t table_id{INVALID_TABLE_ID};
  RID rid;

  auto operator==(const VersionKey &other) const -> bool {
    return table_id == other.table_id && rid == other.rid;
  }
};

struct VersionKeyHash {
  auto operator()(const VersionKey &key) const -> size_t {
    return std::hash<table_id_t>()(key.table_id) ^ (std::hash<RID>()(key.rid) << 1U);
  }
};

using VersionChainHandle = std::shared_ptr<VersionChain>;

/**
 * @brief Global table mapping table/RID resource keys to version chains
 * 
 * Maintains version chains for all tuples known to the shared version manager.
 * A shared handle keeps a chain alive after it is removed from this lookup map.
 */
class VersionTable {
public:
  VersionTable() = default;
  ~VersionTable() = default;
  
  // Disable copy and move
  VersionTable(const VersionTable&) = delete;
  VersionTable& operator=(const VersionTable&) = delete;
  VersionTable(VersionTable&&) = delete;
  VersionTable& operator=(VersionTable&&) = delete;
  
  /**
   * @brief Get or create version chain for a tuple
   * @param key Table and record identifier
   * @return Shared version-chain handle
   */
  auto GetOrCreateVersionChain(const VersionKey &key) -> VersionChainHandle;
  auto GetOrCreateVersionChain(const RID &rid) -> VersionChainHandle {
    return GetOrCreateVersionChain({INVALID_TABLE_ID, rid});
  }
  auto GetVersionChain(const RID &rid) -> VersionChainHandle { return GetOrCreateVersionChain(rid); }

  auto FindVersionChain(const VersionKey &key) const -> VersionChainHandle;
  auto FindVersionChain(const RID &rid) const -> VersionChainHandle {
    return FindVersionChain({INVALID_TABLE_ID, rid});
  }
  
  /**
   * @brief Check if RID has a version chain
   * @param rid Record identifier
   * @return True if version chain exists
   */
  auto HasVersionChain(const VersionKey &key) const -> bool;
  auto HasVersionChain(const RID &rid) const -> bool { return HasVersionChain({INVALID_TABLE_ID, rid}); }
  
  /**
   * @brief Remove version chain for a tuple
   * @param rid Record identifier
   */
  void RemoveVersionChain(const VersionKey &key);
  void RemoveVersionChain(const RID &rid) { RemoveVersionChain({INVALID_TABLE_ID, rid}); }
  
  /**
   * @brief Get all RIDs with version chains
   * @return Vector of RIDs
   */
  auto GetAllKeys() const -> std::vector<VersionKey>;
  auto GetAllRIDs() const -> std::vector<RID>;
  
  /**
   * @brief Statistics about version table
   */
  struct Stats {
    size_t total_chains;
    size_t total_versions;
    size_t chains_needing_gc;
  };
  
  /**
   * @brief Get statistics about version table
   * @return Statistics structure
   */
  auto GetStats() const -> Stats;

private:
  /** Map from table/RID resource key to version chain */
  std::unordered_map<VersionKey, VersionChainHandle, VersionKeyHash> version_map_;
  
  /** Latch protecting version map */
  mutable std::shared_mutex latch_;
};

}  // namespace njudb

#endif  // NJUDB_VERSION_TABLE_H
