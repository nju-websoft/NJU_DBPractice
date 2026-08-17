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

#include "concurrency/version_table.h"

namespace njudb {

auto VersionTable::GetOrCreateVersionChain(const VersionKey &key) -> VersionChainHandle {
  std::unique_lock<std::shared_mutex> lock(latch_);
  
  auto it = version_map_.find(key);
  if (it == version_map_.end()) {
    auto chain = std::make_shared<VersionChain>();
    version_map_[key] = chain;
    return chain;
  }
  
  return it->second;
}

auto VersionTable::FindVersionChain(const VersionKey &key) const -> VersionChainHandle {
  std::shared_lock<std::shared_mutex> lock(latch_);
  auto it = version_map_.find(key);
  return it == version_map_.end() ? nullptr : it->second;
}

auto VersionTable::HasVersionChain(const VersionKey &key) const -> bool {
  return FindVersionChain(key) != nullptr;
}

void VersionTable::RemoveVersionChain(const VersionKey &key) {
  std::unique_lock<std::shared_mutex> lock(latch_);
  version_map_.erase(key);
}

auto VersionTable::GetAllKeys() const -> std::vector<VersionKey> {
  std::shared_lock<std::shared_mutex> lock(latch_);
  
  std::vector<VersionKey> keys;
  keys.reserve(version_map_.size());
  
  for (const auto &entry : version_map_) {
    keys.push_back(entry.first);
  }
  
  return keys;
}

auto VersionTable::GetAllRIDs() const -> std::vector<RID> {
  auto keys = GetAllKeys();
  std::vector<RID> rids;
  rids.reserve(keys.size());
  for (const auto &key : keys) {
    rids.push_back(key.rid);
  }
  return rids;
}

auto VersionTable::GetStats() const -> Stats {
  std::shared_lock<std::shared_mutex> lock(latch_);
  
  Stats stats{0, 0, 0};
  stats.total_chains = version_map_.size();
  
  for (const auto &entry : version_map_) {
    stats.total_versions += entry.second->GetVersionCount();
    if (entry.second->NeedsGC()) {
      stats.chains_needing_gc++;
    }
  }
  
  return stats;
}

}  // namespace njudb
