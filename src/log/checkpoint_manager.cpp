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
// Lab06 Phase 3: Checkpoint Manager Implementation
//

#include "checkpoint_manager.h"
#include "recovery.h"  // For FilePageHash
#include "concurrency/txn_manager.h"
#include "concurrency/version_manager.h"
#include "storage/buffer/buffer_pool_manager.h"
#include "../../../common/error.h"
#include <cstring>

namespace njudb {

void Checkpoint::SerializeTo(char *buffer) const {
  NJUDB_STUDENT_TODO(l6, t4);
}

auto Checkpoint::DeserializeFrom(const char *buffer, size_t buffer_size) -> Checkpoint {
  NJUDB_STUDENT_TODO(l6, t4);
}

auto Checkpoint::GetSize() const -> size_t {
  NJUDB_STUDENT_TODO(l6, t4);
}

void CheckpointManager::CreateCheckpoint() {
  NJUDB_STUDENT_TODO(l6, t4);
}

auto CheckpointManager::CollectATT() -> std::vector<Checkpoint::ATTEntry> {
  NJUDB_STUDENT_TODO(l6, t4);
}

auto CheckpointManager::CollectDPT() -> std::unordered_map<std::pair<std::string, page_id_t>, lsn_t, FilePageHash> {
  NJUDB_STUDENT_TODO(l6, t4);
}

void CheckpointManager::StartPeriodicCheckpointing(uint32_t interval_seconds) {
  // Stop existing thread if running
  StopPeriodicCheckpointing();
  
  checkpoint_interval_seconds_ = interval_seconds;
  stop_flag_.store(false);
  
  // Start background thread for periodic checkpointing
  checkpoint_thread_ = std::thread([this]() {
    while (!stop_flag_.load()) {
      // Sleep for the interval duration, checking stop_flag periodically
      auto start_time = std::chrono::steady_clock::now();
      auto target_time = start_time + std::chrono::seconds(checkpoint_interval_seconds_);
      
      while (std::chrono::steady_clock::now() < target_time) {
        if (stop_flag_.load()) {
          return;  // Exit thread if stop requested
        }
        // Check every second to allow responsive shutdown
        std::this_thread::sleep_for(std::chrono::seconds(1));
      }
      
      // Create checkpoint if not stopped
      if (!stop_flag_.load()) {
        try {
          CreateCheckpoint();
        } catch (const std::exception &e) {
          // Log error but continue periodic checkpointing
          NJUDB_LOG_ERROR(fmt::format("Periodic checkpoint failed: {}", e.what()));
        }
      }
    }
  });
}

void CheckpointManager::StopPeriodicCheckpointing() {
  if (checkpoint_thread_.joinable()) {
    stop_flag_.store(true);
    checkpoint_thread_.join();
  }
}

}  // namespace njudb
