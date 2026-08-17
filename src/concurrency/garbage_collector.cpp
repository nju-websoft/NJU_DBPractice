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

#include "concurrency/garbage_collector.h"
#include "concurrency/version_manager.h"
#include "concurrency/timestamp_oracle.h"
#include <chrono>
#include <thread>

namespace njudb {

auto GarbageCollector::Start() -> void {
  // Check if already running
  if (running_.load()) {
    return;
  }
  
  // Set running flag
  running_.store(true);
  
  // Launch background thread
  gc_thread_ = std::thread(&GarbageCollector::RunGarbageCollection, this);
}

auto GarbageCollector::Stop() -> void {
  // Check if not running
  if (!running_.load()) {
    return;
  }
  
  // Signal thread to stop
  running_.store(false);
  
  // Wait for thread to finish
  if (gc_thread_.joinable()) {
    gc_thread_.join();
  }
}

auto GarbageCollector::CollectGarbage() -> void {
  NJUDB_STUDENT_TODO(l5_bonus, b4);
}

auto GarbageCollector::PruneTuple(table_id_t table_id, const RID &rid) -> size_t {
  NJUDB_STUDENT_TODO(l5_bonus, b4);
}

auto GarbageCollector::RunGarbageCollection() -> void {
  while (running_.load()) {
    // Sleep for configured interval
    std::this_thread::sleep_for(std::chrono::milliseconds(gc_interval_ms_));
    
    // Run garbage collection cycle
    CollectGarbage();
    
    // Increment cycle counter
    gc_cycles_++;
  }
}

}  // namespace njudb
