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

#ifndef NJUDB_LRU_REPLACER_H
#define NJUDB_LRU_REPLACER_H

#include <list>
#include <mutex>  // NOLINT
#include <vector>
#include <unordered_map>
#include "replacer.h"

namespace njudb {

/**
 * LRUReplacer implements the Least Recently Used replacement policy.
 */
class LRUReplacer : public Replacer
{
public:
  /**
   * Create a new LRUReplacer.
   */
  explicit LRUReplacer();

  /**
   * Destroys the LRUReplacer.
   */
  ~LRUReplacer() override = default;

  /**
   * Victimize a frame according to the LRU policy.
   * 1. grant the latch
   * 2. if there is no frame in the LRU list return false
   * 3. get the first (least recently used) evictable frame in the LRU list
   * 4. update the LRU list and hash map
   * @param frame_id
   * @return true if a victim frame was found, false otherwise
   */
  auto Victim(frame_id_t *frame_id) -> bool override;

  /**
   * Pin a frame, indicating that it should not be victimized until it is unpinned.
   * 1. grant the latch
   * 2. update the LRU list and hash map
   * @param frame_id
   */
  void Pin(frame_id_t frame_id) override;

  /**
   * Unpin a frame, indicating that it can now be victimized.
   * 1. grant the latch
   * 2. if the frame is already unpinned return
   * 3. add the frame to the LRU list and construct the LRU hash map
   * @param frame_id
   */
  void Unpin(frame_id_t frame_id) override;

  /**
   * Get the number of elements in the replacer that can be victimized.
   * 1. grant the latch
   * 2. return the number of evictable frames
   * @return the number of elements in the replacer that can be victimized
   */
  auto Size() -> size_t override;

private:
  /// Mutex
  std::mutex latch_;
  /// LRU list to store the frame id and the evictable status
  std::list<std::pair<frame_id_t, bool>> lru_list_;
  /// Hash map to store the frame id and the iterator in the LRU list
  std::unordered_map<frame_id_t, std::list<std::pair<frame_id_t, bool>>::iterator> lru_hash_;
  // number of evictable frames
  size_t cur_size_;
  // maximum number of frames
  size_t max_size_;
};

}  // namespace njudb

#endif  // NJUDB_LRU_REPLACER_H