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
// Created by ziqi on 2024/7/17.
//

#ifndef NJU_DBCOURSE_REPLACER_H
#define NJU_DBCOURSE_REPLACER_H

#include "common/types.h"

namespace njudb {

/**
 * Replacer is an abstract class that tracks page usage.
 */
class Replacer
{
public:
  Replacer()          = default;
  virtual ~Replacer() = default;

  /**
   * Remove the victim frame as defined by the replacement policy.
   * @param[out] frame_id id of frame that was removed, nullptr if no victim was found
   * @return true if a victim frame was found, false otherwise
   */
  virtual auto Victim(frame_id_t *frame_id) -> bool = 0;

  /**
   * Pins a frame, indicating that it should not be victimized until it is unpinned.
   * @param frame_id the id of the frame to pin
   */
  virtual void Pin(frame_id_t frame_id) = 0;

  /**
   * Unpins a frame, indicating that it can now be victimized.
   * @param frame_id the id of the frame to unpin
   */
  virtual void Unpin(frame_id_t frame_id) = 0;

  /** @return the number of elements in the replacer that can be victimized */
  virtual auto Size() -> size_t = 0;
};

}  // namespace njudb

#endif  // NJU_DBCOURSE_REPLACER_H
