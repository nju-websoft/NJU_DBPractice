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
// Created by ziqi on 2024/7/18.
//

#ifndef NJUDB_FRAME_H
#define NJUDB_FRAME_H

#include "common/types.h"
#include "common/config.h"
#include "common/page.h"
class Frame
{
public:
  Frame()  = default;
  ~Frame() = default;

  DISABLE_COPY_MOVE_AND_ASSIGN(Frame)

  [[nodiscard]] inline auto GetPage() -> Page * { return &page_; }

  [[nodiscard]] inline auto InUse() const -> bool { return pin_count_ > 0; }

  [[nodiscard]] inline auto IsDirty() const -> bool { return is_dirty_; }

  inline void SetDirty(bool dirty) { is_dirty_ = dirty; }

  [[nodiscard]] inline auto GetPinCount() const -> int { return pin_count_; }

  inline void Pin() { pin_count_++; }

  inline void Unpin()
  {
    NJUDB_ASSERT(pin_count_ > 0, "Unpin a frame with pin_count = 0");
    pin_count_--;
  }

  inline void Reset()
  {
    page_.Clear();
    is_dirty_  = false;
    pin_count_ = 0;
  }

private:
  Page page_{};
  bool is_dirty_{false};
  int  pin_count_{0};
};

#endif  // NJUDB_FRAME_H
