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
// Created by ziqi on 2024/7/19.
//

#ifndef WSDB_RID_H
#define WSDB_RID_H

#include "common/types.h"

namespace wsdb {

class RID
{
public:
  RID() : page_id_(INVALID_PAGE_ID), slot_id_(INVALID_SLOT_ID) {}
  RID(page_id_t page_id, slot_id_t slot_id) : page_id_(page_id), slot_id_(slot_id) {}
  RID(const RID &other)     = default;
  RID(RID &&other) noexcept = default;
  ~RID()                    = default;

  [[nodiscard]] page_id_t PageID() const { return page_id_; }
  [[nodiscard]] slot_id_t SlotID() const { return slot_id_; }

  auto operator=(const RID &other) -> RID & = default;

  auto operator=(RID &&other) noexcept -> RID & = default;

  auto operator==(const RID &other) const -> bool { return page_id_ == other.page_id_ && slot_id_ == other.slot_id_; }

  auto operator!=(const RID &other) const -> bool { return !(*this == other); }

  // Hash code for this RID
  [[nodiscard]] auto GetHash() const -> size_t { return page_id_ << 16 | slot_id_; }

private:
  page_id_t page_id_;
  slot_id_t slot_id_;
};

const RID INVALID_RID = RID(INVALID_PAGE_ID, INVALID_SLOT_ID);

}  // namespace wsdb

namespace std {
template <>
struct hash<wsdb::RID>
{
  auto operator()(const wsdb::RID &rid) const -> size_t { return rid.GetHash(); }
};
}  // namespace std

#endif  // WSDB_RID_H
