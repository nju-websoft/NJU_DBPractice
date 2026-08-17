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

#ifndef NJUDB_META_H
#define NJUDB_META_H
#include <string>
#include <vector>
#include <memory>
#include "../../common/micro.h"
#include "bitmap.h"
#include "types.h"

struct FieldSchema;

// filed schema is the meta information of a field in a table
struct FieldSchema
{
  table_id_t  table_id_{INVALID_TABLE_ID};
  std::string field_name_{};
  size_t      field_size_{};
  FieldType   field_type_{};
  // maybe use in the future
  //  bool        is_primary_key_{};
  //  bool        is_unique_{};
  bool nullable_{true};
  auto operator==(const FieldSchema &rhs) const -> bool
  {
    return table_id_ == rhs.table_id_ && field_name_ == rhs.field_name_ && field_size_ == rhs.field_size_ &&
           field_type_ == rhs.field_type_ && nullable_ == rhs.nullable_;
  }

  auto ToString() const -> std::string
  {
    return fmt::format("#{}.{}:{}({}){}",
        table_id_ == INVALID_TABLE_ID ? "" : std::to_string(table_id_),
        field_name_,
        FieldTypeToString(field_type_),
        field_size_,
        nullable_ ? "" : "<NOT NULL>");
  }
};

// run time field description, used to record the field information in the query and executions
struct RTField
{
  FieldSchema field_{};
  // run time information
  std::string alias_{};
  bool        is_agg_{false};
  AggType     agg_type_{AggType::AGG_NONE};

  auto operator==(const RTField &rhs) const -> bool
  {
    return field_ == rhs.field_ && alias_ == rhs.alias_ && is_agg_ == rhs.is_agg_ && agg_type_ == rhs.agg_type_;
  }

  auto ToString() const -> std::string
  {
    return fmt::format("{}{}{}",
        field_.ToString(),
        alias_.empty() ? "" : fmt::format("\"{}\"", alias_),
        is_agg_ ? fmt::format("[{}]", AggTypeToString(agg_type_)) : "");
  }
};

/**
 * @brief Tuple metadata stored at the beginning of each tuple slot
 *
 * xmin/xmax and their flags are retained for recovery and diagnostics. They
 * are not authoritative for runtime MVCC visibility. Runtime reads must use
 * the table-aware VersionManager undo-chain resolver.
 * 
 * Layout:
 *   - xmin: Transaction ID that inserted this tuple (4 bytes)
 *   - xmax: Transaction ID that deleted/updated this tuple (4 bytes)
 *   - infomask: Status flags (2 bytes)
 *   - nullmap: Null bitmap for fields (variable size, aligned)
 * 
 * Total size: 10 bytes (fixed) + nullmap_size (variable) + padding to 8-byte alignment
 */
struct RecordHeader {
  txn_id_t xmin_;       // Creator transaction ID
  txn_id_t xmax_;       // Deleter/updater transaction ID (INVALID_TXN_ID if current)
  uint16_t infomask_;   // Status flags
  // Nullmap follows immediately after (variable size based on field count)
  
  // Infomask flags
  static constexpr uint16_t HEAP_XMIN_COMMITTED = 0x0001;  // xmin committed
  static constexpr uint16_t HEAP_XMAX_COMMITTED = 0x0002;  // xmax committed
  static constexpr uint16_t HEAP_XMIN_INVALID   = 0x0004;  // xmin aborted
  static constexpr uint16_t HEAP_XMAX_INVALID   = 0x0008;  // xmax aborted
  static constexpr uint16_t HEAP_HAS_VERSIONS    = 0x0010;  // Has version chain
  static constexpr uint16_t HEAP_UPDATED         = 0x0020;  // Tuple was updated
  static constexpr uint16_t HEAP_DELETED         = 0x0040;  // Tuple was deleted
  
  // Fixed header size (without nullmap)
  static constexpr size_t FIXED_SIZE = sizeof(txn_id_t) + sizeof(txn_id_t) + sizeof(uint16_t);
  
  /**
   * @brief Calculate total header size including nullmap and alignment
   * @param field_count Number of fields in the record
   * @return Total header size in bytes (aligned to 8 bytes)
   */
  static auto GetHeaderSize(size_t field_count) -> size_t {
    size_t nullmap_size = BITMAP_SIZE(field_count);
    size_t total_size = FIXED_SIZE + nullmap_size;
    // Align to 8-byte boundary for better performance
    return (total_size + 7) & ~7;
  }
  
  /**
   * @brief Initialize a new tuple header
   * @param header_ptr Pointer to header location
   * @param xmin Creator transaction ID
   * @param field_count Number of fields (for nullmap initialization)
   */
  static void Initialize(char *header_ptr, txn_id_t xmin, size_t field_count) {
    auto *hdr = reinterpret_cast<RecordHeader*>(header_ptr);
    hdr->xmin_ = xmin;
    hdr->xmax_ = INVALID_TXN_ID;
    hdr->infomask_ = 0;
    
    // Clear nullmap (all fields non-null by default)
    char *nullmap_ptr = header_ptr + FIXED_SIZE;
    memset(nullmap_ptr, 0, BITMAP_SIZE(field_count));
  }
  
  /**
   * @brief Get nullmap pointer from header
   * @param header_ptr Pointer to header location
   * @return Pointer to nullmap
   */
  static auto GetNullMapPtr(char *header_ptr) -> char* {
    return header_ptr + FIXED_SIZE;
  }
  
  static auto GetNullMapPtr(const char *header_ptr) -> const char* {
    return header_ptr + FIXED_SIZE;
  }
  
  /**
   * @brief Check if a field is null
   * @param header_ptr Pointer to header location
   * @param field_idx Field index
   * @return true if field is null
   */
  static auto IsFieldNull(const char *header_ptr, size_t field_idx) -> bool {
    const char *nullmap = GetNullMapPtr(header_ptr);
    return njudb::BitMap::GetBit(nullmap, field_idx);
  }
  
  /**
   * @brief Set field null status
   * @param header_ptr Pointer to header location
   * @param field_idx Field index
   * @param is_null true to mark as null
   */
  static void SetFieldNull(char *header_ptr, size_t field_idx, bool is_null) {
    char *nullmap = GetNullMapPtr(header_ptr);
    njudb::BitMap::SetBit(nullmap, field_idx, is_null);
  }
  
  /**
   * @brief Legacy tuple-header visibility approximation
   * @param header_ptr Pointer to header location
   * @param txn_id Transaction ID checking visibility
   * @return true if visible
   */
  static auto IsVisible(const char *header_ptr, txn_id_t txn_id) -> bool {
    auto *hdr = reinterpret_cast<const RecordHeader*>(header_ptr);
    
    // Tuple deleted by another transaction
    if (hdr->xmax_ != INVALID_TXN_ID && hdr->xmax_ != txn_id) {
      // Check if deleter committed
      if (hdr->infomask_ & HEAP_XMAX_COMMITTED) {
        return false;  // Deleted and committed
      }
      if (hdr->infomask_ & HEAP_XMAX_INVALID) {
        // Deleter aborted, tuple is visible
        return true;
      }
      // xmax is in progress, assume not visible for safety
      return false;
    }
    
    // Check if creator committed
    if (hdr->xmin_ == txn_id) {
      return true;  // Own insert
    }
    
    if (hdr->infomask_ & HEAP_XMIN_COMMITTED) {
      return true;  // Creator committed
    }
    
    if (hdr->infomask_ & HEAP_XMIN_INVALID) {
      return false;  // Creator aborted
    }
    
    // xmin in progress, not visible to others
    return false;
  }
  
  /**
   * @brief Mark transaction as committed
   * @param header_ptr Pointer to header location
   * @param is_xmin true to mark xmin, false to mark xmax
   */
  static void SetCommitted(char *header_ptr, bool is_xmin) {
    auto *hdr = reinterpret_cast<RecordHeader*>(header_ptr);
    if (is_xmin) {
      hdr->infomask_ |= HEAP_XMIN_COMMITTED;
    } else {
      hdr->infomask_ |= HEAP_XMAX_COMMITTED;
    }
  }
  
  /**
   * @brief Mark transaction as invalid (aborted)
   * @param header_ptr Pointer to header location
   * @param is_xmin true to mark xmin, false to mark xmax
   */
  static void SetInvalid(char *header_ptr, bool is_xmin) {
    auto *hdr = reinterpret_cast<RecordHeader*>(header_ptr);
    if (is_xmin) {
      hdr->infomask_ |= HEAP_XMIN_INVALID;
    } else {
      hdr->infomask_ |= HEAP_XMAX_INVALID;
    }
  }
};

/**
 * Table header is the first page of a table, it contains the meta information of the table
 */
struct TableHeader
{
  size_t    page_num_{0};
  page_id_t first_free_page_{INVALID_PAGE_ID};
  size_t    rec_num_{0};
  size_t    rec_size_{0};         // Pure data size (without header)
  size_t    rec_per_page_{0};
  size_t    field_num_{0};
  size_t    bitmap_size_{0};      // Slot occupancy bitmap size
  size_t    nullmap_size_{0};     // Field null bitmap size (per tuple)
  size_t    tuple_header_size_{0}; // Tuple header size including nullmap and alignment
};


#endif  // NJUDB_META_H
