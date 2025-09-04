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

#ifndef NJU_DBCOURSE_DISK_MANAGER_H
#define NJU_DBCOURSE_DISK_MANAGER_H

#include <iostream>
#include <fstream>
#include <future>
#include <unordered_map>
#include "common/types.h"

namespace njudb {
class DiskManager
{
public:
  DiskManager() = default;

  ~DiskManager() = default;

  /**
   * Create a file named file_name and close it immediately
   * @param fname
   */
  static void CreateFile(const std::string &fname);

  /**
   * Destroy file and should check that the file should not be opened,
   * if opened, should close and then destroy (unlink)
   * @param fname
   */
  static void DestroyFile(const std::string &fname);

  /**
   * Open the file named tab_name, add the opened file to the file map, and return the table id
   * If table does not exist, return -1
   * @param tab_name
   */
  auto OpenFile(const std::string &fname) -> file_id_t;

  /**
   * Close the file given table id, and remove related information from structures
   * @param tab_name
   */
  void CloseFile(file_id_t fid);

  void WritePage(file_id_t fid, page_id_t page_id, const char *data);

  void ReadPage(file_id_t fid, page_id_t page_id, char *data);

  void ReadFile(file_id_t fid, char *data, size_t size, size_t offset, int type);

  /**
   *
   * @param fid
   * @param data
   * @param size
   * @param pos ios::beg, ios::cur, ios::end
   */
  void WriteFile(file_id_t fid, const char *data, size_t size, int type, int off = 0);

  void WriteLog(const std::string &log_file, const std::string &log_string);

  void ReadLog(const std::string &log_file, std::string &log_string);

  auto GetFileId(const std::string &fname) -> file_id_t;

  auto GetFileName(file_id_t fid) -> std::string;

  static auto FileExists(const std::string &fname) -> bool;

private:
  std::unordered_map<std::string, file_id_t> name_fid_map_;
  std::unordered_map<file_id_t, std::string> fid_name_map_;
};

}  // namespace njudb

#endif  // NJU_DBCOURSE_DISK_MANAGER_H
