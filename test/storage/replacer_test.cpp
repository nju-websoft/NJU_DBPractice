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
// Created by ziqi on 2024/8/19.
//
#include "storage/buffer/replacer/lru_replacer.h"
#include "storage/buffer/replacer/lru_k_replacer.h"

#include "../config.h"
#include "common/types.h"

#include <cassert>
#include <unordered_map>
#include <vector>
#include <unordered_set>

#include "gtest/gtest.h"

TEST(ReplacerTest, LRU)
{
  std::vector<frame_id_t> frame_ids = {0, 1, 2, 3, 4, 5, 6, 7};
  auto                    replacer  = wsdb::LRUReplacer();
  SUB_TEST(Basic)
  {
    for (auto frame_id : frame_ids) {
      replacer.Pin(frame_id);
    }
    ASSERT_EQ(replacer.Size(), 0);
    for (auto frame_id : frame_ids) {
      replacer.Unpin(frame_id);
    }
    ASSERT_EQ(replacer.Size(), 8);
    frame_id_t frame_id;
    for (int i = 0; i < 8; ++i) {
      replacer.Victim(&frame_id);
      ASSERT_EQ(frame_id,  i);
    }
    ASSERT_EQ(replacer.Size(), 0);
  }

  // randomly pin and unpin frames
  SUB_TEST(RandomlyPinUnpin)
  {
    std::unordered_set<frame_id_t> pinned;
    for (int i = 0; i < 1000; ++i) {
      frame_id_t frame_id = rand() % 8;
      if (pinned.find(frame_id) == pinned.end()) {
        replacer.Pin(frame_id);
        pinned.insert(frame_id);
      } else {
        replacer.Unpin(frame_id);
        pinned.erase(frame_id);
      }
    }
    ASSERT_EQ(replacer.Size(), frame_ids.size() - pinned.size());
  }
  // unpin all pages
  {
    for (auto frame_id : frame_ids) {
      replacer.Unpin(frame_id);
    }
    ASSERT_EQ(replacer.Size(), 8);
  }

  SUB_TEST(VictimOrder)
  {
    // generate a random pin order
    std::vector<frame_id_t> pin_order;
    std::list<frame_id_t>    victim_order;
    pin_order.reserve(1000);
    for (int i = 0; i < 1000; ++i) {
      frame_id_t frame_id = i < 8 ? i : rand() % 8;
      pin_order.push_back(frame_id);
      if(std::find(victim_order.begin(), victim_order.end(), frame_id) == victim_order.end()){
        victim_order.push_back(frame_id);
      } else{
        victim_order.remove(frame_id);
        victim_order.push_back(frame_id);
      }
    }
    // unpin all
    for (auto frame_id : frame_ids) {
      replacer.Unpin(frame_id);
    }
    ASSERT_EQ(replacer.Size(), 8);
    // victimize frames and check if the order is correct
    for (auto frame_id : pin_order) {
      replacer.Pin(frame_id);
    }
    ASSERT_EQ(replacer.Size(), 0);
    for(int i = 0; i < 8; ++i){
      replacer.Unpin(i);
    }
    ASSERT_EQ(replacer.Size(), 8);
    for (auto frame_id : victim_order) {
      frame_id_t victim_frame_id;
      replacer.Victim(&victim_frame_id);
      ASSERT_EQ(victim_frame_id, frame_id);
    }
  }
}

TEST(ReplacerTest, LRUK)
{
  int k = 3;
  // size of frame_ids should be greater than k and be equal to BUFFER_POOL_SIZE
  std::vector<frame_id_t> frame_ids = {0, 1, 2, 3, 4, 5, 6, 7};
  auto                    replacer  = wsdb::LRUKReplacer(k);
  SUB_TEST(Basic)
  {
    for (auto frame_id : frame_ids) {
      replacer.Pin(frame_id);
    }
    ASSERT_EQ(replacer.Size(), 0);
    for (auto frame_id : frame_ids) {
      replacer.Unpin(frame_id);
    }
    ASSERT_EQ(replacer.Size(), 8);
  }
  SUB_TEST(VictimOrder)
  {  // store access history of each frame
    std::unordered_map<frame_id_t, std::list<timestamp_t>> access_history;
    for (auto frame_id : frame_ids) {
      access_history[frame_id] = std::list<timestamp_t>();
    }
    // pin frames, then check if the order of victim is correct
    for (int i = 0; i < 1000; ++i) {
      // make sure all history size is equal to k
      frame_id_t frame_id = i < k * 8 ? i % 8 : rand() % 8;
      replacer.Pin(frame_id);
      auto &history = access_history[frame_id];
      history.push_back(i);
      if (history.size() > static_cast<size_t>(k)) {
        history.pop_front();
      }
    }
    ASSERT_EQ(replacer.Size(), 0);
    for (auto frame_id : frame_ids) {
      replacer.Unpin(frame_id);
    }
    ASSERT_EQ(replacer.Size(), 8);
    // victimize frames
    for (int i = 0; i < 8; i++) {
      frame_id_t frame_id;
      replacer.Victim(&frame_id);
      frame_id_t expected_frame_id = INVALID_FRAME_ID;
      size_t        max_dist          = 0;
      // check if the frame id is the one with the largest backward k distance
      for (auto &[fid, history] : access_history) {
        if (history.size() < static_cast<size_t>(k)) {
          throw;
        }
        size_t dist = 1000 - history.front();
        if (dist > max_dist) {
          max_dist          = dist;
          expected_frame_id = fid;
        }
      }
      ASSERT_EQ(frame_id, expected_frame_id);
      // erase the frame from the access history
      access_history.erase(frame_id);
    }
    ASSERT_EQ(replacer.Size(), 0);
  }  // end of VictimOrder

  SUB_TEST(AccessOneFrame)
  {
    replacer.Pin(0);
    replacer.Pin(0);
    replacer.Pin(0);
    replacer.Pin(0);
    replacer.Pin(0);
    replacer.Pin(1);
    replacer.Pin(1);
    replacer.Pin(2);
    replacer.Pin(2);
    replacer.Pin(1);
    ASSERT_EQ(replacer.Size(), 0);
    replacer.Unpin(0);
    replacer.Unpin(1);
    replacer.Unpin(2);
    ASSERT_EQ(replacer.Size(), 3);
    frame_id_t frame_id;
    replacer.Victim(&frame_id);
    ASSERT_EQ(frame_id, 2);
    replacer.Pin(2);
    replacer.Unpin(2);
    replacer.Victim(&frame_id);
    ASSERT_EQ(frame_id, 2);
    replacer.Pin(3);
    replacer.Pin(3);
    replacer.Pin(3);
    replacer.Unpin(3);
    replacer.Victim(&frame_id);
    ASSERT_EQ(frame_id, 0);
    replacer.Victim(&frame_id);
    ASSERT_EQ(frame_id, 1);
    replacer.Victim(&frame_id);
    ASSERT_EQ(frame_id, 3);
    ASSERT_EQ(replacer.Size(), 0);
  }

  SUB_TEST(AccessInf){
    replacer.Pin(0);
    replacer.Pin(0);
    replacer.Pin(1);
    replacer.Pin(1);
    replacer.Pin(2);
    replacer.Pin(2);
    replacer.Pin(3);
    replacer.Unpin(0);
    replacer.Unpin(1);
    replacer.Unpin(2);
    replacer.Unpin(3);
    ASSERT_EQ(replacer.Size(), 4);
    frame_id_t frame_id;
    replacer.Victim(&frame_id);
    ASSERT_EQ(frame_id, 0);
    replacer.Pin(1);
    replacer.Unpin(1);
    replacer.Victim(&frame_id);
    ASSERT_EQ(frame_id, 2);
    replacer.Victim(&frame_id);
    ASSERT_EQ(frame_id, 3);
    replacer.Victim(&frame_id);
    ASSERT_EQ(frame_id, 1);
    ASSERT_EQ(replacer.Size(), 0);
  }
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
