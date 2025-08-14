//
// Created by ziqi on 2024/7/18.
//
#include "gtest/gtest.h"
#include "../common/error.h"

TEST(HelloTest, Hello)
{
  EXPECT_EQ(1, 1);
  WSDB_ASSERT(1 == 1, "1 == 1");
  WSDB_THROW(wsdb::WSDB_FILE_EXISTS, "");
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}