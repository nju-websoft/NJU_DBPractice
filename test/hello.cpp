//
// Created by ziqi on 2024/7/18.
//
#include "gtest/gtest.h"

TEST(HelloTest, Hello) { EXPECT_EQ(1, 1); }

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}