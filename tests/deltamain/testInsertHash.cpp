/*
 * (C) Copyright 2015 ETH Zurich Systems Group (http://www.systems.ethz.ch/) and others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Contributors:
 *     Markus Pilman <mpilman@inf.ethz.ch>
 *     Simon Loesing <sloesing@inf.ethz.ch>
 *     Thomas Etter <etterth@gmail.com>
 *     Kevin Bocksrocker <kevin.bocksrocker@gmail.com>
 *     Lucas Braun <braunl@inf.ethz.ch>
 */

#include <deltamain/InsertHash.hpp>

#include <gtest/gtest.h>

using namespace tell::store;
using namespace tell::store::deltamain;

namespace {

class InsertTableTest : public ::testing::Test {
protected:
    InsertTableTest()
            : mTable(128),
              mElement1(0x1u), mElement2(0x2u), mElement3(0x3u) {
    }

    InsertTable mTable;

    uint64_t mElement1;
    uint64_t mElement2;
    uint64_t mElement3;
};

/**
 * @class InsertTable
 * @test Check if a simple get after insert returns the element
 */
TEST_F(InsertTableTest, insertAndGet) {
    EXPECT_TRUE(mTable.insert(10u, &mElement1));

    EXPECT_EQ(&mElement1, mTable.get(10u));
}

/**
 * @class InsertTable
 * @test Check if multiple get and inserts return the correct elements
 */
TEST_F(InsertTableTest, insertAndGetMultiple) {
    EXPECT_TRUE(mTable.insert(10u, &mElement1));
    EXPECT_TRUE(mTable.insert(11u, &mElement2));
    EXPECT_TRUE(mTable.insert(138u, &mElement3));

    EXPECT_EQ(&mElement1, mTable.get(10u));
    EXPECT_EQ(&mElement2, mTable.get(11u));
    EXPECT_EQ(&mElement3, mTable.get(138u));
}

/**
 * @class InsertTable
 * @test Check if inserting a duplicate fails
 */
TEST_F(InsertTableTest, insertDuplicate) {
    void* actualData = nullptr;
    EXPECT_TRUE(mTable.insert(10u, &mElement1, &actualData));
    EXPECT_EQ(nullptr, actualData);

    EXPECT_FALSE(mTable.insert(10u, &mElement2, &actualData));
    EXPECT_EQ(&mElement1, actualData);

    EXPECT_EQ(&mElement1, mTable.get(10u));
}

/**
 * @class InsertTable
 * @test Check if removing an element and inserting another one works correctly
 */
TEST_F(InsertTableTest, removeAndInsert) {
    EXPECT_TRUE(mTable.insert(10u, &mElement1));
    EXPECT_EQ(&mElement1, mTable.get(10u));

    EXPECT_TRUE(mTable.remove(10u, &mElement1));
    EXPECT_EQ(nullptr, mTable.get(10u));

    EXPECT_TRUE(mTable.insert(10u, &mElement2));
    EXPECT_EQ(&mElement2, mTable.get(10u));
}

/**
 * @class InsertTable
 * @test Check if removing a changed element is prevented
 */
TEST_F(InsertTableTest, removeChanged) {
    EXPECT_TRUE(mTable.insert(10u, &mElement1));

    void* actualData = nullptr;
    EXPECT_FALSE(mTable.remove(10u, &mElement2, &actualData));
    EXPECT_EQ(&mElement1, actualData);
    EXPECT_EQ(&mElement1, mTable.get(10u));
}

/**
 * @class InsertTable
 * @test Check if updating an element works
 */
TEST_F(InsertTableTest, update) {
    EXPECT_TRUE(mTable.insert(10u, &mElement1));

    EXPECT_TRUE(mTable.update(10u, &mElement1, &mElement2));
    EXPECT_EQ(&mElement2, mTable.get(10u));
}

/**
 * @class InsertTable
 * @test Check if updating a changed element is prevented
 */
TEST_F(InsertTableTest, updateChanged) {
    EXPECT_TRUE(mTable.insert(10u, &mElement1));

    void* actualData = nullptr;
    EXPECT_FALSE(mTable.update(10u, &mElement3, &mElement2, &actualData));
    EXPECT_EQ(&mElement1, actualData);
    EXPECT_EQ(&mElement1, mTable.get(10u));
}

}
