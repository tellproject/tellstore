#include <logstructured/OpenAddressingHash.hpp>

#include <gtest/gtest.h>

using namespace tell::store::logstructured;

namespace {

class OpenAddressingTableTest : public ::testing::Test {
protected:
    OpenAddressingTableTest()
            : mTable(1024),
              mElement1(0x1u), mElement2(0x2u), mElement3(0x3u) {
    }

    OpenAddressingTable mTable;

    uint64_t mElement1;
    uint64_t mElement2;
    uint64_t mElement3;
};

/**
 * @class OpenAddressingTable
 * @test Check if a simple get after insert returns the element
 */
TEST_F(OpenAddressingTableTest, insertAndGet) {
    EXPECT_TRUE(mTable.insert(10u, 11u, &mElement1));

    EXPECT_EQ(&mElement1, mTable.get(10u, 11u));
}

/**
 * @class OpenAddressingTable
 * @test Check if multiple get and inserts return the correct elements
 */
TEST_F(OpenAddressingTableTest, insertAndGetMultiple) {
    EXPECT_TRUE(mTable.insert(10u, 11u, &mElement1));
    EXPECT_TRUE(mTable.insert(10u, 12u, &mElement2));
    EXPECT_TRUE(mTable.insert(11u, 11u, &mElement3));

    EXPECT_EQ(&mElement1, mTable.get(10u, 11u));
    EXPECT_EQ(&mElement2, mTable.get(10u, 12u));
    EXPECT_EQ(&mElement3, mTable.get(11u, 11u));
}

/**
 * @class OpenAddressingTable
 * @test Check if inserting a duplicate fails
 */
TEST_F(OpenAddressingTableTest, insertDuplicate) {
    EXPECT_TRUE(mTable.insert(10u, 11u, &mElement1));
    EXPECT_FALSE(mTable.insert(10u, 11u, &mElement2));

    EXPECT_EQ(&mElement1, mTable.get(10u, 11u));
}

/**
 * @class OpenAddressingTable
 * @test Check if erasing an element works correctly
 */
TEST_F(OpenAddressingTableTest, erase) {
    EXPECT_TRUE(mTable.insert(10u, 11u, &mElement1));
    EXPECT_EQ(&mElement1, mTable.get(10u, 11u));

    EXPECT_TRUE(mTable.erase(10u, 11u, &mElement1));
    EXPECT_EQ(nullptr, mTable.get(10u, 11u));
}

/**
 * @class OpenAddressingTable
 * @test Check if erasing a non existing element works
 */
TEST_F(OpenAddressingTableTest, eraseNonExisting) {
    EXPECT_EQ(nullptr, mTable.get(10u, 11u));

    EXPECT_TRUE(mTable.erase(10u, 11u, &mElement1));
    EXPECT_EQ(nullptr, mTable.get(10u, 11u));
}

/**
 * @class OpenAddressingTable
 * @test Check if erasing a changed element is prevented
 */
TEST_F(OpenAddressingTableTest, eraseChanged) {
    EXPECT_TRUE(mTable.insert(10u, 11u, &mElement1));

    EXPECT_FALSE(mTable.erase(10u, 11u, &mElement2));
    EXPECT_EQ(&mElement1, mTable.get(10u, 11u));
}

/**
 * @class OpenAddressingTable
 * @test Check if updating an element works
 */
TEST_F(OpenAddressingTableTest, update) {
    EXPECT_TRUE(mTable.insert(10u, 11u, &mElement1));

    EXPECT_TRUE(mTable.update(10u, 11u, &mElement1, &mElement2));
    EXPECT_EQ(&mElement2, mTable.get(10u, 11u));
}

/**
 * @class OpenAddressingTable
 * @test Check if updating a changed element is prevented
 */
TEST_F(OpenAddressingTableTest, updateChanged) {
    EXPECT_TRUE(mTable.insert(10u, 11u, &mElement1));

    EXPECT_FALSE(mTable.update(10u, 11u, &mElement3, &mElement2));
    EXPECT_EQ(&mElement1, mTable.get(10u, 11u));
}

}
