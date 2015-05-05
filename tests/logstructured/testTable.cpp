#include <config.h>

#include <logstructured/Table.hpp>

#include <util/CommitManager.hpp>
#include <util/OpenAddressingHash.hpp>
#include <util/PageManager.hpp>
#include <util/Record.hpp>

#include <gtest/gtest.h>

#include <limits>
#include <utility>
#include <vector>

using namespace tell::store;
using namespace tell::store::logstructured;

namespace {

class TableTest : public ::testing::Test {
protected:
    TableTest()
            : mPageManager(TELL_PAGE_SIZE * 4),
              mHashMap(1024),
              mTable(mPageManager, mHashMap, mSchema, 1),
              mTx(mCommitManager.startTx()),
              mField("Test Field") {
    }

    PageManager mPageManager;
    Table::HashTable mHashMap;
    Schema mSchema;

    DummyManager mCommitManager;

    Table mTable;

    SnapshotDescriptor mTx;
    std::string mField;
};

/**
 * @class Table
 * @test Check if an insert followed by a get returns the inserted element
 */
TEST_F(TableTest, insertGet) {
    bool succeeded = false;
    mTable.insert(1, mField.size(), mField.c_str(), mTx, &succeeded);
    EXPECT_TRUE(succeeded);

    std::size_t size = 0;
    const char* ptr = nullptr;
    bool isNewest = false;
    EXPECT_TRUE(mTable.get(1, size, ptr, mTx, isNewest));
    EXPECT_EQ(mField, std::string(ptr, size));
    EXPECT_TRUE(isNewest);
}

/**
 * @class Table
 * @test Check if multiple inserts followed by gets return the inserted elements
 */
TEST_F(TableTest, insertGetMultiple) {
    std::vector<std::pair<uint64_t, std::string>> elements = {
        std::make_pair(1, "Test Field 1"),
        std::make_pair(2, "Test Field 2"),
        std::make_pair(3, "Test Field 3")
    };

    for (auto& e : elements) {
        bool succeeded = false;
        mTable.insert(e.first, e.second.size(), e.second.c_str(), mTx, &succeeded);
        EXPECT_TRUE(succeeded);
    }

    for (auto& e : elements) {
        std::size_t size = 0;
        const char* ptr = nullptr;
        bool isNewest = false;
        EXPECT_TRUE(mTable.get(e.first, size, ptr, mTx, isNewest));
        EXPECT_EQ(e.second, std::string(ptr, size));
        EXPECT_TRUE(isNewest);
    }
}

/**
 * @class Table
 * @test Check if an insert followed by an update returns the updated element
 */
TEST_F(TableTest, insertUpdateGet) {
    std::string fieldNew = "Test Field Update";

    bool succeeded = false;
    mTable.insert(1, mField.size(), mField.c_str(), mTx, &succeeded);
    EXPECT_TRUE(succeeded);

    EXPECT_TRUE(mTable.update(1, fieldNew.size(), fieldNew.c_str(), mTx));

    std::size_t size = 0;
    const char* ptr = nullptr;
    bool isNewest = false;
    EXPECT_TRUE(mTable.get(1, size, ptr, mTx, isNewest));
    EXPECT_EQ(fieldNew, std::string(ptr, size));
    EXPECT_TRUE(isNewest);
}

/**
 * @class Table
 * @test Check if an insert followed by a remove returns no element
 */
TEST_F(TableTest, insertRemoveGet) {
    bool succeeded = false;
    mTable.insert(1, mField.size(), mField.c_str(), mTx, &succeeded);
    EXPECT_TRUE(succeeded);

    EXPECT_TRUE(mTable.remove(1, mTx));

    std::size_t size = 0;
    const char* ptr = nullptr;
    bool isNewest = false;
    EXPECT_FALSE(mTable.get(1, size, ptr, mTx, isNewest));
    EXPECT_TRUE(isNewest);
}

/**
 * @class Table
 * @test Check if a removed element can be written again
 */
TEST_F(TableTest, insertRemoveInsertGet) {
    std::string field2 = "Test Field 2";

    bool succeeded = false;
    mTable.insert(1, mField.size(), mField.c_str(), mTx, &succeeded);
    EXPECT_TRUE(succeeded);

    EXPECT_TRUE(mTable.remove(1, mTx));

    succeeded = false;
    mTable.insert(1, field2.size(), field2.c_str(), mTx, &succeeded);
    EXPECT_TRUE(succeeded);

    std::size_t size = 0;
    const char* ptr = nullptr;
    bool isNewest = false;
    EXPECT_TRUE(mTable.get(1, size, ptr, mTx, isNewest));
    EXPECT_EQ(field2, std::string(ptr, size));
    EXPECT_TRUE(isNewest);
}

/**
 * @class Table
 * @test Check if getNewest on a nonexisting key returns the version 0
 */
TEST_F(TableTest, emptyGetNewest) {
    std::size_t size = 0;
    const char* ptr = nullptr;
    uint64_t version = std::numeric_limits<uint64_t>::max();
    EXPECT_FALSE(mTable.getNewest(1, size, ptr, version));
    EXPECT_EQ(0, version);
}

/**
 * @class Table
 * @test Check if getNewest returns the recently inserted key
 */
TEST_F(TableTest, insertGetNewest) {
    bool succeeded = false;
    mTable.insert(1, mField.size(), mField.c_str(), mTx, &succeeded);
    EXPECT_TRUE(succeeded);

    std::size_t size = 0;
    const char* ptr = nullptr;
    uint64_t version = 0;
    EXPECT_TRUE(mTable.getNewest(1, size, ptr, version));
    EXPECT_EQ(mField, std::string(ptr, size));
    EXPECT_EQ(1, version);
}

/**
 * @class Table
 * @test Check if getNewest on a removed key returns false with the correct version
 */
TEST_F(TableTest, insertRemoveGetNewest) {
    bool succeeded = false;
    mTable.insert(1, mField.size(), mField.c_str(), mTx, &succeeded);
    EXPECT_TRUE(succeeded);

    EXPECT_TRUE(mTable.remove(1, mTx));

    std::size_t size = 0;
    const char* ptr = nullptr;
    uint64_t version = 0;
    EXPECT_FALSE(mTable.getNewest(1, size, ptr, version));
    EXPECT_EQ(1, version);
}

}
