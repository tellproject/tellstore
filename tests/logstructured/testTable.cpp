#include <config.h>

#include <logstructured/Table.hpp>

#include <util/CommitManager.hpp>
#include <util/OpenAddressingHash.hpp>
#include <util/PageManager.hpp>
#include <util/Record.hpp>

#include <crossbow/allocator.hpp>

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
            : mPageManager(PageManager::construct(4 * TELL_PAGE_SIZE)),
              mHashMap(1024),
              mTable(*mPageManager, mSchema, 1, mHashMap),
              mTx(mCommitManager.startTx()),
              mField("Test Field") {
    }

    /**
     * @brief Assert that the table contains the given element
     *
     * @param key Key of the tuple to check
     * @param tx Snapshot descriptor of the current transaction
     * @param expected Expected value of the tuple
     * @param expectedNewest Expected the newest element
     */
    void assertElement(uint64_t key, const SnapshotDescriptor& tx, const std::string& expected, bool expectedNewest) {
        std::size_t size = 0;
        const char* ptr = nullptr;
        bool isNewest = false;
        EXPECT_TRUE(mTable.get(key, size, ptr, tx, isNewest));
        EXPECT_EQ(expected, std::string(ptr, size));
        EXPECT_EQ(expectedNewest, isNewest);
    }

    crossbow::allocator mAlloc;
    PageManager::Ptr mPageManager;
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

    assertElement(1, mTx, mField, true);
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
        assertElement(e.first, mTx, e.second, true);
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
    assertElement(1, mTx, fieldNew, true);
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

    assertElement(1, mTx, field2, true);
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
    EXPECT_EQ(2, version);
}

/**
 * @class Table
 * @test Check if getNewest on a removed key returns false with the correct version
 */
TEST_F(TableTest, insertRemoveGetNewest) {
    bool succeeded = false;
    mTable.insert(1, mField.size(), mField.c_str(), mTx, &succeeded);
    EXPECT_TRUE(succeeded);

    mCommitManager.commitTx(mTx);

    auto tx2 = mCommitManager.startTx();

    EXPECT_TRUE(mTable.remove(1, tx2));

    std::size_t size = 0;
    const char* ptr = nullptr;
    uint64_t version = 0;
    EXPECT_FALSE(mTable.getNewest(1, size, ptr, version));
    EXPECT_EQ(tx2.version(), version);
}

/**
 * @class Table
 * @test Check if updating an element and reverting it restores the previous element
 */
TEST_F(TableTest, insertUpdateRevert) {
    std::string fieldNew = "Test Field Update";

    // Insert first element
    bool succeeded = false;
    mTable.insert(1, mField.size(), mField.c_str(), mTx, &succeeded);
    EXPECT_TRUE(succeeded);

    // Commit insert transaction
    mCommitManager.commitTx(mTx);

    // Begin update-revert transaction
    auto tx2 = mCommitManager.startTx();

    // Update element
    EXPECT_TRUE(mTable.update(1, fieldNew.size(), fieldNew.c_str(), tx2));
    assertElement(1, tx2, fieldNew, true);

    // Revert element
    EXPECT_TRUE(mTable.revert(1, tx2));
    assertElement(1, tx2, mField, true);
}

}
