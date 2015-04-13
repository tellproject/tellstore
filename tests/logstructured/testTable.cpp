#include <config.h>

#include <logstructured/Table.hpp>

#include <util/CommitManager.hpp>
#include <util/OpenAddressingHash.hpp>
#include <util/PageManager.hpp>
#include <util/Record.hpp>

#include <gtest/gtest.h>

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
              mTable(mPageManager, mHashMap, mSchema, 1) {
    }

    PageManager mPageManager;
    Table::HashTable mHashMap;
    Schema mSchema;

    DummyManager mCommitManager;

    Table mTable;
};

/**
 * @class Table
 * @test Check if an insert followed by a get returns the inserted element
 */
TEST_F(TableTest, insertGet) {
    std::string field = "Test Field";

    auto tx = mCommitManager.startTx();

    bool succeeded = false;
    mTable.insert(1, field.size(), field.c_str(), tx, &succeeded);
    EXPECT_TRUE(succeeded);

    std::size_t size = 0;
    const char* ptr = nullptr;
    bool isNewest = false;
    EXPECT_TRUE(mTable.get(1, size, ptr, tx, isNewest));
    EXPECT_EQ(field, std::string(ptr, size));
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

    auto tx = mCommitManager.startTx();

    for (auto& e : elements) {
        bool succeeded = false;
        mTable.insert(e.first, e.second.size(), e.second.c_str(), tx, &succeeded);
        EXPECT_TRUE(succeeded);
    }

    for (auto& e : elements) {
        std::size_t size = 0;
        const char* ptr = nullptr;
        bool isNewest = false;
        EXPECT_TRUE(mTable.get(e.first, size, ptr, tx, isNewest));
        EXPECT_EQ(e.second, std::string(ptr, size));
        EXPECT_TRUE(isNewest);
    }
}

/**
 * @class Table
 * @test Check if an insert followed by an update returns the updated element
 */
TEST_F(TableTest, insertUpdateGet) {
    std::string field = "Test Field";
    std::string fieldNew = "Test Field Update";

    auto tx = mCommitManager.startTx();

    bool succeeded = false;
    mTable.insert(1, field.size(), field.c_str(), tx, &succeeded);
    EXPECT_TRUE(succeeded);

    EXPECT_TRUE(mTable.update(1, fieldNew.size(), fieldNew.c_str(), tx));

    std::size_t size = 0;
    const char* ptr = nullptr;
    bool isNewest = false;
    EXPECT_TRUE(mTable.get(1, size, ptr, tx, isNewest));
    EXPECT_EQ(fieldNew, std::string(ptr, size));
    EXPECT_TRUE(isNewest);
}

/**
 * @class Table
 * @test Check if an insert followed by a remove returns no element
 */
TEST_F(TableTest, insertRemoveGet) {
    std::string field = "Test Field";

    auto tx = mCommitManager.startTx();

    bool succeeded = false;
    mTable.insert(1, field.size(), field.c_str(), tx, &succeeded);
    EXPECT_TRUE(succeeded);

    EXPECT_TRUE(mTable.remove(1, tx));

    std::size_t size = 0;
    const char* ptr = nullptr;
    bool isNewest = false;
    EXPECT_FALSE(mTable.get(1, size, ptr, tx, isNewest));
    EXPECT_TRUE(isNewest);
}

/**
 * @class Table
 * @test Check if a removed element can be written again
 */
TEST_F(TableTest, insertRemoveInsertGet) {
    std::string field1 = "Test Field 1";
    std::string field2 = "Test Field 2";

    auto tx = mCommitManager.startTx();

    bool succeeded = false;
    mTable.insert(1, field1.size(), field1.c_str(), tx, &succeeded);
    EXPECT_TRUE(succeeded);

    EXPECT_TRUE(mTable.remove(1, tx));

    succeeded = false;
    mTable.insert(1, field2.size(), field2.c_str(), tx, &succeeded);
    EXPECT_TRUE(succeeded);

    std::size_t size = 0;
    const char* ptr = nullptr;
    bool isNewest = false;
    EXPECT_TRUE(mTable.get(1, size, ptr, tx, isNewest));
    EXPECT_EQ(field2, std::string(ptr, size));
    EXPECT_TRUE(isNewest);
}

}
