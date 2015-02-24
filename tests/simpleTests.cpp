#include <gtest/gtest.h>
#include <tellstore.hpp>
#include <util/Epoch.hpp>

using namespace tell::store;

namespace {

::testing::AssertionResult correctTableId(const crossbow::string& name, uint64_t tId, Storage& storage)
{
    uint64_t id;
    if (!storage.getTableId(name, id))
        return ::testing::AssertionFailure() << "Table does not exist";
    else if (tId == id)
        return ::testing::AssertionSuccess();
    else
        return ::testing::AssertionFailure() << "Expected tid " << tId << " got " << id << " instead";
}

TEST(simple, insert_and_get)
{
    tell::store::allocator _; // needed to free memory
    StorageConfig config;
    Storage storage(config);
    uint64_t tId;
    crossbow::string tableName = "testTable";
    Schema schema;
    auto res = storage.creteTable(tableName, schema, tId);
    ASSERT_TRUE(res) << "creating table failed";
    EXPECT_TRUE(correctTableId(tableName, tId, storage));
}

}
