#include <gtest/gtest.h>
#include <tellstore.hpp>

#include <crossbow/allocator.hpp>

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
    StorageConfig config;
    config.totalMemory = 0x10000000ull;
    config.hashMapCapacity = 0x100000ull;
    Storage storage(config);
    Schema schema;
    schema.addField(FieldType::INT, "foo", true);
    uint64_t tId;
    crossbow::string tableName = "testTable";
    {
        crossbow::allocator _; // needed to free memory
        auto res = storage.createTable(tableName, schema, tId);
        ASSERT_TRUE(res) << "creating table failed";
        EXPECT_TRUE(correctTableId(tableName, tId, storage));
        // Force GC - since we did not do anything yet, this should
        // just return. We can not really test whether it worked
        // correctly, but at least it must not segfault
        storage.forceGC();
        auto tx = storage.startTx();
        storage.insert(tId, 1, GenericTuple({std::make_pair<crossbow::string, boost::any>("foo", 12)}), tx, &res);
        ASSERT_TRUE(res) << "This insert must not fail!";
        bool isNewest = false;
        const char* rec;
        size_t s;
        res = storage.get(tId, 1, s, rec, tx, isNewest);
        ASSERT_TRUE(res) << "Tuple not found";
        ASSERT_TRUE(isNewest) << "There should not be any versioning at this point";
        storage.forceGC();
    }
    {
        crossbow::allocator _;
        uint64_t sTid;
        ASSERT_TRUE(storage.getTableId(tableName, sTid)) << "This table exists";
        ASSERT_EQ(sTid, tId) << "Table Id did change";
    }
}

}
