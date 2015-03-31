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
    LOG_ASSERT(false, "Blubb");
    tell::store::allocator _; // needed to free memory
    StorageConfig config;
    Storage storage(config);
    uint64_t tId;
    crossbow::string tableName = "testTable";
    Schema schema;
    schema.addField(FieldType::INT, "foo", true);
    auto res = storage.creteTable(tableName, schema, tId);
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
    res = storage.get(tId, 1, rec, tx, isNewest);
    ASSERT_TRUE(res) << "Tuple not found";
    ASSERT_TRUE(isNewest) << "There should not be any versioning at this point";
    storage.forceGC();
}

}
