#include <gtest/gtest.h>
#include <tellstore.hpp>

#include "DummyCommitManager.hpp"

#include <crossbow/allocator.hpp>

using namespace tell::store;

namespace {

::testing::AssertionResult correctTableId(const crossbow::string& name, uint64_t tId, Storage& storage)
{
    uint64_t id;
    if (!storage.getTable(name, id))
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
    Schema schema(TableType::TRANSACTIONAL);
    schema.addField(FieldType::INT, "foo", true);
    Record record(schema);
    uint64_t tId;
    crossbow::string tableName = "testTable";
    DummyCommitManager commitManager;
    {
        crossbow::allocator _; // needed to free memory
        auto res = storage.createTable(tableName, schema, tId);
        ASSERT_TRUE(res) << "creating table failed";
        EXPECT_TRUE(correctTableId(tableName, tId, storage));
        // Force GC - since we did not do anything yet, this should
        // just return. We can not really test whether it worked
        // correctly, but at least it must not segfault
        storage.forceGC();
        auto tx = commitManager.startTx();
        {
            size_t size;
            std::unique_ptr<char[]> rec(record.create(GenericTuple({
                    std::make_pair<crossbow::string, boost::any>("foo", 12)
            }), size));
            storage.insert(tId, 1, size, rec.get(), tx, &res);
            ASSERT_TRUE(res) << "This insert must not fail!";
        }
        {
            bool isNewest = false;
            uint64_t version = 0x0u;
            const char* rec;
            size_t s;
            res = storage.get(tId, 1, s, rec, tx, version, isNewest);
            ASSERT_TRUE(res) << "Tuple not found";
            ASSERT_EQ(tx.descriptor().version(), version) << "Tuple has not the version of the snapshot descriptor";
            ASSERT_TRUE(isNewest) << "There should not be any versioning at this point";
        }
        storage.forceGC();
    }
    {
        crossbow::allocator _;
        uint64_t sTid;
        ASSERT_TRUE(storage.getTable(tableName, sTid) != nullptr) << "This table exists";
        ASSERT_EQ(sTid, tId) << "Table Id did change";
    }
}


int64_t gTupleLargenumber = 0x7FFFFFFF00000001;
crossbow::string gTupleText1 = crossbow::string("Bacon ipsum dolor amet t-bone chicken prosciutto, cupim ribeye turkey "
        "bresaola leberkas bacon.");
crossbow::string gTupleText2 = crossbow::string("Chuck pork loin ham hock tri-tip pork ball tip drumstick tongue. Jowl "
        "swine short loin, leberkas andouille pancetta strip steak doner ham bresaola.");

class HeavyTest : public ::testing::Test {
public:
    HeavyTest()
            : mSchema(TableType::TRANSACTIONAL),
              mTableId(0),
              mTupleSize(0),
              mGo(false) {
        StorageConfig config;
        config.totalMemory = 0x100000000ull;
        config.numScanThreads = 0;
        config.hashMapCapacity = 0x2000000ull;
        mStorage.reset(new Storage(config));
    }

    virtual void SetUp() {
        mSchema.addField(FieldType::INT, "number", true);
        mSchema.addField(FieldType::TEXT, "text1", true);
        mSchema.addField(FieldType::BIGINT, "largenumber", true);
        mSchema.addField(FieldType::TEXT, "text2", true);

        Record record(mSchema);
        for (int32_t i = 0; i < mTuple.size(); ++i) {
            GenericTuple insertTuple({
                    std::make_pair<crossbow::string, boost::any>("number", i),
                    std::make_pair<crossbow::string, boost::any>("text1", gTupleText1),
                    std::make_pair<crossbow::string, boost::any>("largenumber", gTupleLargenumber),
                    std::make_pair<crossbow::string, boost::any>("text2", gTupleText2)
            });
            mTuple[i].reset(record.create(insertTuple, mTupleSize));
        }

        ASSERT_TRUE(mStorage->createTable("testTable", mSchema, mTableId));
    }

    void run(uint64_t startKey, uint64_t endKey) {
        while (!mGo.load()) {
        }

        Record record(mSchema);
        auto transaction = mCommitManager.startTx();

        for (auto key = startKey; key < endKey; ++key) {
            bool succeeded = false;
            mStorage->insert(mTableId, key, mTupleSize, mTuple[key % mTuple.size()].get(), transaction, &succeeded);
            ASSERT_TRUE(succeeded);

            size_t getSize;
            const char* getData;
            uint64_t version = 0x0u;
            bool isNewest = false;
            succeeded = false;
            succeeded = mStorage->get(mTableId, key, getSize, getData, transaction, version, isNewest);
            ASSERT_TRUE(succeeded);
            EXPECT_EQ(version, transaction.descriptor().version());
            EXPECT_TRUE(isNewest);

            auto numberData = getTupleData(getData, record, "number");
            EXPECT_EQ(key % mTuple.size(), *reinterpret_cast<const int32_t*>(numberData));

            auto text1Data = getTupleData(getData, record, "text1");
            EXPECT_EQ(gTupleText1, crossbow::string(text1Data + sizeof(uint32_t),
                    *reinterpret_cast<const uint32_t*>(text1Data)));

            auto largenumberData = getTupleData(getData, record, "largenumber");
            EXPECT_EQ(gTupleLargenumber, *reinterpret_cast<const int64_t*>(largenumberData));

            auto text2Data = getTupleData(getData, record, "text2");
            EXPECT_EQ(gTupleText2, crossbow::string(text2Data + sizeof(uint32_t),
                    *reinterpret_cast<const uint32_t*>(text2Data)));
        }

        transaction.commit();
    }

    const char* getTupleData(const char* data, Record& record, const crossbow::string& name) {
        Record::id_t recordField;
        if (!record.idOf(name, recordField)) {
            LOG_ERROR("%1% field not found", name);
        }
        bool fieldIsNull;
        auto fieldData = record.data(data, recordField, fieldIsNull);
        return fieldData;
    }

protected:
    std::unique_ptr<Storage> mStorage;
    DummyCommitManager mCommitManager;

    Schema mSchema;

    uint64_t mTableId;

    uint64_t mTupleSize;
    std::array<std::unique_ptr<char[]>, 4> mTuple;

    std::atomic<bool> mGo;
};



TEST_F(HeavyTest, heavy) {
    std::array<std::thread, 3> threads = {
        std::thread(std::bind(&HeavyTest::run, this, 0, 2500000)),
        std::thread(std::bind(&HeavyTest::run, this, 2500000, 5000000)),
        std::thread(std::bind(&HeavyTest::run, this, 5000000, 7500000)),
    };

    mGo.store(true);
    run(7500000, 10000000);

    for (auto& t : threads) {
        t.join();
    }
}

}
