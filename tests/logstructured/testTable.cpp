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
#include <config.h>

#include <logstructured/Table.hpp>

#include "../DummyCommitManager.hpp"

#include <util/OpenAddressingHash.hpp>
#include <util/PageManager.hpp>
#include <util/VersionManager.hpp>

#include <tellstore/Record.hpp>

#include <commitmanager/SnapshotDescriptor.hpp>

#include <crossbow/allocator.hpp>

#include <gtest/gtest.h>

#include <limits>
#include <utility>
#include <vector>

using namespace tell;
using namespace tell::store;
using namespace tell::store::logstructured;

namespace {

class TableTest : public ::testing::Test {
protected:
    TableTest()
            : mPageManager(PageManager::construct(4 * TELL_PAGE_SIZE)),
              mHashMap(1024),
              mSchema(TableType::TRANSACTIONAL),
              mTable(*mPageManager, "testTable", mSchema, 1, mVersionManager, mHashMap),
              mTx(mCommitManager.startTx()),
              mField("Test Field") {
    }

    /**
     * @brief Assert that the table contains the given element
     *
     * @param key Key of the tuple to check
     * @param tx Snapshot descriptor of the current transaction
     * @param expected Expected value of the tuple
     * @param expectedVersion Expected version of the element
     * @param expectedNewest Expected the newest element
     */
    void assertElement(uint64_t key, const commitmanager::SnapshotDescriptor& tx, const std::string& expected,
            uint64_t expectedVersion, bool expectedNewest) {
        std::string dest;
        EXPECT_EQ(0, mTable.get(key, tx, [&dest, expectedVersion, expectedNewest]
                (size_t size, uint64_t version, bool isNewest) {
            EXPECT_EQ(expectedVersion, version);
            EXPECT_EQ(expectedNewest, isNewest);
            dest.resize(size);
            return &dest[0];
        }));
        EXPECT_EQ(expected, dest);
    }

    void assertElement(uint64_t key, const commitmanager::SnapshotDescriptor& tx, const std::string& expected,
            bool expectedNewest) {
        assertElement(key, tx, expected, tx.version(), expectedNewest);
    }

    crossbow::allocator mAlloc;
    PageManager::Ptr mPageManager;
    VersionManager mVersionManager;
    Table::HashTable mHashMap;
    Schema mSchema;

    DummyCommitManager mCommitManager;

    Table mTable;

    Transaction mTx;
    std::string mField;
};

/**
 * @class Table
 * @test Check if an insert followed by a get returns the inserted element
 */
TEST_F(TableTest, insertGet) {
    EXPECT_EQ(0, mTable.insert(1, mField.size(), mField.c_str(), *mTx));

    assertElement(1, *mTx, mField, true);
    mTx.commit();
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
        EXPECT_EQ(0, mTable.insert(e.first, e.second.size(), e.second.c_str(), *mTx));
    }

    for (auto& e : elements) {
        assertElement(e.first, *mTx, e.second, true);
    }
    mTx.commit();
}

/**
 * @class Table
 * @test Check if an insert followed by an update returns the updated element
 */
TEST_F(TableTest, insertUpdateGet) {
    std::string fieldNew = "Test Field Update";

    EXPECT_EQ(0, mTable.insert(1, mField.size(), mField.c_str(), *mTx));

    EXPECT_EQ(0, mTable.update(1, fieldNew.size(), fieldNew.c_str(), *mTx));
    assertElement(1, *mTx, fieldNew, true);
    mTx.commit();
}

/**
 * @class Table
 * @test Check if an insert followed by a remove returns no element in the same transaction
 */
TEST_F(TableTest, insertRemoveGetSameTransaction) {
    EXPECT_EQ(0, mTable.insert(1, mField.size(), mField.c_str(), *mTx));

    EXPECT_EQ(0, mTable.remove(1, *mTx));

    std::unique_ptr<char[]> dest;
    EXPECT_EQ(error::not_found, mTable.get(1, *mTx, [&dest] (size_t size, uint64_t version, bool isNewest) {
        dest.reset(new char[size]);
        return dest.get();
    }));

    mTx.commit();
}

/**
 * @class Table
 * @test Check if an insert followed by a remove returns no element
 */
TEST_F(TableTest, insertRemoveGet) {
    EXPECT_EQ(0, mTable.insert(1, mField.size(), mField.c_str(), *mTx));

    // Commit insert transaction
    mTx.commit();

    // Begin remove transaction
    auto tx2 = mCommitManager.startTx();

    // Remove element
    EXPECT_EQ(0, mTable.remove(1, *tx2));

    // Try to retrieve deleted element
    std::unique_ptr<char[]> dest;
    EXPECT_EQ(error::not_found, mTable.get(1, *tx2, [&dest] (size_t size, uint64_t version, bool isNewest) {
        dest.reset(new char[size]);
        return dest.get();
    }));

    tx2.commit();
}

/**
 * @class Table
 * @test Check if a removed element can be written again
 */
TEST_F(TableTest, insertRemoveInsertGet) {
    std::string field2 = "Test Field 2";

    EXPECT_EQ(0, mTable.insert(1, mField.size(), mField.c_str(), *mTx));

    EXPECT_EQ(0, mTable.remove(1, *mTx));

    EXPECT_EQ(0, mTable.insert(1, field2.size(), field2.c_str(), *mTx));

    assertElement(1, *mTx, field2, true);
    mTx.commit();
}

/**
 * @class Table
 * @test Check if updating an element and reverting it restores the previous element
 */
TEST_F(TableTest, insertUpdateRevert) {
    std::string fieldNew = "Test Field Update";

    // Insert first element
    EXPECT_EQ(0, mTable.insert(1, mField.size(), mField.c_str(), *mTx));

    // Commit insert transaction
    mTx.commit();

    // Begin update-revert transaction
    auto tx2 = mCommitManager.startTx();

    // Update element
    EXPECT_EQ(0, mTable.update(1, fieldNew.size(), fieldNew.c_str(), *tx2));
    assertElement(1, *tx2, fieldNew, true);

    // Revert element
    EXPECT_EQ(0, mTable.revert(1, *tx2));
    assertElement(1, *tx2, mField, mTx->version(), true);
    tx2.commit();
}

}
