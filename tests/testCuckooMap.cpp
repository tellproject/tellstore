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
#include <util/CuckooHash.hpp>
#include <gtest/gtest.h>
#include <memory>
#include <random>
#include <set>

using namespace tell::store;

class CuckooTest : public ::testing::Test {
protected:
    CuckooTest()
            : pageManager(20 * TELL_PAGE_SIZE),
              table(new CuckooTable(pageManager)) {
    }

    virtual ~CuckooTest() {
        table->destroy();
        delete table;
    }

    PageManager pageManager;

    CuckooTable* table;
};

TEST_F(CuckooTest, GetOnEmpty) {
    CuckooTable& currTable = *table;
    // we check for 1000 random values, that get fails
    constexpr int num_tests = 1000;
    std::random_device rnd;
    std::uniform_int_distribution<uint64_t> dist;
    for (int i = 0; i < num_tests; ++i) {
        auto val = dist(rnd);
        ASSERT_EQ(currTable.get(val), nullptr) << "Value " << val << " must not exist in table";
    }
}

TEST_F(CuckooTest, SimpleInsert) {
    std::vector<void*> obsoletePages;
    CuckooTable& currTable = *table;
    uint64_t key = 1937;
    std::unique_ptr<int> value(new int(8713));
    auto modifier = currTable.modifier(obsoletePages);
    ASSERT_TRUE(modifier.insert(key, value.get())) << "Insertion of inexistent value not succeeded";
    auto oldTable = table;
    table = modifier.done();
    ASSERT_NE(table, nullptr) << "Modifier done returned nullptr";
    ASSERT_NE(table, oldTable) << "After modification, the table must change";
    ASSERT_FALSE(obsoletePages.empty()) << "After modification, pages must be obsolete";
    delete oldTable;
    pageManager.free(obsoletePages);
    CuckooTable& nTable = *table;
    int* ptr = reinterpret_cast<int*>(nTable.get(key));
    ASSERT_EQ(ptr, value.get()) << "Table get returned wrong value";
    ASSERT_EQ(*ptr, 8713) << "Value changed";
}

class CuckooTestFilled : public CuckooTest {
protected:
    constexpr static size_t numEntries = 1024;
    std::set<uint64_t> entries;
    int value;
    CuckooTestFilled() : value(1) {
        // instantiate with a seed, to make sure, that we always get
        // the same numbers
        std::mt19937 rnd(1);
        std::uniform_int_distribution<uint64_t> dist;
        for (size_t i = 0u; i < numEntries; ++i) {
            auto res = entries.insert(dist(rnd));
            if (!res.second) --i;
        }
    }
    virtual ~CuckooTestFilled() {}
    virtual void SetUp() {
        std::vector<void*> obsoletePages;
        auto m = table->modifier(obsoletePages);
        for (auto e : entries) {
            m.insert(e, &value);
        }
        auto old = table;
        table = m.done();
        delete old;
        pageManager.free(obsoletePages);
    }
};

TEST_F(CuckooTestFilled, AllExist) {
    CuckooTable& t = *table;
    for (auto e : entries) {
        auto ptr = t.get(e);
        ASSERT_NE(ptr, nullptr);
        ASSERT_EQ(*reinterpret_cast<decltype(value)*>(ptr), value);
    }
}

TEST_F(CuckooTestFilled, DoesNotReplace) {
    std::vector<void*> obsoletePages;
    std::unique_ptr<int> value(new int(8713));
    Modifier m = table->modifier(obsoletePages);
    for (auto e : entries) {
        ASSERT_FALSE(m.insert(e, value.get())) << "Replaced value for " << e;
    }
    ASSERT_TRUE(obsoletePages.empty());
}

TEST_F(CuckooTestFilled, TestResize) {
    std::vector<void*> obsoletePages;
    int oVal = 2;
    Modifier m = table->modifier(obsoletePages);
    size_t oldCapacity = m.capacity();
    decltype(entries) newEntries;
    std::mt19937 rnd(10);
    std::uniform_int_distribution<uint64_t> dist;
    auto toAdd = m.capacity() - m.size();
    for (decltype(toAdd) i = 0; i <= toAdd; ++i) {
        auto nVal = dist(rnd);
        if (entries.find(nVal) != entries.end()) {
            --i;
            continue;
        }
        ASSERT_TRUE(m.insert(nVal, &oVal));
    }
    ASSERT_NE(oldCapacity, m.capacity());
    auto oldTable = table;
    table = m.done();
    delete oldTable;
    pageManager.free(obsoletePages);
    auto& t = *table;
    for (auto e : entries) {
        auto ptr = reinterpret_cast<int*>(t.get(e));
        ASSERT_EQ(ptr, &value);
        ASSERT_EQ(*ptr, value);
    }
    for (auto e : newEntries) {
        auto ptr = reinterpret_cast<int*>(t.get(e));
        ASSERT_EQ(ptr, &oVal);
        ASSERT_EQ(*ptr, oVal);
    }
}
