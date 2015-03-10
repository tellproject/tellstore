#include <util/CuckooHash.hpp>
#include <gtest/gtest.h>
#include <memory>
#include <random>
#include <set>

using namespace tell::store;

namespace {
PageManager pageManager(100*TELL_PAGE_SIZE);
std::atomic<CuckooTable*> table;
}

class CuckooTest : public ::testing::Test {
protected:
    CuckooTest()
    {
        table.store(new (allocator::malloc(sizeof(CuckooTable))) CuckooTable(pageManager));
    }
    virtual ~CuckooTest() {
        auto t = table.load();
        t->~CuckooTable();
        allocator::free_now(t);
        table.store(nullptr);
    }
};

TEST_F(CuckooTest, GetOnEmpty) {
    CuckooTable& currTable = *table.load();
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
    allocator alloc;
    CuckooTable& currTable = *table.load();
    uint64_t key = 1937;
    std::unique_ptr<int> value(new int(8713));
    auto modifier = currTable.modifier(alloc);
    ASSERT_TRUE(modifier.insert(key, value.get(), false)) << "Insertion of inexistent value not succeeded";
    table.store(modifier.done());
    ASSERT_NE(table.load(), nullptr) << "Modifier done returned nullptr";
    CuckooTable& nTable = *table.load();
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
        allocator alloc;
        auto m = table.load()->modifier(alloc);
        for (auto e : entries) {
            m.insert(e, &value, false);
        }
        auto old = table.load();
        table.store(m.done());
        allocator::free_now(old);
    }
};

TEST_F(CuckooTestFilled, AllExist) {
    CuckooTable& t = *table.load();
    for (auto e : entries) {
        auto ptr = t.get(e);
        ASSERT_NE(ptr, nullptr);
        ASSERT_EQ(*reinterpret_cast<decltype(value)*>(ptr), value);
    }
}

TEST_F(CuckooTestFilled, DoesNotReplace) {
    allocator alloc;
    Modifier m = table.load()->modifier(alloc);
    for (auto e : entries) {
        ASSERT_FALSE(m.insert(e, nullptr, false)) << "Replaced value for " << e;
    }
}

TEST_F(CuckooTestFilled, TestResize) {
    allocator alloc;
    int oVal = 2;
    Modifier m = table.load()->modifier(alloc);
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
        ASSERT_TRUE(m.insert(nVal, &oVal, false));
    }
    ASSERT_NE(oldCapacity, m.capacity());
    auto oldTable = table.load();
    table.store(m.done());
    oldTable->~CuckooTable();
    allocator::free_now(oldTable);
    auto& t = *table.load();
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
