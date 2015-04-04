#include <config.h>
#include <util/Log.hpp>

#include <gtest/gtest.h>

#include <array>
#include <chrono>
#include <thread>
#include <unordered_set>

using namespace tell::store;

namespace {

class LogPageTest : public ::testing::Test {
protected:
    LogPageTest()
            : mPageManager(TELL_PAGE_SIZE * 1),
              mPage(nullptr) {
    }

    virtual void SetUp() {
        mPage = new(mPageManager.alloc()) LogPage();
    }

    virtual void TearDown() {
        mPageManager.free(mPage);
    }

    PageManager mPageManager;

    LogPage* mPage;
};

/**
 * @class LogPage
 * @test Check if entries are 8 byte aligned
 */
TEST_F(LogPageTest, entryAlignment) {
    auto entry = mPage->append(31);
    EXPECT_EQ(40, entry->size()) << "Entry is not 8 byte aligned";
}

/**
 * @class LogPage
 * @test Test sealing of entries
 */
TEST_F(LogPageTest, sealEntry) {
    auto entry = mPage->append(31);
    EXPECT_FALSE(entry->sealed()) << "Newly created entry is sealed";

    entry->seal();
    EXPECT_TRUE(entry->sealed()) << "Sealed entry is not sealed";
}

/**
 * @class LogPage
 * @test Test sealing of pages
 */
TEST_F(LogPageTest, sealPage) {
    EXPECT_FALSE(mPage->sealed()) << "Newly created page is sealed";

    mPage->seal();
    EXPECT_TRUE(mPage->sealed()) << "Sealed page is not sealed";
}

/**
 * @class LogPage
 * @test Check that no more writes are allowed to a sealed page
 */
TEST_F(LogPageTest, appendSealedPage) {
    mPage->seal();

    auto entry = mPage->append(31);
    EXPECT_EQ(nullptr, entry) << "No entries should be appended to sealed page";
}

/**
 * @class LogPage
 * @test Check that iterating over all entries in a page works correctly
 */
TEST_F(LogPageTest, entryIterator) {
    auto entry1 = mPage->append(31);
    EXPECT_NE(nullptr, entry1) << "Failed to allocate entry";
    auto entry2 = mPage->append(55);
    EXPECT_NE(nullptr, entry2) << "Failed to allocate entry";

    auto i = mPage->begin();
    auto end = mPage->end();

    EXPECT_EQ(entry1, &(*i)) << "Iterator not pointing to first entry";
    EXPECT_NE(end, i) << "Iterator pointing to end";

    ++i;
    EXPECT_EQ(entry2, &(*i)) << "Iterator not pointing to second entry";
    EXPECT_NE(end, i) << "Iterator pointing to end";

    ++i;
    EXPECT_EQ(end, i) << "Iterator not pointing to end";
}

class UnorderedLogTest : public ::testing::Test {
protected:
    UnorderedLogTest()
            : mPageManager(TELL_PAGE_SIZE * 10),
              mLog(mPageManager) {
    }

    PageManager mPageManager;

    Log<UnorderedLogImpl> mLog;
};

/**
 * @class Log
 * @test Check that appends work correctly
 *
 * Allocates 3 entries: One small on head page, then a large one that does not fit directly on the head page, then a
 * second large one that triggers allocation of a new head segment.
 */
TEST_F(UnorderedLogTest, append) {
    auto head = mLog.head();

    auto entry1 = mLog.append(31);
    EXPECT_NE(nullptr, entry1) << "Failed to allocate entry";

    auto entry2 = head->append(LogPage::MAX_SIZE - 4);
    EXPECT_EQ(nullptr, entry2) << "Allocated entry outside page boundary";

    auto entry3 = mLog.append(LogPage::MAX_SIZE - 4);
    EXPECT_NE(nullptr, entry3) << "Failed to allocate entry";

    EXPECT_NE(head, mLog.head()) << "New head is the same as the old head";
    EXPECT_EQ(head, mLog.head()->next()) << "Next pointer of head does not point to old head";
}

/**
 * @class Log
 * @test Check that iterating over all pages in the log works correctly
 */
TEST_F(UnorderedLogTest, pageIterator) {
    auto entry1 = mLog.append(31);
    EXPECT_NE(nullptr, entry1) << "Failed to allocate entry";
    auto entry2 = mLog.append(LogPage::MAX_SIZE - 4);
    EXPECT_NE(nullptr, entry2) << "Failed to allocate entry";

    EXPECT_NE(nullptr, mLog.head()->next()) << "Log contains only one page";

    auto i = mLog.pageBegin();
    auto end = mLog.pageEnd();

    EXPECT_EQ(mLog.head(), &(*i)) << "Iterator not pointing to first page";
    EXPECT_NE(end, i) << "Iterator pointing to end";

    ++i;
    EXPECT_EQ(mLog.head()->next(), &(*i)) << "Iterator not pointing to second page";
    EXPECT_NE(end, i) << "Iterator pointing to end";

    ++i;
    EXPECT_EQ(end, i) << "Iterator not pointing to end";
}

/**
 * @class Log
 * @test Check that iterating over the complete log works correctly
 *
 * Allocates two entries on the first page and a third one on another log page. Because we iterate pages from head to
 * tail and inside pages from top to bottom, the order of elements should be 3 - 1 - 2.
 */
TEST_F(UnorderedLogTest, logIterator) {
    auto entry1 = mLog.append(31);
    EXPECT_NE(nullptr, entry1) << "Failed to allocate entry";
    auto entry2 = mLog.append(55);
    EXPECT_NE(nullptr, entry2) << "Failed to allocate entry";
    auto entry3 = mLog.append(LogPage::MAX_SIZE - 4);
    EXPECT_NE(nullptr, entry3) << "Failed to allocate entry";

    EXPECT_NE(nullptr, mLog.head()->next()) << "Log contains only one page";


    auto i = mLog.begin();
    auto end = mLog.end();

    EXPECT_EQ(entry3, &(*i)) << "Iterator not pointing to third entry";
    EXPECT_NE(end, i) << "Iterator pointing to end";

    ++i;
    EXPECT_EQ(entry1, &(*i)) << "Iterator not pointing to first entry";
    EXPECT_NE(end, i) << "Iterator pointing to end";

    ++i;
    EXPECT_EQ(entry2, &(*i)) << "Iterator not pointing to second entry";
    EXPECT_NE(end, i) << "Iterator pointing to end";

    ++i;
    EXPECT_EQ(end, i) << "Iterator not pointing to end";
}

class OrderedLogTest : public ::testing::Test {
protected:
    OrderedLogTest()
            : mPageManager(TELL_PAGE_SIZE * 10),
              mLog(mPageManager) {
    }

    PageManager mPageManager;

    Log<OrderedLogImpl> mLog;
};

/**
 * @class Log
 * @test Check that appends work correctly
 *
 * Allocates 3 entries: One small on head page, then a large one that does not fit directly on the head page, then a
 * second large one that triggers allocation of a new head segment.
 */
TEST_F(OrderedLogTest, append) {
    auto tail = mLog.head();

    auto entry1 = mLog.append(31);
    EXPECT_NE(nullptr, entry1) << "Failed to allocate entry";

    auto entry2 = tail->append(LogPage::MAX_SIZE - 4);
    EXPECT_EQ(nullptr, entry2) << "Allocated entry outside page boundary";

    auto entry3 = mLog.append(LogPage::MAX_SIZE - 4);
    EXPECT_NE(nullptr, entry3) << "Failed to allocate entry";

    EXPECT_NE(tail, mLog.head()) << "New head is the same as the old head";
    EXPECT_EQ(tail->next(), mLog.head()) << "Next pointer of old head does not point to new head";
}

/**
 * @brief Test fixture for testing prefilled OrderedLog classes
 *
 * Adds 3 entries, the first two on the first page and the third entry on the second page.
 */
class OrderedLogFilledTest : public OrderedLogTest {
protected:
    OrderedLogFilledTest()
            : mEntry1(nullptr), mEntry2(nullptr), mEntry3(nullptr) {
    }

    virtual void SetUp() {
        mEntry1 = mLog.append(31);
        EXPECT_NE(nullptr, mEntry1) << "Failed to allocate entry";
        mEntry2 = mLog.append(55);
        EXPECT_NE(nullptr, mEntry2) << "Failed to allocate entry";
        mEntry3 = mLog.append(LogPage::MAX_SIZE - 4);
        EXPECT_NE(nullptr, mEntry3) << "Failed to allocate entry";

        EXPECT_NE(nullptr, mLog.tail()->next().load()) << "Log contains only one page";
    }

    LogEntry* mEntry1;
    LogEntry* mEntry2;
    LogEntry* mEntry3;
};

/**
 * @class Log
 * @test Check that iterating over all pages in the log works correctly
 */
TEST_F(OrderedLogFilledTest, pageIterator) {
    auto tail = mLog.tail();
    auto i = mLog.pageBegin();
    auto end = mLog.pageEnd();

    EXPECT_EQ(tail, &(*i)) << "Iterator not pointing to first page";
    EXPECT_NE(end, i) << "Iterator pointing to end";

    ++i;
    EXPECT_EQ(tail->next().load(), &(*i)) << "Iterator not pointing to second page";
    EXPECT_NE(end, i) << "Iterator pointing to end";

    ++i;
    EXPECT_EQ(end, i) << "Iterator not pointing to end";
}

/**
 * @class Log
 * @test Check that iterating over the complete log works correctly
 *
 * Allocates two entries on the first page and a third one on another log page. Because we iterate pages from tail to
 * head and inside pages from top to bottom, the order of elements should be 1 - 2 - 3.
 */
TEST_F(OrderedLogFilledTest, logIterator) {
    auto i = mLog.begin();
    auto end = mLog.end();

    EXPECT_EQ(mEntry1, &(*i)) << "Iterator not pointing to first entry";
    EXPECT_NE(end, i) << "Iterator pointing to end";

    ++i;
    EXPECT_EQ(mEntry2, &(*i)) << "Iterator not pointing to second entry";
    EXPECT_NE(end, i) << "Iterator pointing to end";

    ++i;
    EXPECT_EQ(mEntry3, &(*i)) << "Iterator not pointing to third entry";
    EXPECT_NE(end, i) << "Iterator pointing to end";

    ++i;
    EXPECT_EQ(end, i) << "Iterator not pointing to end";
}

/**
 * @class Log
 * @test Check that truncation works correctly
 */
TEST_F(OrderedLogFilledTest, truncateLog) {
    auto tail = mLog.tail();
    auto begin = mLog.begin();

    // Advance iterator to second page
    auto i = begin;
    ++i; ++i;

    EXPECT_TRUE(mLog.truncateLog(begin.page(), i.page()));
    EXPECT_EQ(mLog.tail(), tail->next()) << "New tail not pointing to next page";
}

/**
 * @class Log
 * @test Check that truncating the log to the previous tail page is a no-op
 */
TEST_F(OrderedLogFilledTest, truncateLogTailPage) {
    auto tail = mLog.tail();
    auto begin = mLog.begin();

    // Advance iterator by one element (same page)
    auto i = begin;
    ++i;

    EXPECT_TRUE(mLog.truncateLog(begin.page(), i.page()));
    EXPECT_EQ(mLog.tail(), tail) << "New tail not the same as the previous tail";
}

/**
 * @class Log
 * @test Check that truncation only succeeds if the tail has not changed in the meantime
 */
TEST_F(OrderedLogFilledTest, truncateLogInvalidTail) {
    auto tail = mLog.tail();
    auto begin = mLog.begin();

    // Advance iterator to second page
    auto i = begin;
    ++i; ++i;

    EXPECT_TRUE(mLog.truncateLog(begin.page(), i.page()));
    EXPECT_EQ(mLog.tail(), tail->next()) << "Iterator not pointing to end";

    // Advance iterator to same page
    auto j = begin;
    j++;
    EXPECT_FALSE(mLog.truncateLog(begin.page(), j.page()));
}

template <typename Impl>
class LogTestThreaded : public ::testing::Test {
protected:
    static constexpr int pageCount = 100; // Number of pages to reserve in the page manager - 100

    LogTestThreaded()
            : mPageManager(TELL_PAGE_SIZE * pageCount),
              mLog(mPageManager) {
    }

    PageManager mPageManager;

    Log<Impl> mLog;
};

using Implementations = ::testing::Types<UnorderedLogImpl, OrderedLogImpl>;
TYPED_TEST_CASE(LogTestThreaded, Implementations);

/**
 * @class Log
 * @test Checks if the log is written correctly if concurrent appends happen
 *
 * Starts a number of threads that are writing a 64 bit value into the log then checks if the log contains all values
 * supposed to be written by the threads.
 *
 * This test is an extreme case because it continously appends to the log head with very small objects (8 byte), in this
 * scenario the log contention will be the highest.
 */
TYPED_TEST(LogTestThreaded, append) {
    constexpr int threadCount = 4; // Number of threads running - 4 Threads
    constexpr uint64_t valuesCount = 10000000; // Number of entries every thread writes - 10M

    // Start threads filling the log
    auto startTime = std::chrono::steady_clock::now();
    uint64_t values = 1;

    std::array<std::thread, threadCount> worker;
    for (auto& t : worker) {
        t = std::thread([valuesCount, values, this] () {
            auto end = values + valuesCount;
            for (auto i = values; i < end; ++i) {
                auto entry = this->mLog.append(sizeof(i));
                memcpy(entry->data(), &i, sizeof(i));
                entry->seal();
            }
        });
        values += valuesCount;
    }
    for (auto& t : worker) {
        t.join();
    }
    auto endTime = std::chrono::steady_clock::now();

    auto diff = endTime - startTime;
    std::cout << "Wrote " << (values - 1) << " entries in "
              << std::chrono::duration_cast<std::chrono::milliseconds>(diff).count() << "ms" << std::endl;

    // Fill set with expected values
    std::unordered_set<uint64_t> valueSet;
    valueSet.reserve(values);
    for (uint64_t i = 1; i < values; ++i) {
        valueSet.emplace(i);
    }

    // Iterate over log and remove the value in the entry from the expected values set
    auto end = this->mLog.end();
    for (auto i = this->mLog.begin(); i != end; ++i) {
        EXPECT_TRUE(i->sealed()) << "Entry is not sealed";
        uint64_t value = *reinterpret_cast<uint64_t*>(i->data());

        auto j = valueSet.find(value);
        EXPECT_NE(j, valueSet.end()) << "Value from entry is not in values set";
        valueSet.erase(j);
    }
    EXPECT_TRUE(valueSet.empty()) << "Values set is not empty";
}

}
