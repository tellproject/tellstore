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
 * @test Check if entries return the correct size
 */
TEST_F(LogPageTest, entrySize) {
    auto entry = mPage->append(31);
    EXPECT_EQ(31, entry->size()) << "Size is not the same as in append";
    EXPECT_EQ(40, entry->entrySize()) << "Entry size is not 8 byte padded";
    EXPECT_EQ(0, (reinterpret_cast<uintptr_t>(entry->data()) % 8)) << "Entry data is not 8 byte aligned";
}

/**
 * @class LogPage
 * @test Check if entryFromData works correctly
 */
TEST_F(LogPageTest, entryFromData) {
    auto entry = mPage->append(32);
    EXPECT_EQ(entry, LogEntry::entryFromData(entry->data())) << "Entry pointers are not the same";
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
    EXPECT_EQ(31, entry->size()) << "Size is not the same as in append";
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

/**
 * @brief Test fixture for testing Log implementation classes
 */
template <class Impl>
class BaseLogTest : public ::testing::Test {
protected:
    BaseLogTest()
            : mPageManager(TELL_PAGE_SIZE * 10),
              mLog(mPageManager) {
    }

    PageManager mPageManager;

    Log<Impl> mLog;
};

using UnorderedLogTest = BaseLogTest<UnorderedLogImpl>;

/**
 * @class UnorderedLogImpl
 * @test Check that appends work correctly
 *
 * Allocates 3 entries: One small on head page, then a large one that does not fit directly on the head page, then a
 * second large one that triggers allocation of a new head segment.
 */
TEST_F(UnorderedLogTest, append) {
    auto head = mLog.head();

    auto entry1 = mLog.append(31);
    EXPECT_NE(nullptr, entry1) << "Failed to allocate entry";

    auto entry2 = head->append(LogPage::MAX_DATA_SIZE);
    EXPECT_EQ(nullptr, entry2) << "Allocated entry outside page boundary";

    auto entry3 = mLog.append(LogPage::MAX_DATA_SIZE);
    EXPECT_NE(nullptr, entry3) << "Failed to allocate entry";

    EXPECT_NE(head, mLog.head()) << "New head is the same as the old head";
    EXPECT_EQ(head, mLog.head()->next()) << "Next pointer of head does not point to old head";
}

/**
 * @class UnorderedLogImpl
 * @test Check that appending two pages works
 */
TEST_F(UnorderedLogTest, appendPage) {
    auto page1 = new(mPageManager.alloc()) LogPage();
    auto page2 = new(mPageManager.alloc()) LogPage();
    page2->next().store(page1);
    mLog.appendPage(page2, page1);

    EXPECT_NE(page1, mLog.head()) << "Head pointing to appended page";
    EXPECT_NE(page2, mLog.head()) << "Head pointing to appended page";

    auto i = mLog.pageBegin();
    auto end = mLog.pageEnd();

    EXPECT_EQ(page2, &(*i)) << "Iterator not pointing to appended head page";
    EXPECT_NE(end, i) << "Iterator pointing to end";

    ++i;
    EXPECT_EQ(page1, &(*i)) << "Iterator not pointing to second appended page";
    EXPECT_NE(end, i) << "Iterator pointing to end";

    ++i;
    EXPECT_EQ(mLog.head(), &(*i)) << "Iterator not pointing to head page";
    EXPECT_NE(end, i) << "Iterator pointing to end";

    ++i;
    EXPECT_EQ(end, i) << "Iterator not pointing to end";
}

/**
 * @class UnorderedLogImpl
 * @test Check that appending pages multiple times works
 */
TEST_F(UnorderedLogTest, appendMultiplePage) {
    auto page1 = new(mPageManager.alloc()) LogPage();
    mLog.appendPage(page1);

    auto page2 = new(mPageManager.alloc()) LogPage();
    mLog.appendPage(page2);

    auto i = mLog.pageBegin();
    auto end = mLog.pageEnd();

    EXPECT_EQ(page2, &(*i)) << "Iterator not pointing to second appended head page";
    EXPECT_NE(end, i) << "Iterator pointing to end";

    ++i;
    EXPECT_EQ(page1, &(*i)) << "Iterator not pointing to first appended head page";
    EXPECT_NE(end, i) << "Iterator pointing to end";

    ++i;
    EXPECT_EQ(mLog.head(), &(*i)) << "Iterator not pointing to head page";
    EXPECT_NE(end, i) << "Iterator pointing to end";

    ++i;
    EXPECT_EQ(end, i) << "Iterator not pointing to end";
}

/**
 * @class UnorderedLogImpl
 * @test Check that elements are written to head segment when appending
 */
TEST_F(UnorderedLogTest, appendPageWriteToHead) {
    auto entry1 = mLog.append(31);
    EXPECT_NE(nullptr, entry1) << "Failed to allocate entry";

    auto page1 = new(mPageManager.alloc()) LogPage();
    auto entry2 = page1->append(16);
    EXPECT_NE(nullptr, entry2) << "Failed to allocate entry";

    mLog.appendPage(page1);

    auto entry3 = mLog.append(16);
    EXPECT_NE(nullptr, entry3) << "Failed to allocate entry";

    auto i = mLog.begin();
    auto end = mLog.end();

    EXPECT_EQ(entry2, &(*i)) << "Iterator not pointing to appended entry";
    EXPECT_NE(end, i) << "Iterator pointing to end";

    ++i;
    EXPECT_EQ(entry1, &(*i)) << "Iterator not pointing to first entry";
    EXPECT_NE(end, i) << "Iterator pointing to end";

    ++i;
    EXPECT_EQ(entry3, &(*i)) << "Iterator not pointing to second entry";
    EXPECT_NE(end, i) << "Iterator pointing to end";

    ++i;
    EXPECT_EQ(end, i) << "Iterator not pointing to end";
}

/**
 * @class UnorderedLogImpl
 * @test Check that a new head pages is allocated after the appended pages
 */
TEST_F(UnorderedLogTest, appendPageNewHead) {
    auto entry1 = mLog.append(31);
    EXPECT_NE(nullptr, entry1) << "Failed to allocate entry";

    auto page1 = new(mPageManager.alloc()) LogPage();
    auto entry2 = page1->append(16);
    EXPECT_NE(nullptr, entry2) << "Failed to allocate entry";

    mLog.appendPage(page1);

    auto entry3 = mLog.append(LogPage::MAX_DATA_SIZE);
    EXPECT_NE(nullptr, entry3) << "Failed to allocate entry";

    auto i = mLog.begin();
    auto end = mLog.end();

    EXPECT_EQ(entry3, &(*i)) << "Iterator not pointing to second entry";
    EXPECT_NE(end, i) << "Iterator pointing to end";

    ++i;
    EXPECT_EQ(entry2, &(*i)) << "Iterator not pointing to appended entry";
    EXPECT_NE(end, i) << "Iterator pointing to end";

    ++i;
    EXPECT_EQ(entry1, &(*i)) << "Iterator not pointing to first entry";
    EXPECT_NE(end, i) << "Iterator pointing to end";

    ++i;
    EXPECT_EQ(end, i) << "Iterator not pointing to end";
}

using OrderedLogTest = BaseLogTest<OrderedLogImpl>;

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

    auto entry2 = tail->append(LogPage::MAX_DATA_SIZE);
    EXPECT_EQ(nullptr, entry2) << "Allocated entry outside page boundary";

    auto entry3 = mLog.append(LogPage::MAX_DATA_SIZE);
    EXPECT_NE(nullptr, entry3) << "Failed to allocate entry";

    EXPECT_NE(tail, mLog.head()) << "New head is the same as the old head";
    EXPECT_EQ(tail->next(), mLog.head()) << "Next pointer of old head does not point to new head";
}

/**
 * @brief Test fixture for testing prefilled Log implementation classes
 *
 * Adds 4 entries, the first two on the first page, the third entry on the second page and the fourth entry on the third
 * page.
 */
template <class Impl>
class BaseLogFilledTest : public BaseLogTest<Impl> {
protected:
    BaseLogFilledTest()
            : mEntry1(nullptr), mEntry2(nullptr), mEntry3(nullptr), mEntry4(nullptr) {
    }

    virtual void SetUp() {
        mEntry1 = BaseLogTest<Impl>::mLog.append(31);
        EXPECT_NE(nullptr, mEntry1) << "Failed to allocate entry";
        mEntry2 = BaseLogTest<Impl>::mLog.append(55);
        EXPECT_NE(nullptr, mEntry2) << "Failed to allocate entry";
        mEntry3 = BaseLogTest<Impl>::mLog.append(LogPage::MAX_DATA_SIZE);
        EXPECT_NE(nullptr, mEntry3) << "Failed to allocate entry";
        mEntry4 = BaseLogTest<Impl>::mLog.append(LogPage::MAX_DATA_SIZE);
        EXPECT_NE(nullptr, mEntry4) << "Failed to allocate entry";
    }

    LogEntry* mEntry1;
    LogEntry* mEntry2;
    LogEntry* mEntry3;
    LogEntry* mEntry4;
};

using UnorderedLogFilledTest = BaseLogFilledTest<UnorderedLogImpl>;

/**
 * @class Log
 * @test Check that iterating over all pages in the log works correctly
 */
TEST_F(UnorderedLogFilledTest, pageIterator) {
    auto i = mLog.pageBegin();
    auto end = mLog.pageEnd();

    EXPECT_EQ(mLog.head(), &(*i)) << "Iterator not pointing to first page";
    EXPECT_NE(end, i) << "Iterator pointing to end";

    ++i;
    EXPECT_EQ(mLog.head()->next().load(), &(*i)) << "Iterator not pointing to second page";
    EXPECT_NE(end, i) << "Iterator pointing to end";

    ++i;
    EXPECT_EQ(mLog.head()->next().load()->next().load(), &(*i)) << "Iterator not pointing to third page";
    EXPECT_NE(end, i) << "Iterator pointing to end";

    ++i;
    EXPECT_EQ(end, i) << "Iterator not pointing to end";
}

/**
 * @class Log
 * @test Check that iterating over the complete log works correctly
 *
 * Because we iterate pages from head to tail and inside pages from top to bottom, the order of elements should
 * be 4 - 3 - 1 - 2.
 */
TEST_F(UnorderedLogFilledTest, logIterator) {
    auto i = mLog.begin();
    auto end = mLog.end();

    EXPECT_EQ(mEntry4, &(*i)) << "Iterator not pointing to fourth entry";
    EXPECT_NE(end, i) << "Iterator pointing to end";

    ++i;
    EXPECT_EQ(mEntry3, &(*i)) << "Iterator not pointing to third entry";
    EXPECT_NE(end, i) << "Iterator pointing to end";

    ++i;
    EXPECT_EQ(mEntry1, &(*i)) << "Iterator not pointing to first entry";
    EXPECT_NE(end, i) << "Iterator pointing to end";

    ++i;
    EXPECT_EQ(mEntry2, &(*i)) << "Iterator not pointing to second entry";
    EXPECT_NE(end, i) << "Iterator pointing to end";

    ++i;
    EXPECT_EQ(end, i) << "Iterator not pointing to end";
}

/**
 * @class Log
 * @test Check that erase works correctly
 *
 * Erases the second page between head and tail.
 */
TEST_F(UnorderedLogFilledTest, erase) {
    auto head = mLog.head();
    auto tail = head->next().load()->next().load();
    mLog.erase(head, tail);

    auto i = mLog.begin();
    auto end = mLog.end();

    EXPECT_EQ(mEntry4, &(*i)) << "Iterator not pointing to fourth entry";
    EXPECT_NE(end, i) << "Iterator pointing to end";

    ++i;
    EXPECT_EQ(mEntry1, &(*i)) << "Iterator not pointing to first entry";
    EXPECT_NE(end, i) << "Iterator pointing to end";

    ++i;
    EXPECT_EQ(mEntry2, &(*i)) << "Iterator not pointing to second entry";
    EXPECT_NE(end, i) << "Iterator pointing to end";

    ++i;
    EXPECT_EQ(end, i) << "Iterator not pointing to end";
}

using OrderedLogFilledTest = BaseLogFilledTest<OrderedLogImpl>;

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
    EXPECT_EQ(tail->next().load()->next().load(), &(*i)) << "Iterator not pointing to third page";
    EXPECT_NE(end, i) << "Iterator pointing to end";

    ++i;
    EXPECT_EQ(end, i) << "Iterator not pointing to end";
}

/**
 * @class Log
 * @test Check that iterating over the complete log works correctly
 *
 * Because we iterate pages from tail to head and inside pages from top to bottom, the order of elements should
 * be 1 - 2 - 3 - 4.
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
    EXPECT_EQ(mEntry4, &(*i)) << "Iterator not pointing to fourth entry";
    EXPECT_NE(end, i) << "Iterator pointing to end";

    ++i;
    EXPECT_EQ(end, i) << "Iterator not pointing to end";
}

/**
 * @class Log
 * @test Check that erase works correctly
 *
 * Erases the second page between head and tail.
 */
TEST_F(OrderedLogFilledTest, erase) {
    mLog.erase(mLog.tail(), mLog.head());

    auto i = mLog.begin();
    auto end = mLog.end();

    EXPECT_EQ(mEntry1, &(*i)) << "Iterator not pointing to first entry";
    EXPECT_NE(end, i) << "Iterator pointing to end";

    ++i;
    EXPECT_EQ(mEntry2, &(*i)) << "Iterator not pointing to second entry";
    EXPECT_NE(end, i) << "Iterator pointing to end";

    ++i;
    EXPECT_EQ(mEntry4, &(*i)) << "Iterator not pointing to fourth entry";
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

using UnorderedLogThreadedTest = LogTestThreaded<UnorderedLogImpl>;

/**
 * @class UnorderedLogImpl
 * @test Checks if the log is written correctly if concurrent appends happen to the log and other pages
 *
 * Similar to the LogTestThreaded::append test but divides the threads into two groups: One that writes to the head
 * segment directly and one that writes into private pages, appending the pages to the log when they are full.
 */
TEST_F(UnorderedLogThreadedTest, append) {
    constexpr int headThreadCount = 2; // Number of threads appending to head - 2 Threads
    constexpr int appendThreadCount = 2; // Number of threads appending to head - 2 Threads
    constexpr uint64_t valuesCount = 10000000; // Number of entries every thread writes - 10M

    // Start threads filling the log
    auto startTime = std::chrono::steady_clock::now();
    uint64_t values = 1;

    std::array<std::thread, headThreadCount + appendThreadCount> worker;
    for (size_t i = 0; i < headThreadCount; ++i) {
        worker[i] = std::thread([valuesCount, values, this] () {
            auto end = values + valuesCount;
            for (auto i = values; i < end; ++i) {
                auto entry = this->mLog.append(sizeof(i));
                memcpy(entry->data(), &i, sizeof(i));
                entry->seal();
            }
        });
        values += valuesCount;
    }
    for (size_t i = headThreadCount; i < headThreadCount + appendThreadCount; ++i) {
        worker[i] = std::thread([valuesCount, values, this] () {
            auto page = new(this->mPageManager.alloc()) LogPage();
            auto end = values + valuesCount;
            for (auto i = values; i < end; ++i) {
                auto entry = page->append(sizeof(i));
                if (entry == nullptr) {
                    this->mLog.appendPage(page);
                    page = new(this->mPageManager.alloc()) LogPage();
                    entry = page->append(sizeof(i));
                }
                memcpy(entry->data(), &i, sizeof(i));
                entry->seal();
            }
            this->mLog.appendPage(page);
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
