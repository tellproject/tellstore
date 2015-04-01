#include <config.h>
#include <util/Log.hpp>

#include <gtest/gtest.h>

#include <array>
#include <chrono>
#include <thread>
#include <unordered_set>

using namespace tell::store;

namespace {

class LogTest : public ::testing::Test {
protected:
    LogTest()
            : mPageManager(TELL_PAGE_SIZE * 10),
              mLog(mPageManager) {
    }

    PageManager mPageManager;

    Log mLog;
};

/**
 * @class Log
 * @test Check if entries are 8 byte aligned
 */
TEST_F(LogTest, entryAlignment) {
    auto entry = mLog.append(31);
    EXPECT_EQ(40, entry->size()) << "Entry is not 8 byte aligned";
}

/**
 * @class Log
 * @test Test sealing of entries
 */
TEST_F(LogTest, sealEntry) {
    auto entry = mLog.append(31);
    EXPECT_FALSE(entry->sealed()) << "Newly created entry is sealed";

    entry->seal();
    EXPECT_TRUE(entry->sealed()) << "Sealed entry is not sealed";
}

/**
 * @class Log
 * @test Test sealing of pages
 */
TEST_F(LogTest, sealPage) {
    auto page = mLog.head();
    EXPECT_FALSE(page->sealed()) << "Newly created page is sealed";

    page->seal();
    EXPECT_TRUE(page->sealed()) << "Sealed page is not sealed";
}

/**
 * @class Log
 * @test Check that no more writes are allowed to a sealed page
 */
TEST_F(LogTest, appendSealedPage) {
    auto page = mLog.head();
    page->seal();

    auto entry = page->append(31);
    EXPECT_EQ(nullptr, entry) << "No entries should be appended to sealed page";
}

/**
 * @class Log
 * @test Check that appends work correctly
 *
 * Allocates 3 entries: One small on head page, then a large one that does not fit directly on the head page, then a
 * second large one that triggers allocation of a new head segment.
 */
TEST_F(LogTest, append) {
    auto head = mLog.head();

    auto entry1 = mLog.append(31);
    EXPECT_NE(nullptr, entry1) << "Failed to allocate entry";

    auto entry2 = head->append(Log::MAX_SIZE - 4);
    EXPECT_EQ(nullptr, entry2) << "Allocated entry outside page boundary";

    auto entry3 = mLog.append(Log::MAX_SIZE - 4);
    EXPECT_NE(nullptr, entry3) << "Failed to allocate entry";

    EXPECT_NE(head, mLog.head()) << "New head is the same as the old head";
    EXPECT_EQ(head, mLog.head()->previous()) << "Previous pointer of head does not point to old head";
}

/**
 * @class Log
 * @test Check that iterating over all entries in a page works correctly
 */
TEST_F(LogTest, entryIterator) {
    auto head = mLog.head();

    auto entry1 = head->append(31);
    EXPECT_NE(nullptr, entry1) << "Failed to allocate entry";
    auto entry2 = head->append(55);
    EXPECT_NE(nullptr, entry2) << "Failed to allocate entry";

    auto i = head->begin();
    auto end = head->end();

    EXPECT_EQ(entry1, &(*i)) << "Iterator not pointing to first entry";
    EXPECT_NE(end, i) << "Iterator pointing to end";

    ++i;
    EXPECT_EQ(entry2, &(*i)) << "Iterator not pointing to second entry";
    EXPECT_NE(end, i) << "Iterator pointing to end";

    ++i;
    EXPECT_EQ(end, i) << "Iterator not pointing to end";
}

/**
 * @class Log
 * @test Check that iterating over all pages in the log works correctly
 */
TEST_F(LogTest, pageIterator) {
    auto entry1 = mLog.append(31);
    EXPECT_NE(nullptr, entry1) << "Failed to allocate entry";
    auto entry2 = mLog.append(Log::MAX_SIZE - 4);
    EXPECT_NE(nullptr, entry2) << "Failed to allocate entry";

    EXPECT_NE(nullptr, mLog.head()->previous()) << "Log contains only one page";

    auto i = mLog.pageBegin();
    auto end = mLog.pageEnd();

    EXPECT_EQ(mLog.head(), &(*i)) << "Iterator not pointing to first page";
    EXPECT_NE(end, i) << "Iterator pointing to end";

    ++i;
    EXPECT_EQ(mLog.head()->previous(), &(*i)) << "Iterator not pointing to second page";
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
TEST_F(LogTest, logIterator) {
    auto entry1 = mLog.append(31);
    EXPECT_NE(nullptr, entry1) << "Failed to allocate entry";
    auto entry2 = mLog.append(55);
    EXPECT_NE(nullptr, entry2) << "Failed to allocate entry";
    auto entry3 = mLog.append(Log::MAX_SIZE - 4);
    EXPECT_NE(nullptr, entry3) << "Failed to allocate entry";

    EXPECT_NE(nullptr, mLog.head()->previous()) << "Log contains only one page";


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
TEST(LogTestThreaded, append) {
    constexpr int threadCount = 4; // Number of threads running - 4 Threads
    constexpr uint64_t valuesCount = 10000000; // Number of entries every thread writes - 10M

    constexpr int pageCount = 100; // Number of pages to reserve in the page manager

    PageManager pageManager(TELL_PAGE_SIZE * pageCount);
    Log log(pageManager);

    // Start threads filling the log
    auto startTime = std::chrono::steady_clock::now();
    uint64_t values = 1;
    std::array<std::thread, threadCount> worker;
    for (auto& t : worker) {
        t = std::thread([valuesCount, values, &log] () {
            auto end = values + valuesCount;
            for (auto i = values; i < end; ++i) {
                auto entry = log.append(sizeof(i));
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
    auto end = log.end();
    for (auto i = log.begin(); i != end; ++i) {
        EXPECT_TRUE(i->sealed()) << "Entry is not sealed";
        uint64_t value = *reinterpret_cast<uint64_t*>(i->data());

        auto j = valueSet.find(value);
        EXPECT_NE(j, valueSet.end()) << "Value from entry is not in values set";
        valueSet.erase(j);
    }
    EXPECT_TRUE(valueSet.empty()) << "Values set is not empty";
}

}
