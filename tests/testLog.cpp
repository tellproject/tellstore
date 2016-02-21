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
#include <util/Log.hpp>
#include <util/PageManager.hpp>

#include <gtest/gtest.h>

#include <boost/dynamic_bitset.hpp>

#include <array>
#include <chrono>
#include <thread>

using namespace tell::store;

namespace {

class TestBase : public ::testing::Test {
protected:
    TestBase(size_t pageCount)
            : mPageManager(pageCount * TELL_PAGE_SIZE) {
    }

    PageManager mPageManager;
};

class LogPageTest : public TestBase {
protected:
    LogPageTest()
            : TestBase(1),
              mPage(nullptr) {
    }

    virtual void SetUp() {
        mPage = new(mPageManager.alloc()) LogPage();
    }

    virtual void TearDown() {
        mPageManager.free(mPage);
    }

    LogPage* mPage;
};

/**
 * @class LogPage
 * @test Check if entries return the correct size
 */
TEST_F(LogPageTest, entrySize) {
    auto entry = mPage->append(31);
    EXPECT_EQ(31u, entry->size()) << "Size is not the same as in append";
    EXPECT_EQ(48u, entry->entrySize()) << "Entry size is not 16 byte padded";
    EXPECT_EQ(0u, (reinterpret_cast<uintptr_t>(entry->data()) % 16)) << "Entry data is not 16 byte aligned";
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
    EXPECT_EQ(31u, entry->size()) << "Size is not the same as in append";
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
    EXPECT_TRUE(end != i) << "Iterator pointing to end";

    ++i;
    EXPECT_EQ(entry2, &(*i)) << "Iterator not pointing to second entry";
    EXPECT_TRUE(end != i) << "Iterator pointing to end";

    ++i;
    EXPECT_TRUE(end == i) << "Iterator not pointing to end";
}

/**
 * @brief Test fixture for testing Log implementation classes
 */
template <class Impl>
class BaseLogTest : public TestBase {
protected:
    BaseLogTest()
            : TestBase(10),
              mLog(mPageManager) {
    }

    Log<Impl> mLog;
};

using BaseLogTestImplementations = ::testing::Types<UnorderedLogImpl, OrderedLogImpl>;
TYPED_TEST_CASE(BaseLogTest, BaseLogTestImplementations);

/**
 * @class Log
 * @test Check that begin() == end() when the log is empty
 */
TYPED_TEST(BaseLogTest, emptyLogIteration) {
    auto begin = this->mLog.begin();
    auto end = this->mLog.end();
    EXPECT_TRUE(begin == end) << "begin() == end() iterator in empty log";
}

/**
 * @class Log
 * @test Check that the iterator is valid when an append on the same page happened in the meantime
 *
 * Allocates 1 entry, retrieves the iterator, increments it, inserts another element and checks if the iterator still
 * points to the end element (not the second inserted element).
 */
TYPED_TEST(BaseLogTest, logIteratorAppendSamePage) {
    auto entry1 = this->mLog.append(31);
    ASSERT_NE(nullptr, entry1) << "Failed to allocate entry";
    this->mLog.seal(entry1);
    EXPECT_TRUE(entry1->sealed()) << "Failed to seal element";

    auto i = this->mLog.begin();
    auto end = this->mLog.end();

    EXPECT_EQ(entry1, &(*i)) << "Iterator not pointing to first entry";
    EXPECT_TRUE(end != i) << "Iterator pointing to end";

    ++i;
    EXPECT_TRUE(end == i) << "Iterator not pointing to end";

    auto entry2 = this->mLog.append(31);
    ASSERT_NE(nullptr, entry2) << "Failed to allocate entry";
    this->mLog.seal(entry2);
    EXPECT_TRUE(entry2->sealed()) << "Failed to seal element";

    EXPECT_TRUE(end == i) << "Iterator not pointing to end";
}

/**
 * @class Log
 * @test Check that the iterator is valid when an append on the same page happened in the meantime
 *
 * Allocates 1 entry, retrieves the iterator, increments it, inserts another element (which lands on a new page) and
 * checks if the iterator still points to the end element (not the second inserted element).
 */
TYPED_TEST(BaseLogTest, logIteratorAppendNewPage) {
    auto entry1 = this->mLog.append(31);
    ASSERT_NE(nullptr, entry1) << "Failed to allocate entry";
    this->mLog.seal(entry1);
    EXPECT_TRUE(entry1->sealed()) << "Failed to seal element";

    auto i = this->mLog.begin();
    auto end = this->mLog.end();

    EXPECT_EQ(entry1, &(*i)) << "Iterator not pointing to first entry";
    EXPECT_TRUE(end != i) << "Iterator pointing to end";

    ++i;
    EXPECT_TRUE(end == i) << "Iterator not pointing to end";

    auto entry2 = this->mLog.append(LogPage::MAX_DATA_SIZE);
    ASSERT_NE(nullptr, entry2) << "Failed to allocate entry";
    this->mLog.seal(entry2);
    EXPECT_TRUE(entry2->sealed()) << "Failed to seal element";

    EXPECT_TRUE(end == i) << "Iterator not pointing to end";
}

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

    EXPECT_EQ(3u, mLog.pages()) << "Page count not incremented";
    EXPECT_NE(page1, mLog.head()) << "Head pointing to appended page";
    EXPECT_NE(page2, mLog.head()) << "Head pointing to appended page";

    auto i = mLog.pageBegin();
    auto end = mLog.pageEnd();

    EXPECT_EQ(page2, &(*i)) << "Iterator not pointing to appended head page";
    EXPECT_TRUE(end != i) << "Iterator pointing to end";

    ++i;
    EXPECT_EQ(page1, &(*i)) << "Iterator not pointing to second appended page";
    EXPECT_TRUE(end != i) << "Iterator pointing to end";

    ++i;
    EXPECT_EQ(mLog.head(), &(*i)) << "Iterator not pointing to head page";
    EXPECT_TRUE(end != i) << "Iterator pointing to end";

    ++i;
    EXPECT_TRUE(end == i) << "Iterator not pointing to end";
}

/**
 * @class UnorderedLogImpl
 * @test Check that appending pages multiple times works
 */
TEST_F(UnorderedLogTest, appendMultiplePage) {
    auto page1 = new(mPageManager.alloc()) LogPage();
    mLog.appendPage(page1);
    EXPECT_EQ(2u, mLog.pages()) << "Page count not incremented";

    auto page2 = new(mPageManager.alloc()) LogPage();
    mLog.appendPage(page2);
    EXPECT_EQ(3u, mLog.pages()) << "Page count not incremented";

    auto i = mLog.pageBegin();
    auto end = mLog.pageEnd();

    EXPECT_EQ(page2, &(*i)) << "Iterator not pointing to second appended head page";
    EXPECT_TRUE(end != i) << "Iterator pointing to end";

    ++i;
    EXPECT_EQ(page1, &(*i)) << "Iterator not pointing to first appended head page";
    EXPECT_TRUE(end != i) << "Iterator pointing to end";

    ++i;
    EXPECT_EQ(mLog.head(), &(*i)) << "Iterator not pointing to head page";
    EXPECT_TRUE(end != i) << "Iterator pointing to end";

    ++i;
    EXPECT_TRUE(end == i) << "Iterator not pointing to end";
}

/**
 * @class UnorderedLogImpl
 * @test Check that elements are written to head segment when appending
 */
TEST_F(UnorderedLogTest, appendPageWriteToHead) {
    auto entry1 = mLog.append(31);
    ASSERT_NE(nullptr, entry1) << "Failed to allocate entry";
    mLog.seal(entry1);
    EXPECT_TRUE(entry1->sealed()) << "Failed to seal element";

    auto page1 = new(mPageManager.alloc()) LogPage();
    auto entry2 = page1->append(16);
    ASSERT_NE(nullptr, entry2) << "Failed to allocate entry";
    mLog.seal(entry2);
    EXPECT_TRUE(entry2->sealed()) << "Failed to seal element";

    mLog.appendPage(page1);

    auto entry3 = mLog.append(16);
    ASSERT_NE(nullptr, entry3) << "Failed to allocate entry";
    mLog.seal(entry3);
    EXPECT_TRUE(entry3->sealed()) << "Failed to seal element";

    auto i = mLog.begin();
    auto end = mLog.end();

    EXPECT_EQ(entry2, &(*i)) << "Iterator not pointing to appended entry";
    EXPECT_TRUE(end != i) << "Iterator pointing to end";

    ++i;
    EXPECT_EQ(entry1, &(*i)) << "Iterator not pointing to first entry";
    EXPECT_TRUE(end != i) << "Iterator pointing to end";

    ++i;
    EXPECT_EQ(entry3, &(*i)) << "Iterator not pointing to second entry";
    EXPECT_TRUE(end != i) << "Iterator pointing to end";

    ++i;
    EXPECT_TRUE(end == i) << "Iterator not pointing to end";
}

/**
 * @class UnorderedLogImpl
 * @test Check that a new head pages is allocated after the appended pages
 */
TEST_F(UnorderedLogTest, appendPageNewHead) {
    auto entry1 = mLog.append(31);
    ASSERT_NE(nullptr, entry1) << "Failed to allocate entry";
    mLog.seal(entry1);
    EXPECT_TRUE(entry1->sealed()) << "Failed to seal element";

    auto page1 = new(mPageManager.alloc()) LogPage();
    auto entry2 = page1->append(16);
    ASSERT_NE(nullptr, entry2) << "Failed to allocate entry";
    mLog.seal(entry2);
    EXPECT_TRUE(entry2->sealed()) << "Failed to seal element";

    mLog.appendPage(page1);

    auto entry3 = mLog.append(LogPage::MAX_DATA_SIZE);
    ASSERT_NE(nullptr, entry3) << "Failed to allocate entry";
    mLog.seal(entry3);
    EXPECT_TRUE(entry3->sealed()) << "Failed to seal element";

    auto i = mLog.begin();
    auto end = mLog.end();

    EXPECT_EQ(entry3, &(*i)) << "Iterator not pointing to second entry";
    EXPECT_TRUE(end != i) << "Iterator pointing to end";

    ++i;
    EXPECT_EQ(entry2, &(*i)) << "Iterator not pointing to appended entry";
    EXPECT_TRUE(end != i) << "Iterator pointing to end";

    ++i;
    EXPECT_EQ(entry1, &(*i)) << "Iterator not pointing to first entry";
    EXPECT_TRUE(end != i) << "Iterator pointing to end";

    ++i;
    EXPECT_TRUE(end == i) << "Iterator not pointing to end";
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
    ASSERT_NE(nullptr, entry1) << "Failed to allocate entry";
    mLog.seal(entry1);
    EXPECT_TRUE(entry1->sealed()) << "Failed to seal element";

    auto entry2 = tail->append(LogPage::MAX_DATA_SIZE);
    EXPECT_EQ(nullptr, entry2) << "Allocated entry outside page boundary";

    auto entry3 = mLog.append(LogPage::MAX_DATA_SIZE);
    ASSERT_NE(nullptr, entry3) << "Failed to allocate entry";
    mLog.seal(entry3);
    EXPECT_TRUE(entry3->sealed()) << "Failed to seal element";

    EXPECT_NE(tail, mLog.head()) << "New head is the same as the old head";
    EXPECT_EQ(tail->next(), mLog.head()) << "Next pointer of old head does not point to new head";
}

/**
 * @class Log
 * @test Check that appends work correctly
 *
 * Allocates 3 entries: One small on head page, then a large one that does not fit directly on the head page, then a
 * second large one that triggers allocation of a new head segment.
 */
TEST_F(OrderedLogTest, sealedLogIterator) {
    // Insert first element
    auto entry1 = mLog.append(31);
    ASSERT_NE(nullptr, entry1) << "Failed to allocate entry";
    EXPECT_EQ(entry1, &(*mLog.sealedEnd())) << "Sealed End does not point to first element";

    // Insert second element on same page
    auto entry2 = mLog.append(55);
    ASSERT_NE(nullptr, entry2) << "Failed to allocate entry";
    EXPECT_EQ(entry1, &(*mLog.sealedEnd())) << "Sealed End does not point to first element";

    // Seal second element, sealed end must still point to beginning
    mLog.seal(entry2);
    EXPECT_TRUE(entry2->sealed()) << "Failed to seal element";
    EXPECT_EQ(entry1, &(*mLog.sealedEnd())) << "Sealed End does not point to first element";

    // Insert third element on next page
    auto entry3 = mLog.append(LogPage::MAX_DATA_SIZE);
    ASSERT_NE(nullptr, entry3) << "Failed to allocate entry";
    EXPECT_EQ(entry1, &(*mLog.sealedEnd())) << "Sealed End does not point to first element";

    // Seal first entry, sealed end must point to third element
    mLog.seal(entry1);
    EXPECT_TRUE(entry1->sealed()) << "Failed to seal element";
    EXPECT_EQ(entry3, &(*mLog.sealedEnd())) << "Sealed End does not point to third element";

    // Seal third entry, sealed end must point past third element
    mLog.seal(entry3);
    EXPECT_TRUE(entry3->sealed()) << "Failed to seal element";
    EXPECT_EQ(mLog.end(), mLog.sealedEnd()) << "Sealed End does not point to end";
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
        ASSERT_NE(nullptr, mEntry1) << "Failed to allocate entry";
        BaseLogTest<Impl>::mLog.seal(mEntry1);
        EXPECT_TRUE(mEntry1->sealed()) << "Failed to seal element";

        mEntry2 = BaseLogTest<Impl>::mLog.append(55);
        ASSERT_NE(nullptr, mEntry2) << "Failed to allocate entry";
        BaseLogTest<Impl>::mLog.seal(mEntry2);
        EXPECT_TRUE(mEntry2->sealed()) << "Failed to seal element";

        mEntry3 = BaseLogTest<Impl>::mLog.append(LogPage::MAX_DATA_SIZE);
        ASSERT_NE(nullptr, mEntry3) << "Failed to allocate entry";
        BaseLogTest<Impl>::mLog.seal(mEntry3);
        EXPECT_TRUE(mEntry3->sealed()) << "Failed to seal element";

        mEntry4 = BaseLogTest<Impl>::mLog.append(LogPage::MAX_DATA_SIZE);
        ASSERT_NE(nullptr, mEntry4) << "Failed to allocate entry";
        BaseLogTest<Impl>::mLog.seal(mEntry4);
        EXPECT_TRUE(mEntry4->sealed()) << "Failed to seal element";
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
    EXPECT_TRUE(end != i) << "Iterator pointing to end";

    ++i;
    EXPECT_EQ(mLog.head()->next().load(), &(*i)) << "Iterator not pointing to second page";
    EXPECT_TRUE(end != i) << "Iterator pointing to end";

    ++i;
    EXPECT_EQ(mLog.head()->next().load()->next().load(), &(*i)) << "Iterator not pointing to third page";
    EXPECT_TRUE(end != i) << "Iterator pointing to end";

    ++i;
    EXPECT_TRUE(end == i) << "Iterator not pointing to end";
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
    EXPECT_TRUE(end != i) << "Iterator pointing to end";

    ++i;
    EXPECT_EQ(mEntry3, &(*i)) << "Iterator not pointing to third entry";
    EXPECT_TRUE(end != i) << "Iterator pointing to end";

    ++i;
    EXPECT_EQ(mEntry1, &(*i)) << "Iterator not pointing to first entry";
    EXPECT_TRUE(end != i) << "Iterator pointing to end";

    ++i;
    EXPECT_EQ(mEntry2, &(*i)) << "Iterator not pointing to second entry";
    EXPECT_TRUE(end != i) << "Iterator pointing to end";

    ++i;
    EXPECT_TRUE(end == i) << "Iterator not pointing to end";
}

/**
 * @class Log
 * @test Check that erase works correctly
 *
 * Erases the second page between head and tail.
 */
TEST_F(UnorderedLogFilledTest, erase) {
    std::vector<void*> obsoletePages;

    auto head = mLog.head();
    auto tail = head->next().load()->next().load();
    mLog.erase(head, tail, obsoletePages);

    EXPECT_EQ(1u, obsoletePages.size()) << "Number of obsolete pages must be 1";
    mPageManager.free(obsoletePages);

    auto i = mLog.begin();
    auto end = mLog.end();

    EXPECT_EQ(mEntry4, &(*i)) << "Iterator not pointing to fourth entry";
    EXPECT_TRUE(end != i) << "Iterator pointing to end";

    ++i;
    EXPECT_EQ(mEntry1, &(*i)) << "Iterator not pointing to first entry";
    EXPECT_TRUE(end != i) << "Iterator pointing to end";

    ++i;
    EXPECT_EQ(mEntry2, &(*i)) << "Iterator not pointing to second entry";
    EXPECT_TRUE(end != i) << "Iterator pointing to end";

    ++i;
    EXPECT_TRUE(end == i) << "Iterator not pointing to end";
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
    EXPECT_TRUE(end != i) << "Iterator pointing to end";

    ++i;
    EXPECT_EQ(tail->next().load(), &(*i)) << "Iterator not pointing to second page";
    EXPECT_TRUE(end != i) << "Iterator pointing to end";

    ++i;
    EXPECT_EQ(tail->next().load()->next().load(), &(*i)) << "Iterator not pointing to third page";
    EXPECT_TRUE(end != i) << "Iterator pointing to end";

    ++i;
    EXPECT_TRUE(end == i) << "Iterator not pointing to end";
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
    EXPECT_TRUE(end != i) << "Iterator pointing to end";

    ++i;
    EXPECT_EQ(mEntry2, &(*i)) << "Iterator not pointing to second entry";
    EXPECT_TRUE(end != i) << "Iterator pointing to end";

    ++i;
    EXPECT_EQ(mEntry3, &(*i)) << "Iterator not pointing to third entry";
    EXPECT_TRUE(end != i) << "Iterator pointing to end";

    ++i;
    EXPECT_EQ(mEntry4, &(*i)) << "Iterator not pointing to fourth entry";
    EXPECT_TRUE(end != i) << "Iterator pointing to end";

    ++i;
    EXPECT_TRUE(end == i) << "Iterator not pointing to end";
}

/**
 * @class Log
 * @test Check that truncation works correctly when truncating over a page
 */
TEST_F(OrderedLogFilledTest, truncateLogDifferentPage) {
    std::vector<void*> obsoletePages;
    auto tail = mLog.tail();
    auto begin = mLog.begin();

    // Advance iterator to second page
    auto i = begin;
    EXPECT_EQ(mEntry1, &(*i)) << "Iterator not pointing to first entry";

    ++i;
    EXPECT_EQ(mEntry2, &(*i)) << "Iterator not pointing to second entry";

    ++i;
    EXPECT_EQ(mEntry3, &(*i)) << "Iterator not pointing to third entry";

    EXPECT_TRUE(mLog.truncateLog(begin, i, obsoletePages));
    EXPECT_EQ(mLog.tail(), tail->next()) << "New tail not pointing to next page";

    EXPECT_EQ(1u, obsoletePages.size()) << "Number of obsolete pages must be 1";
    mPageManager.free(obsoletePages);

    auto j = mLog.begin();
    EXPECT_EQ(mEntry3, &(*j)) << "Iterator not pointing to third entry";
}

/**
 * @class Log
 * @test Check that truncation works correctly on the same page
 */
TEST_F(OrderedLogFilledTest, truncateLogSamePage) {
    std::vector<void*> obsoletePages;
    auto tail = mLog.tail();
    auto begin = mLog.begin();

    // Advance iterator to second page
    auto i = begin;
    EXPECT_EQ(mEntry1, &(*i)) << "Iterator not pointing to first entry";

    ++i;
    EXPECT_EQ(mEntry2, &(*i)) << "Iterator not pointing to second entry";

    EXPECT_TRUE(mLog.truncateLog(begin, i, obsoletePages));
    EXPECT_EQ(mLog.tail(), tail) << "New tail not pointing to same page";
    EXPECT_TRUE(obsoletePages.empty()) << "No obsolete page";

    auto j = mLog.begin();
    EXPECT_EQ(mEntry2, &(*j)) << "Iterator not pointing to second entry";
}

/**
 * @class Log
 * @test Check that truncation only succeeds if the tail has not changed in the meantime
 */
TEST_F(OrderedLogFilledTest, truncateLogInvalidTail) {
    std::vector<void*> obsoletePages;
    auto tail = mLog.tail();
    auto begin = mLog.begin();

    // Advance iterator to second page
    auto i = begin;
    ++i; ++i;

    EXPECT_TRUE(mLog.truncateLog(begin, i, obsoletePages));
    EXPECT_EQ(mLog.tail(), tail->next()) << "Iterator not pointing to end";
    mPageManager.free(obsoletePages);
    obsoletePages.clear();

    // Advance iterator to same page
    auto j = begin;
    j++;
    EXPECT_FALSE(mLog.truncateLog(begin, j, obsoletePages));
    EXPECT_TRUE(obsoletePages.empty()) << "Failed truncate must not have obsolete pages";
}

template <typename Impl>
class LogTestThreaded : public TestBase {
protected:
    static constexpr int pageCount = 400; // Number of pages to reserve in the page manager - 400

    LogTestThreaded()
            : TestBase(pageCount),
              mLog(mPageManager) {
    }

    void assertWritten(size_t count) {
        // Iterate over log and add the value in the entry to the expected values set
        boost::dynamic_bitset<> valueSet(count);
        auto end = mLog.end();
        for (auto i = mLog.begin(); i != end; ++i) {
            EXPECT_TRUE(i->sealed()) << "Entry is not sealed";
            uint64_t value = *reinterpret_cast<uint64_t*>(i->data());

            EXPECT_FALSE(valueSet.test(value)) << "Duplicate value " << value << " encountered";
            valueSet.set(value);
        }
        EXPECT_EQ(count, valueSet.count()) << "Not all values encountered";
    }

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
TYPED_TEST(LogTestThreaded, DISABLED_append) {
    static constexpr int threadCount = 4; // Number of threads running - 4 Threads
    static constexpr uint64_t valuesCount = 10000000; // Number of entries every thread writes - 10M
    static constexpr uint64_t totalCount = valuesCount * threadCount;

    // Start threads filling the log
    auto startTime = std::chrono::steady_clock::now();
    uint64_t values = 0;

    std::array<std::thread, threadCount> worker;
    for (auto& t : worker) {
        t = std::thread([values, this] () {
            auto end = values + valuesCount;
            for (auto i = values; i < end; ++i) {
                auto entry = this->mLog.append(sizeof(i));
                memcpy(entry->data(), &i, sizeof(i));
                this->mLog.seal(entry);
            }
        });
        values += valuesCount;
    }
    for (auto& t : worker) {
        t.join();
    }
    auto endTime = std::chrono::steady_clock::now();

    auto diff = endTime - startTime;
    std::cout << "Wrote " << values << " entries in " <<
            std::chrono::duration_cast<std::chrono::milliseconds>(diff).count() << "ms" << std::endl;

    this->assertWritten(totalCount);
}

using UnorderedLogThreadedTest = LogTestThreaded<UnorderedLogImpl>;

/**
 * @class UnorderedLogImpl
 * @test Checks if the log is written correctly if concurrent appends happen to the log and other pages
 *
 * Similar to the LogTestThreaded::append test but divides the threads into two groups: One that writes to the head
 * segment directly and one that writes into private pages, appending the pages to the log when they are full.
 */
TEST_F(UnorderedLogThreadedTest, DISABLED_append) {
    static constexpr int headThreadCount = 2; // Number of threads appending to head - 2 Threads
    static constexpr int appendThreadCount = 2; // Number of threads appending to head - 2 Threads
    static constexpr uint64_t valuesCount = 10000000; // Number of entries every thread writes - 10M
    static constexpr uint64_t totalCount = valuesCount * (headThreadCount + appendThreadCount);

    // Start threads filling the log
    auto startTime = std::chrono::steady_clock::now();
    uint64_t values = 0;

    std::array<std::thread, headThreadCount + appendThreadCount> worker;
    for (size_t i = 0; i < headThreadCount; ++i) {
        worker[i] = std::thread([values, this] () {
            auto end = values + valuesCount;
            for (auto i = values; i < end; ++i) {
                auto entry = this->mLog.append(sizeof(i));
                memcpy(entry->data(), &i, sizeof(i));
                this->mLog.seal(entry);
            }
        });
        values += valuesCount;
    }
    for (size_t i = headThreadCount; i < headThreadCount + appendThreadCount; ++i) {
        worker[i] = std::thread([values, this] () {
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
                this->mLog.seal(entry);
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
    std::cout << "Wrote " << values << " entries in "
              << std::chrono::duration_cast<std::chrono::milliseconds>(diff).count() << "ms" << std::endl;

    this->assertWritten(totalCount);
}

}
