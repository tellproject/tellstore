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

#pragma once

#include <config.h>
#include <deltamain/Record.hpp>

#include <atomic>
#include <cstdint>
#include <cstddef>
#include <vector>

namespace tell {
namespace store {

class Modifier;
class PageManager;
class Record;

namespace deltamain {

class ColumnMapContext;

/**
 * @brief Struct storing information about a single element in the column map page
 */
struct alignas(8) ColumnMapMainEntry {
public:
    ColumnMapMainEntry(uint64_t _key, uint64_t _version)
            : key(_key),
              version(_version),
              newest(0x0u) {
    }

    const uint64_t key;
    const uint64_t version;
    std::atomic<uintptr_t> newest;
};

/**
 * @brief Struct in the column map page storing the offset and the 4 byte prefix of a single variable sized field
 */
struct alignas(8) ColumnMapHeapEntry {
    ColumnMapHeapEntry(uint32_t _offset)
        : offset(_offset),
          prefix{} {
    }

    ColumnMapHeapEntry(uint32_t _offset, const char* data);

    ColumnMapHeapEntry(uint32_t offset, uint32_t size, const char* data);

    /// Offset from the end of the variable sized heap to the field value
    const uint32_t offset;

    /// First 4 bytes of the field value
    char prefix[4];
};

/**
 * @brief Struct storing the header of a column map page
 *
 * The data in a column map is layed out in the following way:
 * - Count: The number N of elements stored in this page
 * - Entries: An array of size N containing the key, version and newest pointer for every element stored in the page.
 *     All versions of a single key are clustered together in descending order (i.e. newest version first). The newest
 *     pointer is either null, points to the newest entry in the update log not contained in the page or points to the
 *     highest version of the key in another page if it was garbage collected. Only the pointer of the newest version of
 *     a key is used.
 * - Sizes: An array of size N containing the complete sizes of the elements stored in the page
 * - Record data: The actual data of all entries stored in the page consisting of an array of arrays of size N.
 * --- Header: The first array contains the header (null bitmap) for every element stored in the page iff the schema
 *       requires a header. All headers are 8 byte padded like in the final record layout.
 * --- Fixed size fields: One array for every fixed size column in the schema storing the data associated with the field
 *       of every record.
 * --- Variable size fields: One array for every variable size field in the schema containing the offset into the
 *       variable size heap and the 4 byte prefix of the value of every element stored in the page. As the heap grows
 *       backwards the offset is always calculated from the end of the heap.
 * - Variable size heap: Heap containing all the variable sized data of the elements stored in the page. The data of an
 *     element is stored in a single contigous memory block in the heap (no split into columns). The heap grows from the
 *     end of the page and the data must be inserted with increasing offset so that the variable size data for the first
 *     element is stored right before the end of the variable size heap.
 */
struct alignas(8) ColumnMapMainPage {
    ColumnMapMainPage()
            : count(0u),
              headerOffset(0u),
              fixedOffset(0u),
              variableOffset(0u) {
    }

    ColumnMapMainPage(const ColumnMapContext& context, uint32_t _count);

    /**
     * @brief Pointer to the array holding the entries stored in this page
     */
    const ColumnMapMainEntry* entryData() const {
        return reinterpret_cast<const ColumnMapMainEntry*>(data());
    }

    ColumnMapMainEntry* entryData() {
        return const_cast<ColumnMapMainEntry*>(const_cast<const ColumnMapMainPage*>(this)->entryData());
    }

    /**
     * @brief Pointer to the array holding the size of the elements stored in this page
     */
    const uint32_t* sizeData() const {
        return reinterpret_cast<const uint32_t*>(data() + count * sizeof(ColumnMapMainEntry));
    }

    uint32_t* sizeData() {
        return const_cast<uint32_t*>(const_cast<const ColumnMapMainPage*>(this)->sizeData());
    }

    /**
     * @brief Pointer to the beginning of the section where headers are stored
     */
    const char* headerData() const {
        return reinterpret_cast<const char*>(this) + headerOffset;
    }

    char* headerData() {
        return const_cast<char*>(const_cast<const ColumnMapMainPage*>(this)->headerData());
    }

    /**
     * @brief Pointer to the beginning of the section where fixed size fields are stored
     */
    const char* fixedData() const {
        return reinterpret_cast<const char*>(this) + fixedOffset;
    }

    char* fixedData() {
        return const_cast<char*>(const_cast<const ColumnMapMainPage*>(this)->fixedData());
    }

    /**
     * @brief Pointer to the beginning of the section where variable size fields are stored
     */
    const ColumnMapHeapEntry* variableData() const {
        return reinterpret_cast<const ColumnMapHeapEntry*>(reinterpret_cast<const char*>(this) + variableOffset);
    }

    ColumnMapHeapEntry* variableData() {
        return const_cast<ColumnMapHeapEntry*>(const_cast<const ColumnMapMainPage*>(this)->variableData());
    }

    /**
     * @brief Pointer to the variable size heap stored in this page
     *
     * The heap grows backwards, i.e. the pointer points to the end of the heap.
     */
    const char* heapData() const {
        return reinterpret_cast<const char*>(this) + TELL_PAGE_SIZE;
    }

    char* heapData() {
        return const_cast<char*>(const_cast<const ColumnMapMainPage*>(this)->heapData());
    }

    /**
     * @brief Pointer to the complete data region (entries, sizes and record data) of this page
     */
    const char* data() const {
        return reinterpret_cast<const char*>(this) + sizeof(ColumnMapMainPage);
    }

    char* data() {
        return const_cast<char*>(const_cast<const ColumnMapMainPage*>(this)->data());
    }

    /// The number of elements stored in this page
    uint32_t count;

    uint32_t headerOffset;

    uint32_t fixedOffset;

    uint32_t variableOffset;
};

/**
 * @brief Garbage collector for the column map implementation
 *
 * While cleaning a column map page the old page is first analyzed and the actions required to clean the page are
 * generated. These actions contain the pointer to the page, the start and end index of the elements to copy from the
 * page and the adjustment to the variable sized heap offset in case the offset of the variable sized fields differs
 * from the old page. In this stage only the ColumnMapMainEntry structs and the variable sized heaps are written to the
 * final fill page.
 *
 * Any pending updates and inserts of new elements are first transformed into the column map format and written to a
 * separate update page. The update page has the same layout as a normal column map page except that it has no space
 * allocated for the variable sized heap. After transferring a record into the update page the corresponding action is
 * generated and enqueued. As before only the ColumnMapMainEntry structs and the variable sized heaps are written to the
 * final fill page.
 *
 * While generating the clean actions the algorithm tries to minimize the number of required actions by combining
 * consecutive ranges into a single action.
 *
 * The final fill page is written when the size of the elements to write exceeds the capacity of a page: For each of the
 * data fields the clean actions are executed by copying the data from the old page to the new page. Afterwards the
 * newest pointers of the copied elements are adjusted to point to the newly written elements. Finally a new fill page
 * is allocated and the update page is zeroed. If a page is flushed before being able to write all versions of the same
 * key into the fill page the partially written elements are removed from the fill page and inserted in the new fill
 * page.
 */
class ColumnMapPageModifier {
public:
    ColumnMapPageModifier(const ColumnMapContext& context, PageManager& pageManager, Modifier& mainTableModifier,
            uint64_t minVersion);

    /**
     * @brief Rewrite and clean the page from garbage
     *
     * @param page The page to clean
     * @return True if the page was rewritten and cleaned or false if the page does not need cleaning
     */
    bool clean(ColumnMapMainPage* page);

    /**
     * @brief Appends the new record into the main
     *
     * @param oldRecord The record to insert
     * @return True if the record was successfully inserted into the main or false if it can be discarded
     */
    bool append(InsertRecord& oldRecord);

    /**
     * @brief Completes the garbage collection process
     */
    std::vector<ColumnMapMainPage*> done();

private:
    /**
     * @brief Helper struct recording a range of elements to copy into the new main page during garbage collection
     */
    struct CleanAction {
        CleanAction(ColumnMapMainPage* _page, uint32_t _startIdx, uint32_t _endIdx, int32_t _offsetCorrection)
                : page(_page),
                  startIdx(_startIdx),
                  endIdx(_endIdx),
                  offsetCorrection(_offsetCorrection) {
        }

        /// Pointer to the source page
        ColumnMapMainPage* page;

        /// Index of the first element to copy from the source page
        uint32_t startIdx;

        /// Index of the element past the last element to copy from the source page
        uint32_t endIdx;

        /// Difference between the offset into the variable sized heap of the old page and the new page
        int32_t offsetCorrection;
    };

    /**
     * @brief Helper struct recording one required change to the newest pointer during garbage collection
     */
    struct NewestPointerAction {
        NewestPointerAction(std::atomic<uintptr_t>* _ptr, uintptr_t _expected, ColumnMapMainEntry* _desired)
                : ptr(_ptr),
                  expected(_expected),
                  desired(_desired) {
        }

        /// Pointer to the old newest pointer
        std::atomic<uintptr_t>* ptr;

        /// Expected value of the old newest pointer
        uintptr_t expected;

        /// Desired new value of the old newest pointer
        ColumnMapMainEntry* desired;
    };

    /**
     * @brief Whether the page needs to be cleaned
     */
    bool needsCleaning(const ColumnMapMainPage* page);

    /**
     * @brief Add a clean action from an existing main page
     *
     * Copies the variable size heap for the elements to the fill page and enqueues a cleaning action with the new
     * offset correction.
     */
    void addCleanAction(ColumnMapMainPage* page, uint32_t startIdx, uint32_t endIdx);

    /**
     * @brief Write all updates to the update log
     *
     * @param newest Pointer to the newest update log entry
     * @param baseVersion Highest version of the base record
     * @param lowestVersion Version of the last element
     * @param wasDelete Whether the last element was a delete
     * @return True in case the updates were written successfully, false if the current page is full
     */
    bool processUpdates(const UpdateLogEntry* newest, uint64_t baseVersion, uint64_t& lowestVersion, bool& wasDelete);

    /**
     * @brief Write the entry from the update log into the main page
     *
     * Creates a header in the fill page and copies the variable sized fields to the heap of the fill page, then writes
     * the data into the update page.
     */
    void writeUpdate(const UpdateLogEntry* entry);

    /**
     * @brief Write the entry from the insert log into the main page
     *
     * Creates a header in the fill page and copies the variable sized fields to the heap of the fill page, then writes
     * the data into the update page.
     */
    void writeInsert(const InsertLogEntry* entry);

    /**
     * @brief Writes the data from the log entry data in row format into the update page in column format
     */
    void writeData(const char* srcData, uint32_t size);

    /**
     * @brief Flush the current fill page and allocate a new fill page
     */
    void flush();

    /**
     * @brief Flush the current fill page
     *
     * Writes back all data from the enqueued cleaning actions into the fill page, performs the correction on the
     * offsets into the variable sized field and changes the pointers from the old record to point to the newly written
     * elements.
     */
    void flushFillPage();

    const ColumnMapContext& mContext;

    const Record& mRecord;

    PageManager& mPageManager;

    Modifier& mMainTableModifier;

    uint64_t mMinVersion;

    std::vector<CleanAction> mCleanActions;

    std::vector<NewestPointerAction> mPointerActions;

    std::vector<ColumnMapMainPage*> mPageList;

    /// Current update page
    ColumnMapMainPage* mUpdatePage;

    /// Index of the first element to be copied from the update page
    uint32_t mUpdateStartIdx;

    /// Last valid (i.e. completely written) index into the update page
    uint32_t mUpdateEndIdx;

    /// Current index into the update page
    uint32_t mUpdateIdx;

    /// Current fill page
    ColumnMapMainPage* mFillPage;

    /// Current position of the variable sized heap
    char* mFillHeap;

    /// Last valid (i.e. completely written) index into the fill page
    uint32_t mFillEndIdx;

    /// Current index into the fill page
    uint32_t mFillIdx;

    /// Current amount of data to be written
    uint32_t mFillSize;
};

} // namespace deltamain
} // namespace store
} // namespace tell
