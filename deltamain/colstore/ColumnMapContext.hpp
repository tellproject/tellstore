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

#include "ColumnMapPage.hpp"
#include "ColumnMapScanProcessor.hpp"
#include "LLVMColumnMapMaterialize.hpp"

#include <util/LLVMJIT.hpp>

#include <config.h>

#include <cstdint>
#include <vector>

namespace tell {
namespace store {

class PageManager;
class Record;

namespace deltamain {

class ColumnMapRecord;
class ConstColumnMapRecord;

/**
 * @brief ColumnMap specific metadata for every fixed size field in a record
 */
struct FixedColumnMetaData {
    FixedColumnMetaData(uint32_t _offset, uint32_t _length)
            : offset(_offset),
              length(_length) {
    }

    uint32_t offset;
    uint32_t length;
};

/**
 * @brief Context containing approach-specific information for the column map implementation
 */
class ColumnMapContext {
public:
    using Scan = ColumnMapScan;
    using Page = ColumnMapMainPage;
    using PageModifier = ColumnMapPageModifier;

    using MainRecord = ColumnMapRecord;
    using ConstMainRecord = ConstColumnMapRecord;

    /**
     * @brief Worst case padding overhead on a page
     *
     * If the number of elements is uneven a 4-byte padding has to be added after the size array so that the following
     * record data is 8-byte aligned. In addition a maximum padding of 6 byte has to be inserted to ensure that the
     * ColumnMapHeapEntry array is 8 byte aligned after the last fixed size field.
     */
    static constexpr uint32_t PAGE_PADDING_OVERHEAD = 10u;

    /**
     * @brief Maximum number of bytes that fit into a column map page
     *
     * If the record contains any variable sized field an additional sentry ColumnMapHeapEntry must be allocated.
     */
    static constexpr uint32_t MAX_DATA_SIZE = TELL_PAGE_SIZE
            - (sizeof(ColumnMapMainPage) + sizeof(ColumnMapHeapEntry) + PAGE_PADDING_OVERHEAD);

    static const char* implementationName() {
        return "Delta-Main Rewrite (Column Map)";
    }

    /**
     * @brief Calculate the index of the given entry on the page
     */
    static uint32_t pageIndex(const ColumnMapMainPage* page, const ColumnMapMainEntry* entry) {
        return static_cast<uint32_t>(entry - page->entryData());
    }

    ColumnMapContext(const PageManager& pageManager, const Record& record);

    const Record& record() const {
        return mRecord;
    }

    /**
     * @brief Size of an element in the column map (excluding the variable heap)
     */
    uint32_t staticSize() const {
        return mStaticSize;
    }

    /**
     * @brief Maximum number of elements fitting in one page (without the variable size heap)
     */
    uint32_t staticCapacity() const {
        return mStaticCapacity;
    }

    /**
     * @brief Size of the header segment of a record
     */
    uint32_t headerSize() const {
        return mHeaderSize;
    }

    /**
     * @brief Size of the fixed size value segment of a record
     */
    uint32_t fixedSize() const {
        return mFixedSize;
    }

    /**
     * @brief ColumnMap specific metadata for all static sized fields of a record
     */
    const std::vector<FixedColumnMetaData>& fixedMetaData() const {
        return mFixedMetaData;
    }

    /**
     * @brief The page the given element is located on
     */
    const ColumnMapMainPage* pageFromEntry(const ColumnMapMainEntry* entry) const {
        return reinterpret_cast<const ColumnMapMainPage*>(reinterpret_cast<uintptr_t>(entry)
                - ((reinterpret_cast<uintptr_t>(entry) - mPageData) % TELL_PAGE_SIZE));
    }

    /**
     * @brief Materialize the tuple from the page into the destination buffer
     */
    void materialize(const ColumnMapMainPage* page, uint32_t idx, uint32_t size, char* dest) const {
        mMaterializeFun(reinterpret_cast<const char*>(page), idx, size, dest);
    }

    /**
     * @brief Function pointer for materializing a tuple from the page
     */
    LLVMColumnMapMaterializeBuilder::Signature materializeFunction() const {
        return mMaterializeFun;
    }

private:
    void prepareMaterializeFunction();

    const Record& mRecord;

    /// Pointer to the start of the page manager data region
    uintptr_t mPageData;

    /// \copydoc ColumnMapContext::staticSize() const
    uint32_t mStaticSize;

    /// \copydoc ColumnMapContext::staticCapacity() const
    uint32_t mStaticCapacity;

    /// \copydoc ColumnMapContext::headerSize() const
    uint32_t mHeaderSize;

    /// \copydoc ColumnMapContext::fixedSize() const
    uint32_t mFixedSize;

    /// \copydoc ColumnMapContext::fixedMetaData() const
    std::vector<FixedColumnMetaData> mFixedMetaData;

    /// \copydoc ColumnMapContext::materializeFunction() const
    LLVMColumnMapMaterializeBuilder::Signature mMaterializeFun;

    LLVMJIT mLLVMJit;
};

} // namespace deltamain
} // namespace store
} // namespace tell
