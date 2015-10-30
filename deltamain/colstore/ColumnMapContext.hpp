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
 * @brief Context containing approach-specific information for the column map implementation
 */
class ColumnMapContext {
public:
    using ScanProcessor = ColumnMapScanProcessor;
    using Page = ColumnMapMainPage;
    using PageModifier = ColumnMapPageModifier;

    using MainRecord = ColumnMapRecord;
    using ConstMainRecord = ConstColumnMapRecord;

    /**
     * @brief Maximum number of bytes that fit into a column map page
     */
    static constexpr uint32_t MAX_DATA_SIZE = TELL_PAGE_SIZE - sizeof(ColumnMapMainPage);

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

    /**
     * @brief Additional overhead required by every element in a column map page
     */
    uint32_t entryOverhead() const {
        return mEntryOverhead;
    }

    /**
     * @brief Maximum number of elements fitting in one page excluding any variable sized fields
     */
    uint32_t fixedSizeCapacity() const {
        return mFixedSizeCapacity;
    }

    /**
     * @brief Size of all fixed size fields of an element
     */
    uint32_t fixedSize() const {
        return mFixedSize;
    }

    /**
     * @brief Number of variable sized fields of an element
     */
    uint32_t varSizeFieldCount() const {
        return mVarSizeFieldCount;
    }

    /**
     * @brief Vector containing the lengths for all static sized fields of an element
     */
    const std::vector<uint32_t>& fieldLengths() const {
        return mFieldLengths;
    }

    /**
     * @brief The page the given element is located on
     */
    const ColumnMapMainPage* pageFromEntry(const ColumnMapMainEntry* entry) const {
        return reinterpret_cast<const ColumnMapMainPage*>(reinterpret_cast<uintptr_t>(entry)
                - ((reinterpret_cast<uintptr_t>(entry) - mPageData) % TELL_PAGE_SIZE));
    }

private:
    /// Pointer to the start of the page manager data region
    uintptr_t mPageData;

    /// \copydoc ColumnMapContext::entryOverhead() const
    uint32_t mEntryOverhead;

    /// \copydoc ColumnMapContext::fixedSizeCapacity() const
    uint32_t mFixedSizeCapacity;

    /// \copydoc ColumnMapContext::fixedSize() const
    uint32_t mFixedSize;

    /// \copydoc ColumnMapContext::varSizeFieldCount() const
    uint32_t mVarSizeFieldCount;

    /// \copydoc ColumnMapContext::fieldLengths() const
    std::vector<uint32_t> mFieldLengths;
};

} // namespace deltamain
} // namespace store
} // namespace tell
