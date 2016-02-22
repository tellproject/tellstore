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

#include "ColumnMapPage.hpp"

#include "ColumnMapContext.hpp"

#include <deltamain/Record.hpp>
#include <deltamain/Table.hpp>
#include <util/CuckooHash.hpp>
#include <util/PageManager.hpp>
#include <tellstore/Record.hpp>

#include <cstring>
#include <type_traits>

namespace tell {
namespace store {
namespace deltamain {

ColumnMapHeapEntry::ColumnMapHeapEntry(uint32_t _offset, const char* data)
        : offset(_offset) {
    memcpy(prefix, data, sizeof(prefix));
}

ColumnMapHeapEntry::ColumnMapHeapEntry(uint32_t _offset, uint32_t size, const char* data)
        : offset(_offset),
          prefix{} {
    memcpy(prefix, data, size < sizeof(prefix) ? size : sizeof(prefix));
}

ColumnMapMainPage::ColumnMapMainPage(const ColumnMapContext& context, uint32_t _count)
        : count(_count),
          headerOffset(crossbow::align(sizeof(ColumnMapMainPage)
                + count * (sizeof(ColumnMapMainEntry) + sizeof(uint32_t)), 8u)),
          fixedOffset(crossbow::align(headerOffset + count * context.headerSize(), 8u)),
          variableOffset(crossbow::align(fixedOffset + count * context.fixedSize(), 8u)) {
    // Create sentinel heap entry
    auto& record = context.record();
    if (record.varSizeFieldCount() != 0) {
        new (reinterpret_cast<char*>(this) + variableOffset) ColumnMapHeapEntry(TELL_PAGE_SIZE);
        variableOffset += sizeof(ColumnMapHeapEntry);
    }
}

ColumnMapPageModifier::ColumnMapPageModifier(const ColumnMapContext& context, PageManager& pageManager,
        CuckooTable& mainTable, Modifier& mainTableModifier, uint64_t minVersion)
        : mContext(context),
          mRecord(mContext.record()),
          mPageManager(pageManager),
          mMainTable(mainTable),
          mMainTableModifier(mainTableModifier),
          mMinVersion(minVersion),
          mUpdatePage(nullptr),
          mUpdateStartIdx(0u),
          mUpdateEndIdx(0u),
          mUpdateIdx(0u),
          mUpdateDirty(false),
          mFillPage(nullptr),
          mFillHeap(nullptr),
          mFillEndIdx(0u),
          mFillIdx(0u),
          mFillSize(0u),
          mFillDirty(false) {
}

ColumnMapPageModifier::~ColumnMapPageModifier() {
    releasePage(mFillPage, mFillDirty);
    releasePage(mUpdatePage, mUpdateDirty);
}

bool ColumnMapPageModifier::clean(ColumnMapMainPage* page) {
    if (!needsCleaning(page)) {
        mPageList.emplace_back(page);
        return false;
    }

    lazyInitializePages();

    auto entries = page->entryData();
    auto sizes = page->sizeData();

    const ColumnMapHeapEntry* heapEntries = nullptr;
    if (mRecord.varSizeFieldCount() != 0) {
        heapEntries = page->variableData();
    }

    uint32_t mainStartIdx = 0;
    uint32_t mainEndIdx = 0;

    typename std::remove_const<decltype(page->count)>::type i = 0;
    while (i < page->count) {
START:
        LOG_ASSERT(mFillIdx == mFillEndIdx, "Current fill index must be at the end index");
        LOG_ASSERT(mUpdateIdx == mUpdateEndIdx, "Current update index must be at the end index");

        auto baseIdx = i;
        auto newest = entries[baseIdx].newest.load();
        bool wasDelete = false;
        if (newest != 0u) {
            if (mainStartIdx != mainEndIdx) {
                LOG_ASSERT(mUpdateStartIdx == mUpdateEndIdx, "Main and update copy at the same time");
                addCleanAction(page, mainStartIdx, mainEndIdx);
                mainStartIdx = 0;
                mainEndIdx = 0;
            }

            uint64_t lowestVersion;

            // Write all updates into the update page
            if (!processUpdates(reinterpret_cast<const UpdateLogEntry*>(newest), entries[baseIdx].version,
                    lowestVersion, wasDelete)) {
                // The page is full, flush any pending clean actions and repeat
                if (mainStartIdx != mainEndIdx) {
                    LOG_ASSERT(mUpdateStartIdx == mUpdateEndIdx, "Main and update copy at the same time");
                    addCleanAction(page, mainStartIdx, mainEndIdx);
                    mainStartIdx = 0;
                    mainEndIdx = 0;
                }
                flush();
                continue;
            }

            // If all elements were overwritten by updates the main page does not need to be processed
            if (lowestVersion <= mMinVersion) {
                if (mUpdateIdx == mUpdateEndIdx) {
                    LOG_ASSERT(mFillIdx == mFillEndIdx, "No elements written but fill index advanced");

                    // Invalidate the element and remove it from the hash table - Retry from beginning if the
                    // invalidation fails
                    if (!entries[baseIdx].newest.compare_exchange_strong(newest,
                            crossbow::to_underlying(NewestPointerTag::INVALID))) {
                        LOG_ASSERT(i == baseIdx, "Changed base index without iterating");
                        continue;
                    }

                    __attribute__((unused)) auto res = mMainTable.remove(entries[baseIdx].key);
                    LOG_ASSERT(res, "Removing key from hash table did not succeed");
                } else {
                    LOG_ASSERT(mFillIdx != mFillEndIdx, "Elements written without advancing the fill index");

                    // Flush the changes to the current element
                    auto fillEntry = mFillPage->entryData() + mFillEndIdx;
                    mPointerActions.emplace_back(&entries[baseIdx].newest, newest, fillEntry);

                    __attribute__((unused)) auto res = mMainTable.update(entries[baseIdx].key, fillEntry);
                    LOG_ASSERT(res, "Inserting key into hash table did not succeed");

                    mUpdateEndIdx = mUpdateIdx;
                    mFillEndIdx = mFillIdx;
                }

                // Skip to next key and start from the beginning
                for (++i; i < page->count && entries[i].key == entries[baseIdx].key; ++i) {
                }
                continue;
            }

            // Skip elements with version above lowest version
            for (; i < page->count && entries[i].key == entries[baseIdx].key && entries[i].version >= lowestVersion;
                    ++i) {
            }
        }

        // Process all main entries up to the first element that can be discarded
        auto copyStartIdx = i;
        auto copyEndIdx = i;
        for (; i < page->count && entries[i].key == entries[baseIdx].key; ++i) {
            if (wasDelete) {
                LOG_ASSERT(sizes[i] != 0u, "Only data entry can follow a delete");
                if (entries[i].version < mMinVersion) {
                    --mFillIdx;
                    mFillSize -= mContext.staticSize();
                    if (copyStartIdx == copyEndIdx) { // The delete must come from an update entry
                        LOG_ASSERT(mUpdateIdx > mUpdateEndIdx, "No element written before the delete");
                        --mUpdateIdx;
                    } else { // The delete must come from the previous main entry
                        --copyEndIdx;
                    }
                    wasDelete = false;
                    break;
                }
            }
            auto size = mContext.staticSize();
            if (sizes[i] == 0u) {
                if (entries[i].version <= mMinVersion) {
                    break;
                }
                wasDelete = true;
            } else {
                // Add size of variable heap
                if (heapEntries != nullptr) {
                    auto beginOffset = heapEntries[i].offset;
                    auto endOffset = heapEntries[static_cast<int32_t>(i) - 1].offset;
                    size += (endOffset - beginOffset);
                }
                wasDelete = false;
            }

            mFillSize += size;
            if (mFillSize > ColumnMapContext::MAX_DATA_SIZE) {
                if (mainStartIdx != mainEndIdx) {
                    LOG_ASSERT(mUpdateStartIdx == mUpdateEndIdx, "Main and update copy at the same time");
                    addCleanAction(page, mainStartIdx, mainEndIdx);
                    mainStartIdx = 0;
                    mainEndIdx = 0;
                }
                flush();
                i = baseIdx;
                goto START;
            }

            new (mFillPage->entryData() + mFillIdx) ColumnMapMainEntry(entries[baseIdx].key, entries[i].version);
            ++mFillIdx;
            mFillDirty = true;

            ++copyEndIdx;

            // Check if the element is already the oldest readable element
            if (entries[i].version <= mMinVersion) {
                break;
            }
        }

        LOG_ASSERT(!wasDelete, "Last element must not be a delete");
        LOG_ASSERT(mFillIdx - mFillEndIdx == (copyEndIdx - copyStartIdx) + (mUpdateIdx - mUpdateEndIdx),
                "Fill count does not match actual number of written elements");

        // Invalidate the element if it can be removed completely otherwise enqueue modification of the newest pointer
        // Retry from beginning if the invalidation fails
        if (mFillIdx == mFillEndIdx) {
            if (!entries[baseIdx].newest.compare_exchange_strong(newest,
                    crossbow::to_underlying(NewestPointerTag::INVALID))) {
                i = baseIdx;
                continue;
            }
            __attribute__((unused)) auto res = mMainTable.remove(entries[baseIdx].key);
            LOG_ASSERT(res, "Removing key from hash table did not succeed");
        } else {
            auto fillEntry = mFillPage->entryData() + mFillEndIdx;
            mPointerActions.emplace_back(&entries[baseIdx].newest, newest, fillEntry);

            __attribute__((unused)) auto res = mMainTable.update(entries[baseIdx].key, fillEntry);
            LOG_ASSERT(res, "Inserting key into hash table did not succeed");

            // No updates and copy region starts at end from previous - simply extend copy region from main
            if (mainEndIdx == copyStartIdx && mUpdateIdx == mUpdateStartIdx) {
                mainEndIdx = copyEndIdx;
            } else {
                // Flush any pending actions from previous main
                if (mainStartIdx != mainEndIdx) {
                    LOG_ASSERT(mUpdateStartIdx == mUpdateEndIdx, "Main and update copy at the same time");
                    addCleanAction(page, mainStartIdx, mainEndIdx);
                    mainStartIdx = 0;
                    mainEndIdx = 0;
                }
                // Enqueue updates
                mUpdateEndIdx = mUpdateIdx;

                // Enqueue main
                if (copyStartIdx != copyEndIdx) {
                    if (mUpdateStartIdx != mUpdateEndIdx) {
                        mCleanActions.emplace_back(mUpdatePage, mUpdateStartIdx, mUpdateEndIdx, 0);
                        mUpdateStartIdx = mUpdateEndIdx;
                    }
                    mainStartIdx = copyStartIdx;
                    mainEndIdx = copyEndIdx;
                }
            }
            mFillEndIdx = mFillIdx;
        }

        // Skip to next key
        for (; i < page->count && entries[i].key == entries[baseIdx].key; ++i) {
        }
    }

    LOG_ASSERT(i == page->count, "Not at end of page");

    // Append last pending clean action
    if (mainStartIdx != mainEndIdx) {
        LOG_ASSERT(mUpdateStartIdx == mUpdateIdx, "Main and update copy at the same time");
        addCleanAction(page, mainStartIdx, mainEndIdx);
    }

    return true;
}

bool ColumnMapPageModifier::append(InsertRecord& oldRecord) {
    lazyInitializePages();

    while (true) {
        LOG_ASSERT(mFillIdx == mFillEndIdx, "Current fill index must be at the end index");
        LOG_ASSERT(mUpdateIdx == mUpdateEndIdx, "Current update index must be at the end index");
        bool wasDelete = false;
        if (oldRecord.newest() != 0u) {
            uint64_t lowestVersion;
            if (!processUpdates(reinterpret_cast<const UpdateLogEntry*>(oldRecord.newest()), oldRecord.baseVersion(),
                    lowestVersion, wasDelete)) {
                flush();
                continue;
            }

            // Check if all elements were overwritten by the update log
            if (wasDelete && oldRecord.baseVersion() < mMinVersion) {
                mFillSize -= mContext.staticSize();
                LOG_ASSERT(mFillIdx > mFillEndIdx, "No element written before the delete");
                --mFillIdx;
                LOG_ASSERT(mUpdateIdx > mUpdateEndIdx, "No element written before the delete");
                --mUpdateIdx;
            } else if (lowestVersion > std::max(mMinVersion, oldRecord.baseVersion())) {
                auto value = oldRecord.value();

                mFillSize += mContext.calculateFillSize(value->data());
                if (mFillSize > ColumnMapContext::MAX_DATA_SIZE) {
                    flush();
                    continue;
                }

                writeInsert(value);
            }

            // Invalidate the element if it can be removed completely
            // Retry from beginning if the invalidation fails
            if (mUpdateIdx == mUpdateEndIdx) {
                if (!oldRecord.tryInvalidate()) {
                    continue;
                }
                return false;
            }
        } else {
            auto value = oldRecord.value();

            mFillSize += mContext.calculateFillSize(value->data());
            if (mFillSize > ColumnMapContext::MAX_DATA_SIZE) {
                flush();
                continue;
            }

            writeInsert(value);
        }

        LOG_ASSERT(mFillIdx - mFillEndIdx == mUpdateIdx - mUpdateEndIdx,
                "Fill count does not match actual number of written elements");

        auto fillEntry = mFillPage->entryData() + mFillEndIdx;
        mPointerActions.emplace_back(&oldRecord.value()->newest, oldRecord.newest(), fillEntry);

        __attribute__((unused)) auto res = mMainTableModifier.insert(oldRecord.key(), fillEntry);
        LOG_ASSERT(res, "Inserting key into hash table did not succeed");

        mUpdateEndIdx = mUpdateIdx;
        mFillEndIdx = mFillIdx;
        return true;
    }
}

std::vector<ColumnMapMainPage*> ColumnMapPageModifier::done() {
    if (mFillEndIdx != 0u) {
        flush();
    }

    std::vector<ColumnMapMainPage*> pageList;
    mPageList.swap(pageList);
    return std::move(pageList);
}

bool ColumnMapPageModifier::needsCleaning(const ColumnMapMainPage* page) {
    auto entries = page->entryData();
    typename std::remove_const<decltype(page->count)>::type i = 0;
    while (i < page->count) {
        // In case the record has pending updates it needs to be cleaned
        auto newest = entries[i].newest.load();
        LOG_ASSERT(newest % 8u == 0u, "Newest pointer must be unmarked before garbage collection");
        if (newest != 0x0u) {
            return true;
        }

        // Skip to next key
        // The record needs cleaning if one but the first version can be purged
        auto key = entries[i].key;
        for (++i; i < page->count && entries[i].key == key; ++i) {
            LOG_ASSERT(entries[i].newest.load() == 0x0u, "Newest pointer must be null");
            if (entries[i].version < mMinVersion) {
                return true;
            }
        }
    }
    return false;
}

void ColumnMapPageModifier::addCleanAction(ColumnMapMainPage* page, uint32_t startIdx, uint32_t endIdx) {
    LOG_ASSERT(endIdx > startIdx, "End index must be larger than start index");

    // Do not copy and adjust heap if the table has no variable sized fields
    if (mRecord.varSizeFieldCount() == 0u) {
        mCleanActions.emplace_back(page, startIdx, endIdx, 0u);
        return;
    }

    // Determine begin and end offset of the variable size heap
    auto heapEntries = page->variableData();
    auto beginOffset = heapEntries[static_cast<int32_t>(endIdx) - 1].offset;
    auto endOffset = heapEntries[static_cast<int32_t>(startIdx) - 1].offset;
    LOG_ASSERT(beginOffset <= endOffset, "Begin offset larger than end offset");
    LOG_ASSERT(endOffset <= TELL_PAGE_SIZE, "End offset larger than page size")

    auto length = endOffset - beginOffset;

    // Copy varsize heap into the fill page
    mFillHeap -= length;
    memcpy(mFillHeap, reinterpret_cast<const char*>(page) + beginOffset, length);

    // Add clean action with varsize heap offset correct
    auto offsetCorrect = static_cast<int32_t>(mFillHeap - reinterpret_cast<const char*>(mFillPage))
            - static_cast<int32_t>(beginOffset);
    mCleanActions.emplace_back(page, startIdx, endIdx, offsetCorrect);
}

bool ColumnMapPageModifier::processUpdates(const UpdateLogEntry* newest, uint64_t baseVersion, uint64_t& lowestVersion,
        bool& wasDelete) {
    UpdateRecordIterator updateIter(newest, baseVersion);

    // Loop over update log
    for (; !updateIter.done(); updateIter.next()) {
        auto value = updateIter.value();
        auto logEntry = LogEntry::entryFromData(reinterpret_cast<const char*>(value));
        // If the previous update was a delete and the element is below the lowest active version then the
        // delete can be discarded. In this case the update index counter can simply be decremented by one as a
        // delete only writes the header entry in the fill page.
        if (wasDelete) {
            LOG_ASSERT(logEntry->type() == crossbow::to_underlying(RecordType::DATA),
                    "Only data entry can follow a delete");
            LOG_ASSERT(mUpdateIdx > mUpdateEndIdx, "Was delete but no element written");
            if (updateIter->version < mMinVersion) {
                --mUpdateIdx;
                --mFillIdx;
                mFillSize -= mContext.staticSize();
                wasDelete = false;
                break;
            }
        }

        if (logEntry->type() == crossbow::to_underlying(RecordType::DELETE)) {
            // The entry this entry marks as deleted can not be read, skip deletion and break
            if (updateIter->version <= mMinVersion) {
                break;
            }
            mFillSize += mContext.staticSize();
            wasDelete = true;
        } else {
            mFillSize += mContext.calculateFillSize(value->data());
            wasDelete = false;
        }

        if (mFillSize > ColumnMapContext::MAX_DATA_SIZE) {
            return false;
        }

        writeUpdate(value);

        // Check if the element is already the oldest readable element
        if (updateIter->version <= mMinVersion) {
            break;
        }
    }

    lowestVersion = updateIter.lowestVersion();
    return true;
}

void ColumnMapPageModifier::writeUpdate(const UpdateLogEntry* entry) {
    // Write entries into the fill page
    new (mFillPage->entryData() + mFillIdx) ColumnMapMainEntry(entry->key, entry->version);

    auto logEntry = LogEntry::entryFromData(reinterpret_cast<const char*>(entry));

    // Everything stays zero initialized when the entry marks a deletion
    if (logEntry->type() != crossbow::to_underlying(RecordType::DELETE)) {
        // Write data into update page
        writeData(entry->data(), logEntry->size() - sizeof(UpdateLogEntry));
    } else {
        mUpdatePage->sizeData()[mUpdateIdx] = 0u;
        if (mRecord.varSizeFieldCount() != 0u) {
            // Deletes do not have any data on the var size heap but the offsets must be correct nonetheless
            // Copy the current offset of the heap
            auto heapOffset = static_cast<uint32_t>(mFillHeap - reinterpret_cast<const char*>(mFillPage));

            auto variableData = mUpdatePage->variableData() + mUpdateIdx;
            for (decltype(mRecord.varSizeFieldCount()) i = 0; i < mRecord.varSizeFieldCount(); ++i) {
                new (variableData) ColumnMapHeapEntry(heapOffset, 0u, nullptr);

                // Advance pointer to offset entry of next field
                variableData += mUpdatePage->count;
            }
        }
    }

    ++mFillIdx;
    mFillDirty = true;
    ++mUpdateIdx;
    mUpdateDirty = true;
}

void ColumnMapPageModifier::writeInsert(const InsertLogEntry* entry) {
    // Write entries into the fill page
    new (mFillPage->entryData() + mFillIdx) ColumnMapMainEntry(entry->key, entry->version);

    // Write data into update page
    auto logEntry = LogEntry::entryFromData(reinterpret_cast<const char*>(entry));
    writeData(entry->data(), logEntry->size() - sizeof(InsertLogEntry));

    ++mFillIdx;
    mFillDirty = true;
    ++mUpdateIdx;
    mUpdateDirty = true;
}

void ColumnMapPageModifier::writeData(const char* data, uint32_t size) {
    // Write size into the update page
    LOG_ASSERT(size != 0, "Size must be larger than 0");
    mUpdatePage->sizeData()[mUpdateIdx] = size;

    // Copy the header if the record has one into the update page
    if (mRecord.headerSize() != 0) {
        auto headerData = mUpdatePage->headerData() + mUpdateIdx;
        for (decltype(mRecord.fieldCount()) i = 0; i < mRecord.fieldCount(); ++i) {
            auto& fieldMeta = mRecord.getFieldMeta(i);
            auto& field = fieldMeta.field;
            if (field.isNotNull()) {
                continue;
            }
            *headerData = mRecord.isFieldNull(data, fieldMeta.nullIdx);
            headerData += mUpdatePage->count;
        }
    }

    // Copy all fixed size fields into the update page
    if (mRecord.fixedSizeFieldCount() != 0) {
        auto fixedData = mUpdatePage->fixedData();
        for (decltype(mRecord.fixedSizeFieldCount()) i = 0; i < mRecord.fixedSizeFieldCount(); ++i) {
            auto& fieldMeta = mRecord.getFieldMeta(i);
            auto& field = fieldMeta.field;
            auto fieldLength = field.staticSize();
            memcpy(fixedData + mUpdateIdx * fieldLength, data + fieldMeta.offset, fieldLength);
            fixedData += mUpdatePage->count * fieldLength;
        }
    }

    // Copy all variable sized fields into the fill page
    if (mRecord.varSizeFieldCount() != 0) {
        auto heapLength = mRecord.heapSize(data);
        mFillHeap -= heapLength;
        memcpy(mFillHeap, data + mRecord.staticSize(), heapLength);
        auto heapOffset = static_cast<uint32_t>(mFillHeap - reinterpret_cast<const char*>(mFillPage));

        auto variableData = mUpdatePage->variableData() + mUpdateIdx;
        auto offsetData = reinterpret_cast<const uint32_t*>(data + mRecord.variableOffset());
        for (decltype(mRecord.varSizeFieldCount()) i = 0; i < mRecord.varSizeFieldCount(); ++i) {
            auto varOffset = offsetData[i];
            auto varSize = offsetData[i + 1] - varOffset;

            // Write heap entry and advance to next field
            new (variableData) ColumnMapHeapEntry(heapOffset, varSize, data + varOffset);

            // Advance pointer to offset entry of next field
            variableData += mUpdatePage->count;

            // Advance offset into the heap
            heapOffset += varSize;
        }
    }
}

void ColumnMapPageModifier::flush() {
    LOG_ASSERT(mFillPage, "Fill page must not be null");
    LOG_ASSERT(mFillEndIdx > 0, "Trying to flush empty page");
    LOG_ASSERT(mFillDirty, "Fill page must be dirty when modified");

    // Enqueue any pending update actions
    if (mUpdateStartIdx != mUpdateEndIdx) {
        mCleanActions.emplace_back(mUpdatePage, mUpdateStartIdx, mUpdateEndIdx, 0);
        mUpdateStartIdx = mUpdateEndIdx;
    }

    // Set correct count
    new (mFillPage) ColumnMapMainPage(mContext, mFillEndIdx);
    mPageList.emplace_back(mFillPage);

    // Copy sizes
    auto sizes = mFillPage->sizeData();
    for (const auto& action : mCleanActions) {
        auto srcData = action.page->sizeData() + action.startIdx;
        auto count = action.endIdx - action.startIdx;
        memcpy(sizes, srcData, count * sizeof(uint32_t));
        sizes += count;
    }
    LOG_ASSERT(sizes == mFillPage->sizeData() + mFillPage->count, "Did not copy all sizes");

    // Copy all header into the fill page
    if (mRecord.headerSize() != 0) {
        auto headerData = mFillPage->headerData();
        for (decltype(mRecord.headerSize()) i = 0; i < mRecord.headerSize(); ++i) {
            for (const auto& action : mCleanActions) {
                auto srcData = action.page->headerData() + action.page->count * i + action.startIdx;
                auto length = (action.endIdx - action.startIdx);
                memcpy(headerData, srcData, length);
                headerData += length;
            }
        }
    }

    // Copy all fixed size fields into the fill page
    if (mRecord.fixedSizeFieldCount() != 0) {
        size_t fieldOffset = 0;
        auto fixedData = mFillPage->fixedData();
        for (decltype(mRecord.fixedSizeFieldCount()) i = 0; i < mRecord.fixedSizeFieldCount(); ++i) {
            auto& field = mRecord.getFieldMeta(i).field;
            auto fieldLength = field.staticSize();

            for (const auto& action : mCleanActions) {
                auto srcData = action.page->fixedData() + action.page->count * fieldOffset
                        + action.startIdx * fieldLength;
                auto length = (action.endIdx - action.startIdx) * fieldLength;
                memcpy(fixedData, srcData, length);
                fixedData += length;
            }
            fieldOffset += fieldLength;
        }
    }

    // Copy all variable size field heap entries
    // If the offset correction is 0 we can do a single memory copy otherwise we have to adjust the offset for every
    // element.
    if (mRecord.varSizeFieldCount() != 0) {
#ifndef NDEBUG
        auto minOffset = mFillPage->variableOffset + sizeof(ColumnMapHeapEntry) * mFillPage->count
                * mRecord.varSizeFieldCount();
#endif
        auto variableData = mFillPage->variableData();
        for (decltype(mRecord.varSizeFieldCount()) i = 0; i < mRecord.varSizeFieldCount(); ++i) {
            for (const auto& action : mCleanActions) {
                auto heapEntries = action.page->variableData() + (action.page->count * i) + action.startIdx;
                auto count = (action.endIdx - action.startIdx);
                if (action.offsetCorrection == 0) {
                    memcpy(variableData, heapEntries, count * sizeof(ColumnMapHeapEntry));
                    variableData += count;
                } else {
                    for (auto endData = heapEntries + count; heapEntries != endData; ++heapEntries, ++variableData) {
                        auto newOffset = static_cast<int32_t>(heapEntries->offset) + action.offsetCorrection;
                        LOG_ASSERT(newOffset >= static_cast<int32_t>(minOffset),
                                "Corrected offset must be larger than page metadata");
                        LOG_ASSERT(newOffset <= static_cast<int32_t>(TELL_PAGE_SIZE),
                                "Corrected offset larger than page size")
                        new (variableData) ColumnMapHeapEntry(static_cast<uint32_t>(newOffset), heapEntries->prefix);
                    }
                }
            }
        }
    }
    mCleanActions.clear();

    // Adjust newest pointer
    for (auto& action : mPointerActions) {
        auto desired = reinterpret_cast<uintptr_t>(action.desired) | crossbow::to_underlying(NewestPointerTag::MAIN);
        while (!action.ptr->compare_exchange_strong(action.expected, desired)) {
            LOG_ASSERT(action.expected % 8u == 0u && action.expected != 0x0u, "Changed pointer is invalid");
            action.desired->newest.store(action.expected);
        }
    }
    mPointerActions.clear();

    // Clean pages
    if (mUpdateDirty) {
        LOG_ASSERT(mUpdatePage, "Update page must not be null");
        memset(mUpdatePage, 0, TELL_PAGE_SIZE);
        new (mUpdatePage) ColumnMapMainPage(mContext, mContext.staticCapacity());
        mUpdateStartIdx = 0u;
        mUpdateEndIdx = 0u;
        mUpdateIdx = 0u;
        mUpdateDirty = false;
    }

    auto page = mPageManager.alloc();
    if (!page) {
        LOG_ERROR("PageManager ran out of space");
        std::terminate();
    }
    mFillPage = new (page) ColumnMapMainPage();
    mFillHeap = mFillPage->heapData();
    mFillEndIdx = 0u;
    mFillIdx = 0u;
    mFillSize = 0u;
    mFillDirty = false;
}

void ColumnMapPageModifier::lazyInitializePages() {
    if (mUpdatePage) {
        LOG_ASSERT(mFillPage, "Fill page must not be null");
        return;
    }

    auto page = mPageManager.alloc();
    if (!page) {
        LOG_ERROR("PageManager ran out of space");
        std::terminate();
    }
    mUpdatePage = new (page) ColumnMapMainPage(mContext, mContext.staticCapacity());

    page = mPageManager.alloc();
    if (!page) {
        LOG_ERROR("PageManager ran out of space");
        std::terminate();
    }
    mFillPage = new (page) ColumnMapMainPage();
    mFillHeap = mFillPage->heapData();
}

void ColumnMapPageModifier::releasePage(ColumnMapMainPage* page, bool dirty) {
    if (page == nullptr) {
        return;
    }

    if (dirty) {
        mPageManager.free(page);
    } else {
        // Clear the sentinel ColumnMapHeapEntry
        if (mRecord.varSizeFieldCount() != 0) {
            auto variableData = page->variableData();
            --variableData;
            memset(variableData, 0u, sizeof(ColumnMapHeapEntry));
        }
        memset(page, 0u, sizeof(ColumnMapMainPage));
        mPageManager.freeEmpty(page);
    }
}

} // namespace deltamain
} // namespace store
} // namespace tell
