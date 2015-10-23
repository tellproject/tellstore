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

#include "RowStorePage.hpp"

#include <config.h>
#include <util/CuckooHash.hpp>
#include <util/Log.hpp>
#include <util/PageManager.hpp>

#include <crossbow/logger.hpp>

namespace tell {
namespace store {
namespace deltamain {
namespace {

constexpr uint32_t MAX_DATA_SIZE = TELL_PAGE_SIZE - sizeof(RowStoreMainPage);

} // anonymous namespace

bool RowStoreMainPage::needsCleaning(uint64_t minVersion) const {
    for (auto& ptr : *this) {
        ConstRowStoreRecord record(&ptr);
        if (record.needsCleaning(minVersion)) {
            return true;
        }
    }
    return false;
}

RowStoreMainEntry* RowStoreMainPage::append(uint64_t key, const std::vector<RecordHolder>& elements) {
    static_assert(std::is_standard_layout<RowStoreMainEntry>::value, "Record class must be a POD");

    auto recordSize = RowStoreMainEntry::serializedSize(elements);
    if (mOffset + recordSize > MAX_DATA_SIZE) {
        return nullptr;
    }

    auto ptr = RowStoreMainEntry::serialize(data() + mOffset, key, elements);
    mOffset += recordSize;
    return ptr;
}

RowStoreMainEntry* RowStoreMainPage::append(const RowStoreMainEntry* record) {
    static_assert(std::is_standard_layout<RowStoreMainEntry>::value, "Record class must be a POD");

    auto offsets = record->offsetData();
    auto recordSize = offsets[record->versionCount];
    if (mOffset + recordSize > MAX_DATA_SIZE) {
        return nullptr;
    }

    auto ptr = RowStoreMainEntry::serialize(data() + mOffset, record, recordSize);
    mOffset += recordSize;
    return ptr;
}

bool RowStorePageModifier::clean(RowStoreMainPage* page) {
    if (!page->needsCleaning(mMinVersion)) {
        return false;
    }

    for (auto& ptr : *page) {
        RowStoreRecord oldRecord(&ptr);
        LOG_ASSERT(oldRecord.newest() % 8 == crossbow::to_underlying(NewestPointerTag::UPDATE),
                "Newest pointer must point to untagged update record");

        RowStoreMainEntry* newEntry;
        if (!oldRecord.needsCleaning(mMinVersion)) {
            newEntry = internalAppend([this, &oldRecord] () {
                return mFillPage->append(oldRecord.value());
            });
        } else {
            if (!collectElements(oldRecord)) {
                __attribute__((unused)) auto res = mMainTableModifier.remove(oldRecord.key());
                LOG_ASSERT(res, "Removing key from hash table did not succeed");
                continue;
            }

            // Append to page
            newEntry = internalAppend([this, &oldRecord] () {
                return mFillPage->append(oldRecord.key(), mElements);
            });
            mElements.clear();
        }
        recycleEntry(oldRecord, newEntry, true);
    }

    return true;
}

bool RowStorePageModifier::append(InsertRecord& oldRecord) {
    if (!collectElements(oldRecord)) {
        return false;
    }

    // Append to page
    auto newRecord = internalAppend([this, &oldRecord] () {
        return mFillPage->append(oldRecord.key(), mElements);
    });
    mElements.clear();

    recycleEntry(oldRecord, newRecord, false);

    return true;
}

template <typename Rec>
bool RowStorePageModifier::collectElements(Rec& rec) {
    while (true) {
        // Collect elements from update log
        UpdateRecordIterator updateIter(reinterpret_cast<const UpdateLogEntry*>(rec.newest()), rec.baseVersion());
        for (; !updateIter.done(); updateIter.next()) {
            auto entry = LogEntry::entryFromData(reinterpret_cast<const char*>(updateIter.value()));
            mElements.emplace_back(updateIter->version, updateIter->data(), entry->size() - sizeof(UpdateLogEntry));

            // Check if the element is already the oldest readable element
            if (updateIter->version <= mMinVersion) {
                break;
            }
        }

        // Collect elements from record
        rec.collect(mMinVersion, updateIter.lowestVersion(), mElements);

        // Remove last element if it is a delete
        if (!mElements.empty() && mElements.back().size == 0u) {
            mElements.pop_back();
        }

        // Invalidate record if the record has no valid elements
        if (mElements.empty()) {
            if (!rec.tryInvalidate()) {
                continue;
            }
            return false;
        }

        LOG_ASSERT(mElements.back().size != 0, "Size of oldest element must not be 0");
        return true;
    }
}

template <typename Rec>
void RowStorePageModifier::recycleEntry(Rec& oldRecord, RowStoreMainEntry* newRecord, bool replace) {
    LOG_ASSERT(newRecord != nullptr, "Can not recycle an old record to null");

    __attribute__((unused)) auto res = mMainTableModifier.insert(oldRecord.key(), newRecord, replace);
    LOG_ASSERT(res, "Inserting key into hash table did not succeed");

    while (!oldRecord.tryUpdate(reinterpret_cast<uintptr_t>(newRecord)
            | crossbow::to_underlying(NewestPointerTag::MAIN))) {
        newRecord->newest.store(oldRecord.newest());
    }
}

template <typename Fun>
RowStoreMainEntry* RowStorePageModifier::internalAppend(Fun fun) {
    if (!mFillPage) {
        mFillPage = new (mPageManager.alloc()) RowStoreMainPage();
        if (!mFillPage) {
            return nullptr;
        }
        mPageList.emplace_back(mFillPage);
    }

    if (auto newRecord = fun()) {
        return newRecord;
    }

    mFillPage = new (mPageManager.alloc()) RowStoreMainPage();
    if (!mFillPage) {
        return nullptr;
    }
    mPageList.emplace_back(mFillPage);

    if (auto newRecord = fun()) {
        return newRecord;
    }

    LOG_ASSERT(false, "Insert on new allocated page failed");
    return nullptr;
}

} // namespace deltamain
} // namespace store
} // namespace tell
