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

    auto recordSize = record->size();
    if (mOffset + recordSize > MAX_DATA_SIZE) {
        return nullptr;
    }

    auto ptr = RowStoreMainEntry::serialize(data() + mOffset, record, recordSize);
    mOffset += recordSize;
    return ptr;
}

void* RowStorePageModifier::append(const RowStoreMainEntry* record) {
    return internalAppend([this, record] () {
        return mFillPage->append(record);
    });
}

RowStoreMainEntry* RowStorePageModifier::appendRecord(uint64_t key, const std::vector<RecordHolder>& elements) {
    return internalAppend([this, key, elements] () {
        return mFillPage->append(key, elements);
    });
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
