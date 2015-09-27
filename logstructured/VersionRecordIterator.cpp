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
#include "VersionRecordIterator.hpp"

#include "Table.hpp"

#include <crossbow/logger.hpp>

namespace tell {
namespace store {
namespace logstructured {

VersionRecordIterator::VersionRecordIterator(Table& table, uint64_t key)
        : mTable(table),
          mPrev(nullptr),
          mCurrent(retrieveHead(key)) {
    setCurrentEntry();
    LOG_ASSERT(isNewest(), "Start iterator not newest");
}

VersionRecordIterator::VersionRecordIterator(Table& table, ChainedVersionRecord* head)
        : mTable(table),
          mPrev(nullptr),
          mCurrent(head) {
    setCurrentEntry();
    LOG_ASSERT(isNewest(), "Start iterator not newest");
}

void VersionRecordIterator::next() {
    LOG_ASSERT(!done(), "Trying to iterate over the end");

    mPrev = mCurrent;
    mPrevData = mCurrentData;
    mCurrent = mCurrentData.next();

    setCurrentEntry();
}

bool VersionRecordIterator::find(ChainedVersionRecord* element) {
    for (; !done(); next()) {
        // Check if the iterator reached the element
        if (mCurrent == element) {
            return true;
        }

        // Check if the current element is already older than the given element
        if (mCurrent->validFrom() < element->validFrom()) {
            return false;
        }
    }

    return false;
}

bool VersionRecordIterator::find(uint64_t version) {
    for (; !done(); next()) {
        // Check if the iterator reached the element with the given version
        if (mCurrent->validFrom() == version) {
            return true;
        }

        // Check if the current element is already older than the given version
        if (mCurrent->validFrom() < version) {
            return false;
        }
    }

    return false;
}

bool VersionRecordIterator::remove() {
    LOG_ASSERT(!done(), "Remove only supported on valid element");
    LOG_ASSERT(isNewest(), "Remove only supported on the beginning");

    auto next = mCurrentData.next();
    auto res = mCurrent->tryInvalidate(mCurrentData, next);
    if (!res) {
        // The validTo version or the next pointer changed (the current element is still valid)
        if (!mCurrentData.isInvalid()) {
            return res;
        }

        // The current element is invalid - Reset the iterator to the next element
        next = mCurrentData.next();
    } else if (next) {
        reactivateNextElement();
    }
    removeCurrentInvalidFromHead(next);
    setCurrentEntry();
    return res;
}

bool VersionRecordIterator::replace(ChainedVersionRecord* next) {
    LOG_ASSERT(next->validFrom() == mCurrent->validFrom(), "Replace only supported on element with same version");
    LOG_ASSERT(!done(), "Replace only supported on valid element");

    next->mutableData(mCurrentData);
    auto res = mCurrent->tryInvalidate(mCurrentData, next);
    if (!res) {
        // The validTo version or the next pointer changed (the current element is still valid)
        if (!mCurrentData.isInvalid()) {
            return res;
        }

        // The current element is invalid
        next = mCurrentData.next();
    }
    replaceCurrentInvalidElement(next);
    setCurrentEntry();
    return res;
}

bool VersionRecordIterator::insert(ChainedVersionRecord* element) {
    LOG_ASSERT(isNewest(), "Insert only supported on the beginning");
    LOG_ASSERT(!mCurrent || mCurrent->validFrom() < element->validFrom(), "Version of the tuple to insert must be "
            "larger than current tuple");
    auto next = mCurrent;
    mCurrentData = MutableRecordData(ChainedVersionRecord::ACTIVE_VERSION, next, false);
    element->mutableData(mCurrentData);

    auto res = insertHead(element);
    if (!res) {
        setCurrentEntry();
    } else if (next) {
        expireNextElement();
    }

    return res;
}

ChainedVersionRecord* VersionRecordIterator::retrieveHead(uint64_t key) {
    auto element = reinterpret_cast<ChainedVersionRecord*>(mTable.mHashMap.get(mTable.mTableId, key));
    LOG_ASSERT(!element || (element->key() == key), "Element in hash map is of different key");
    return element;
}

void VersionRecordIterator::expireNextElement() {
    auto next = mCurrentData.next();
    LOG_ASSERT(next, "Next element is null while expiring element");

    auto nextData = next->mutableData();
    while (true) {
        LOG_ASSERT(nextData.validTo() == ChainedVersionRecord::ACTIVE_VERSION, "Next element is already set to expire");
        if (nextData.isInvalid()) {
            next = nextData.next();
            if (!next) {
                return;
            }
            nextData = next->mutableData();
            continue;
        }

        if (next->tryExpire(nextData, mCurrent->validFrom())) {
            return;
        }
    }
}

void VersionRecordIterator::reactivateNextElement() {
    auto next = mCurrentData.next();
    LOG_ASSERT(next, "Next element is null while reactivating element");

    auto nextData = next->mutableData();
    while (true) {
        LOG_ASSERT(nextData.validTo() == mCurrent->validFrom(), "Next element is not set to expire in the current "
                "element\'s version");
        if (nextData.isInvalid()) {
            next = nextData.next();
            if (!next) {
                return;
            }
            nextData = next->mutableData();
            continue;
        }

        if (next->tryReactivate(nextData)) {
            return;
        }
    }
}

bool VersionRecordIterator::insertHead(ChainedVersionRecord* element) {
    LOG_ASSERT(isNewest(), "Insert only supported on the beginning");
    void* actualData = nullptr;
    auto res = (mCurrent ? mTable.mHashMap.update(mTable.mTableId, element->key(), mCurrent, element, &actualData)
                         : mTable.mHashMap.insert(mTable.mTableId, element->key(), element, &actualData));
    if (res) {
        mCurrent = element;
    } else if (actualData) {
        mCurrent = reinterpret_cast<ChainedVersionRecord*>(actualData);
    } else {
        mCurrent = retrieveHead(element->key());
    }
    return res;
}

bool VersionRecordIterator::removeCurrentInvalidFromHead(ChainedVersionRecord* next) {
    LOG_ASSERT(isNewest(), "Remove only supported on the beginning");
    LOG_ASSERT(!done(), "Remove not supported at the end");

    void* actualData = nullptr;
    auto res = (next ? mTable.mHashMap.update(mTable.mTableId, mCurrent->key(), mCurrent, next, &actualData)
                     : mTable.mHashMap.erase(mTable.mTableId, mCurrent->key(), mCurrent, &actualData));
    mCurrent = (res ? next : reinterpret_cast<ChainedVersionRecord*>(actualData));
    return res;
}

bool VersionRecordIterator::removeCurrentInvalidFromPrevious(ChainedVersionRecord* next) {
    LOG_ASSERT(!isNewest(), "Remove not supported on the beginning");
    LOG_ASSERT(!done(), "Remove not supported at the end");

    if (mPrev->tryNext(mPrevData, next)) {
        mCurrent = next;
        return true;
    }

    if (mPrevData.isInvalid()) {
        // The previous element itself got deleted from the version list - Retry from the beginning
        mCurrent = retrieveHead(mPrev->key());
        mPrev = nullptr;
        return false;
    }

    // The previous element is still valid but the next pointer changed
    // This happens when the current entry was recycled - Reset iterator to the updated pointer
    mCurrent = mPrevData.next();
    return false;
}

void VersionRecordIterator::setCurrentEntry() {
    while (mCurrent) {
        LOG_ASSERT(!mPrev || (mCurrent->key() == mPrev->key()), "Element in version list is of different key");

        mCurrentData = mCurrent->mutableData();
        if (!mCurrentData.isInvalid()) {
            return;
        }

        replaceCurrentInvalidElement(mCurrentData.next());
    }

    // Only reached when mRecord is null
    mCurrentData = MutableRecordData();
}

} // namespace logstructured
} // namespace store
} // namespace tell
