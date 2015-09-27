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
#include "deltamain/InsertMap.hpp"
#include "deltamain/Record.hpp"

#include <util/CuckooHash.hpp>

namespace tell {
namespace store {
namespace deltamain {

auto RowStorePage::Iterator::operator++() -> Iterator&
{
    CDMRecord rec(current);
    current += rec.size();
    return *this;
}

auto RowStorePage::Iterator::operator++(int) -> Iterator
{
    auto res = *this;
    ++(*this);
    return res;
}

const char* RowStorePage::Iterator::operator*() const {
    return current;
}

bool RowStorePage::Iterator::operator== (const Iterator& other) const {
    return current == other.current;
}

auto RowStorePage::begin() const -> Iterator {
    return Iterator(mData + 8);
}

auto RowStorePage::end() const -> Iterator {
    return Iterator(mData + mSize);
}

//TODO: question: this implementation relies on the fact that fresh pages are meset to 0s. Is that actually the case?
//TODO: question: will mStartOffset be valid for the whole GC phase? Can it happen that the page is called later again
// by GC, but with a newly generated Page object (and mStartIndex reset to 8)?
char* RowStorePage::gc(uint64_t lowestActiveVersion,
        InsertMap& insertMap,
        bool& done,
        Modifier& hashTable)
{
        // We iterate throgh our page
        uint64_t offset = mStartOffset;
        // in the first iteration we just decide wether we
        // need to collect any garbage here
        bool hasToClean = mStartOffset != 8;
        while (offset <= mSize && !hasToClean) {
            CDMRecord rec(mData + offset);
            if (rec.needsCleaning(lowestActiveVersion, insertMap)) {
                hasToClean = true;
                break;
            }
            offset += rec.size();
        }
        if (!hasToClean) {
            // we are done - no cleaning needed for this page
            done = true;
            return mData;
        }

        // At this point we know that we will need to clean the page
        // if its the first gc call to that page, we have to mark it for deletion
        if (mStartOffset == 8)
            markCurrentForDeletion();

        // construct new fill page if needed
        constructFillPage();

        offset = mStartOffset;
        while (offset < mSize) {
            CDMRecord rec(mData + offset);
            bool couldRelocate = false;
            auto nSize = rec.copyAndCompact(lowestActiveVersion,
                    insertMap,
                    mFillPage + mFillOffset,
                    TELL_PAGE_SIZE - mFillOffset,
                    couldRelocate);
            if (!couldRelocate) {
                // The current fillPage is full
                // In this case we set its used memory, return it and set mFillPage to null
                *reinterpret_cast<uint64_t*>(mFillPage) = mFillOffset;
                auto res = mFillPage;
                mFillPage = nullptr;
                done = false;
                return res;
            }
            mFillOffset += nSize;
            hashTable.insert(rec.key(), mFillPage + mFillOffset, true);
            offset += rec.size();
        }

        // we are done, but the fillpage is (most probably) not full yet
        done = true;
        return nullptr;
}

char *RowStorePage::fillWithInserts(uint64_t lowestActiveVersion, InsertMap& insertMap, Modifier& hashTable)
{
    // construct new fill page if needed
    constructFillPage();

    char dummyRecord[40];
    dummyRecord[0] = crossbow::to_underlying(RecordType::MULTI_VERSION_RECORD);
    // there are 0 number of versions
    *reinterpret_cast<uint32_t*>(dummyRecord + 4) = 1;
    *reinterpret_cast<const char**>(dummyRecord + 16) = nullptr;
    *reinterpret_cast<uint64_t*>(dummyRecord + 24) = 0;
    *reinterpret_cast<uint32_t*>(dummyRecord + 32) = 40;
    *reinterpret_cast<uint32_t*>(dummyRecord + 36) = 40;
    DMRecord dummy(dummyRecord);
    while (!insertMap.empty()) {
        bool couldRelocate;
        auto fst = insertMap.begin();
        uint64_t key = fst->first.key;
        // since we truncate the log only on a page level, it could be that
        // there are still some inserts that got processed in the previous GC phase
        if (hashTable.get(key)) {
            insertMap.erase(fst);
            continue;
        }
        auto pos = mFillPage + mFillOffset;
        dummy.writeKey(key);
        mFillOffset += dummy.copyAndCompact(lowestActiveVersion,
                insertMap,
                pos,
                TELL_PAGE_SIZE - mFillOffset,
                couldRelocate);
        if (couldRelocate) {
            hashTable.insert(key, pos);
            insertMap.erase(fst);
        } else {
            break;
        }
    }
    *reinterpret_cast<uint64_t*>(mFillPage) = mFillOffset;
    auto res = mFillPage;
    mFillPage = nullptr;
    return res;
}

} // namespace deltamain
} // namespace store
} // namespace tell

