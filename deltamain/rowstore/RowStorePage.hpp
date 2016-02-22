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

#include "RowStoreRecord.hpp"

#include <deltamain/Record.hpp>

#include <commitmanager/SnapshotDescriptor.hpp>

#include <cstdint>
#include <type_traits>
#include <vector>

namespace tell {
namespace store {

class Modifier;
class PageManager;

namespace deltamain {

class alignas(8) RowStoreMainPage {
public:
    template <typename EntryType>
    class IteratorImpl {
    public:
        static constexpr bool is_const_iterator = std::is_const<EntryType>::value;
        using reference = typename std::conditional<is_const_iterator, const EntryType&, EntryType&>::type;
        using pointer = typename std::conditional<is_const_iterator, const EntryType*, EntryType*>::type;

        IteratorImpl(uintptr_t current)
                : mCurrent(current) {
        }

        IteratorImpl<EntryType>& operator++() {
            auto entry = reinterpret_cast<EntryType*>(mCurrent);
            auto offsets = entry->offsetData();
            mCurrent += offsets[entry->versionCount];
            return *this;
        }

        IteratorImpl<EntryType> operator++(int) {
            IteratorImpl<EntryType> result(*this);
            operator++();
            return result;
        }

        bool operator==(const IteratorImpl<EntryType>& rhs) const {
            return (mCurrent == rhs.mCurrent);
        }

        bool operator!=(const IteratorImpl<EntryType>& rhs) const {
            return !operator==(rhs);
        }

        reference operator*() const {
            return *operator->();
        }

        pointer operator->() const {
            return reinterpret_cast<pointer>(mCurrent);
        }

    private:
        /// Current record this iterator is pointing to
        uintptr_t mCurrent;
    };

    using Iterator = IteratorImpl<RowStoreMainEntry>;
    using ConstIterator = IteratorImpl<const RowStoreMainEntry>;

    RowStoreMainPage()
            : mOffset(0u) {
    }

    ConstIterator cbegin() const {
        return ConstIterator(reinterpret_cast<uintptr_t>(data()));
    }

    Iterator begin() {
        return Iterator(reinterpret_cast<uintptr_t>(data()));
    }

    ConstIterator begin() const {
        return cbegin();
    }

    ConstIterator cend() const {
        return ConstIterator(reinterpret_cast<uintptr_t>(data()) + mOffset);
    }

    Iterator end() {
        return Iterator(reinterpret_cast<uintptr_t>(data()) + mOffset);
    }

    ConstIterator end() const {
        return cend();
    }

    bool needsCleaning(uint64_t minVersion) const;

    RowStoreMainEntry* append(uint64_t key, const std::vector<RecordHolder>& elements);

    RowStoreMainEntry* append(const RowStoreMainEntry* record);

private:
    const char* data() const {
        return reinterpret_cast<const char*>(this) + sizeof(RowStoreMainPage);
    }

    char* data() {
        return const_cast<char*>(const_cast<const RowStoreMainPage*>(this)->data());
    }

    uint64_t mOffset;
};

class RowStorePageModifier {
public:
    RowStorePageModifier(const RowStoreContext& /* context */, PageManager& pageManager, Modifier& mainTableModifier,
            uint64_t minVersion)
            : mPageManager(pageManager),
              mMainTableModifier(mainTableModifier),
              mMinVersion(minVersion),
              mFillPage(nullptr) {
    }

    bool clean(RowStoreMainPage* page);

    bool append(InsertRecord& oldRecord);

    std::vector<RowStoreMainPage*> done();

private:
    template <typename Rec>
    bool collectElements(Rec& rec);

    template <typename Rec>
    void recycleEntry(Rec& oldRecord, RowStoreMainEntry* newRecord, bool replace);

    template <typename Fun>
    RowStoreMainEntry* internalAppend(Fun fun);

    PageManager& mPageManager;

    Modifier& mMainTableModifier;

    uint64_t mMinVersion;

    std::vector<RowStoreMainPage*> mPageList;

    RowStoreMainPage* mFillPage;

    std::vector<RecordHolder> mElements;
};

} // namespace deltamain
} // namespace store
} // namespace tell
