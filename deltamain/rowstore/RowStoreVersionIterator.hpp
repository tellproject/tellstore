#pragma once

#include <util/IteratorEntry.hpp>
#include <tellstore/Record.hpp>

namespace tell {
namespace store {
namespace deltamain {

class RowStoreVersionIterator {
    public:
        using IteratorEntry = tell::store::BaseIteratorEntry;
    private:
        IteratorEntry currEntry;
        const Record* record;
        const char* current = nullptr;
        int idx = 0;
        void initRes();

    public:

        RowStoreVersionIterator(const Record* record, const char* current);

        RowStoreVersionIterator() {}

        bool isValid() const { return current != nullptr; }

        RowStoreVersionIterator& operator++();

        const IteratorEntry& operator*() const {
            return currEntry;
        }

        const IteratorEntry* operator->() const {
            return &currEntry;
        }
    };

} // namespace deltamain
} // namespace store
} // namespace tell
