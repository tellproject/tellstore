#pragma once

#include <cstdint>
#include <cstddef>

#include <config.h>
#include <util/PageManager.hpp>
#include <util/Scan.hpp>

#include "deltamain/Record.hpp"
#include "deltamain/InsertMap.hpp"

#include <crossbow/allocator.hpp>

namespace tell {
namespace store {

class CuckooTable;
class Modifier;

namespace deltamain {

class RowStorePage {
    PageManager& mPageManager;
    char* mData;
    uint64_t mStartOffset;
public:
    class Iterator {
        friend class RowStorePage;
    private:
        const char* current;
        Iterator(const char* current) : current(current) {}
    public:
        Iterator() {}
        Iterator(const Iterator&) = default;
        Iterator& operator=(const Iterator&) = default;
    public:
        Iterator& operator++();
        Iterator operator++(int);
        bool operator==(const Iterator& other) const;
        bool operator!=(const Iterator& other) const {
            return !(*this == other);
        }
        const char* operator*() const;
    };
    RowStorePage(PageManager& pageManager, char* data)
        : mPageManager(pageManager)
        , mData(data)
        , mStartOffset(8) {}


    uint64_t usedMemory(const char* data) const {
        return uint64_t(*reinterpret_cast<const uint64_t*>(data));
    }

    uint64_t usedMemory() const {
        return usedMemory(mData);
    }

    void markCurrentForDeletion() {
        auto oldPage = mData;
        auto& pageManager = mPageManager;
        crossbow::allocator::invoke([oldPage, &pageManager]() { pageManager.free(oldPage); });
    }

    char* gc(uint64_t lowestActiveVersion, InsertMap& insertMap, char*& fillPage, bool& done, Modifier& hashTable);
    static void fillWithInserts(uint64_t lowestActiveVersion, InsertMap& insertMap, char*& fillPage, Modifier& hashTable);

    Iterator begin() const;
    Iterator end() const;
};

} // namespace deltamain
} // namespace store
} // namespace tell
