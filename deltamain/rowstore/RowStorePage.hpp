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
    RowStorePage(PageManager& pageManager, char* data, Table *table = nullptr)
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

    /**
     * performs a gc step on this page and returns a pointer to a newly filled
     * page that needs to be kept in / added to the page list. New entries are
     * written into the fillList first and then possibly into another newly
     * allocated page. A gc scheduler must call this function on every existing
     * page making sure that if page-ptr is returned, the page is kept active and
     * that if done is set to false, the call is repeated (with a newly allocated
     * fillpage on the same page again).
     */
    char* gc(uint64_t lowestActiveVersion, InsertMap& insertMap, char*& fillPage, bool& done, Modifier& hashTable);
    static void fillWithInserts(uint64_t lowestActiveVersion, InsertMap& insertMap, char*& fillPage, Modifier& hashTable);

    Iterator begin() const;
    Iterator end() const;
};

} // namespace deltamain
} // namespace store
} // namespace tell
