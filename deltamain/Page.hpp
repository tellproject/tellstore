#pragma once

#include <cstdint>
#include <cstddef>

#include <config.h>
#include <util/PageManager.hpp>
#include <util/Epoch.hpp>
#include <util/Scan.hpp>

#include "Record.hpp"
#include "InsertMap.hpp"

namespace tell {
namespace store {

class CuckooTable;
class Modifier;

namespace deltamain {

class Page {
    /**
     * This class is used to
     * recycle pages in a save
     * manner.
     */
    class DummyPage {
    };
    PageManager& mPageManager;
    char* mData;
    uint64_t mStartOffset;
public:
    class Iterator {
        friend class Page;
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
    Page(PageManager& pageManager, char* data)
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
        allocator::free(new (allocator::malloc(sizeof(DummyPage))) DummyPage(),
                [oldPage, &pageManager]() { pageManager.free(oldPage); });
    }

    char* gc(uint64_t lowestActiveVersion, InsertMap& insertMap, char*& fillPage, bool& done, Modifier& hashTable);
    static void fillWithInserts(uint64_t lowestActiveVersion, InsertMap& insertMap, char*& fillPage, Modifier& hashTable);

    Iterator begin() const;
    Iterator end() const;
};

} // namespace deltamain
} // namespace store
} // namespace tell
