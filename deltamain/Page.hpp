#pragma once

#include <cstdint>
#include <cstddef>

#include <config.h>
#include <util/PageManager.hpp>
#include <util/Epoch.hpp>

#include "Record.hpp"
#include "InsertMap.hpp"

namespace tell {
namespace store {
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

    char* gc(uint64_t lowestActiveVersion, InsertMap& insertMap, char*& fillPage, bool& done);
};

} // namespace deltamain
} // namespace store
} // namespace tell

