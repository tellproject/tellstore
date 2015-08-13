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

/**
 * Contains the functionality for garbagabe collection and scan of a memory
 * page in column format. Please find the page layout description and the
 * single-record operation details in ColumnMapRecord.hpp.
 */
class ColumnMapPage {
    PageManager& mPageManager;
    char* mData;
    Table *mTable;
public:

    ColumnMapPage(PageManager& pageManager, char* data, Table *table)
        : mPageManager(pageManager)
        , mData(data)
        , mTable(table) {}

    void markCurrentForDeletion() {
        auto oldPage = mData;
        auto& pageManager = mPageManager;
        crossbow::allocator::invoke([oldPage, &pageManager]() { pageManager.free(oldPage); });
    }

    char* gc(uint64_t lowestActiveVersion, InsertMap& insertMap, char*& fillPage, bool& done, Modifier& hashTable);
    static void fillWithInserts(uint64_t lowestActiveVersion, InsertMap& insertMap, char*& fillPage, Modifier& hashTable);
};

} // namespace deltamain
} // namespace store
} // namespace tell
