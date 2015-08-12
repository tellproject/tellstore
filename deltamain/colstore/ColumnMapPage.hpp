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

class ColumnMapPage {
    PageManager& mPageManager;
    char* mData;
public:

    ColumnMapPage(PageManager& pageManager, char* data)
        : mPageManager(pageManager)
        , mData(data) {}

    char* gc(uint64_t lowestActiveVersion, InsertMap& insertMap, char*& fillPage, bool& done, Modifier& hashTable);
    static void fillWithInserts(uint64_t lowestActiveVersion, InsertMap& insertMap, char*& fillPage, Modifier& hashTable);
};

} // namespace deltamain
} // namespace store
} // namespace tell
