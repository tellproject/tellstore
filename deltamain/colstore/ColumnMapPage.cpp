#include "ColumnMapPage.hpp"
#include "deltamain/InsertMap.hpp"
#include <util/CuckooHash.hpp>

namespace tell {
namespace store {
namespace deltamain {

char* ColumnMapPage::gc(
        uint64_t lowestActiveVersion,
        InsertMap& insertMap,
        char*& fillPage,
        bool& done,
        Modifier& hashTable)
{
    //TODO: implement
    return nullptr;
}

void ColumnMapPage::fillWithInserts(uint64_t lowestActiveVersion, InsertMap& insertMap, char*& fillPage, Modifier& hashTable)
{
    //TODO: implement
}

} // namespace deltamain
} // namespace store
} // namespace tell

