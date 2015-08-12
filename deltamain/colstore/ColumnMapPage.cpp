#include "ColumnMapPage.hpp"
#include "deltamain/InsertMap.hpp"
#include "deltamain/Record.hpp"

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
    CDMRecord rec(mData + 8);
    if (rec.needsCleaning(lowestActiveVersion, insertMap)) {
        // we are done - no cleaning needed for this page
        done = true;
        return mData;
    }
    //TODO: continue here

    return nullptr;
}

void ColumnMapPage::fillWithInserts(uint64_t lowestActiveVersion, InsertMap& insertMap, char*& fillPage, Modifier& hashTable)
{
    //TODO: implement
}

} // namespace deltamain
} // namespace store
} // namespace tell

