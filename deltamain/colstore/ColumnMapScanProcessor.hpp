#pragma once

#include <util/Log.hpp>

#include "deltamain/Page.hpp"

namespace tell {
namespace store {
namespace deltamain {

class ColumnMapScanProcessor {
private:
    friend class Table;
    using LogIterator = Log<OrderedLogImpl>::ConstLogIterator;
    using PageList = std::vector<char*>;
private: // assigned members
    std::shared_ptr<crossbow::allocator> mAllocator;
    const PageList* pages;
    size_t pageIdx;
    size_t pageEndIdx;
    LogIterator logIter;
    LogIterator logEnd;
    PageManager* pageManager;
    ScanQueryBatchProcessor query;
    const Record* record;
private: // calculated members

    Page::Iterator pageIter;
    Page::Iterator pageEnd;
    uint64_t currKey;
public:
    ColumnMapScanProcessor(const std::shared_ptr<crossbow::allocator>& alloc,
             const PageList* pages,
             size_t pageIdx,
             size_t pageEndIdx,
             const LogIterator& logIter,
             const LogIterator& logEnd,
             PageManager* pageManager,
             const char* queryBuffer,
             const std::vector<ScanQuery*>& queryData,
             const Record* record);

    void process();
};

} // namespace deltamain
} // namespace store
} // namespace tell
