#include "ColumnMapScanProcessor.hpp"


namespace tell {
namespace store {
namespace deltamain {


ColumnMapScanProcessor::ColumnMapScanProcessor(const std::shared_ptr<crossbow::allocator>& alloc,
        const PageList* pages,
        size_t pageIdx,
        size_t pageEndIdx,
        const LogIterator& logIter,
        const LogIterator& logEnd,
        PageManager* pageManager,
        const char* queryBuffer,
        const std::vector<ScanQuery*>& queryData,
        const Record* record)
    : mAllocator(alloc)
    , pages(pages)
    , pageIdx(pageIdx)
    , pageEndIdx(pageEndIdx)
    , logIter(logIter)
    , logEnd(logEnd)
    , pageManager(pageManager)
    , query(queryBuffer, queryData)
    , record(record)
    , pageIter(Page(*pageManager, (*pages)[pageIdx]).begin())
    , pageEnd (Page(*pageManager, (*pages)[pageIdx]).end())
    , currKey(0u)
{
}

void ColumnMapScanProcessor::process()
{
    //TODO:implement
}

} // namespace deltamain
} // namespace store
} // namespace tell
