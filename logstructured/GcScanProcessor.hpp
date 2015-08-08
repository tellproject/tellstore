#pragma once

#include <util/Log.hpp>
#include <util/ScanQuery.hpp>

#include <crossbow/non_copyable.hpp>

#include <cstdint>
#include <vector>

namespace tell {
namespace store {
namespace logstructured {

class ChainedVersionRecord;
class Table;

/**
 * @brief Scan processor for the Log-Structured Memory approach that performs Garbage Collection as part of its scan
 */
class GcScanProcessor : crossbow::non_copyable {
public:
    using LogImpl = Log<UnorderedLogImpl>;

    GcScanProcessor(Table& table, const LogImpl::PageIterator& begin, const LogImpl::PageIterator& end,
            const char* queryBuffer, const std::vector<ScanQuery*>& queryData, uint64_t minVersion);

    GcScanProcessor(GcScanProcessor&& other);

    /**
     * @brief Scans over all entries in the log
     *
     * Processes all valid entries with the associated scan queries.
     *
     * Performs garbage collection while scanning over a page.
     */
    void process();

private:
    /**
     * @brief Advance the entry iterator to the next entry, advancing to the next page if necessary
     *
     * The processor must not be at the end when calling this function.
     */
    bool advanceEntry();

    /**
     * @brief Recycle the given element
     *
     * Copies the element to a recycling page and replaces the old element in the version list with the new element.
     *
     * @param oldElement The element to recycle
     * @param size Size of the old element
     * @param type Type of the old element
     */
    void recycleEntry(ChainedVersionRecord* oldElement, uint32_t size, uint32_t type);

    /**
     * @brief Replaces the given old element with the given new element in the version list of the record
     *
     * The two elements must belong to the same record.
     *
     * @param oldElement Old element to remove from the version list
     * @param newElement New element to replace the old element with
     * @return Whether the replacement was successful
     */
    bool replaceElement(ChainedVersionRecord* oldElement, ChainedVersionRecord* newElement);

    Table& mTable;
    ScanQueryBatchProcessor mQueries;
    uint64_t mMinVersion;

    LogImpl::PageIterator mPagePrev;
    LogImpl::PageIterator mPageIt;
    LogImpl::PageIterator mPageEnd;

    LogPage::EntryIterator mEntryIt;
    LogPage::EntryIterator mEntryEnd;

    LogPage* mRecyclingHead;
    LogPage* mRecyclingTail;

    /// Amount of garbage in the current page
    uint32_t mGarbage;

    /// Whether all entries in the current page were sealed
    bool mSealed;

    /// Whether the current page is being recycled
    /// Initialized to false to prevent the first page from being garbage collected
    bool mRecycle;
};

} // namespace logstructured
} // namespace store
} // namespace tell
