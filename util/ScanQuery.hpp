#pragma once

#include "PredicateBitmap.hpp"

#include <tellstore/Record.hpp>

#include <commitmanager/SnapshotDescriptor.hpp>

#include <crossbow/byte_buffer.hpp>
#include <crossbow/enum_underlying.hpp>
#include <crossbow/logger.hpp>
#include <crossbow/non_copyable.hpp>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <system_error>
#include <tuple>
#include <vector>

namespace tell {
namespace store {

class ScanQueryProcessor;

/**
 * @brief Iterator for iterating over the query data when the query type is a projection
 */
class ProjectionIterator {
public:
    ProjectionIterator()
            : mPos(nullptr) {
    }

    ProjectionIterator(const char* pos)
            : mPos(pos) {
    }

    ProjectionIterator& operator++() {
        mPos += ENTRY_SIZE;
        return *this;
    }

    ProjectionIterator operator++(int) {
        ProjectionIterator result(*this);
        operator++();
        return result;
    }

    bool operator==(const ProjectionIterator& rhs) const {
        return (mPos == rhs.mPos);
    }

    bool operator!=(const ProjectionIterator& rhs) const {
        return !operator==(rhs);
    }

    Record::id_t operator*() const {
        return *reinterpret_cast<const Record::id_t*>(mPos);
    }

private:
    static constexpr size_t ENTRY_SIZE = 2u;

    const char* mPos;
};

/**
 * @brief @brief Iterator for iterating over the query data when the query type is a aggregation
 */
class AggregationIterator {
public:
    AggregationIterator()
            : mPos(nullptr) {
    }

    AggregationIterator(const char* pos)
            : mPos(pos) {
    }

    AggregationIterator& operator++() {
        mPos += ENTRY_SIZE;
        return *this;
    }

    AggregationIterator operator++(int) {
        AggregationIterator result(*this);
        operator++();
        return result;
    }

    bool operator==(const AggregationIterator& rhs) const {
        return (mPos == rhs.mPos);
    }

    bool operator!=(const AggregationIterator& rhs) const {
        return !operator==(rhs);
    }

    std::tuple<Record::id_t, AggregationType> operator*() const {
        return std::make_tuple(*reinterpret_cast<const Record::id_t*>(mPos),
                *reinterpret_cast<const AggregationType*>(mPos + AGGREGATION_TYPE_OFFSET));
    }

private:
    static constexpr size_t ENTRY_SIZE = 4u;

    static constexpr size_t AGGREGATION_TYPE_OFFSET = 2u;

    const char* mPos;
};

/**
 * @brief Information about a scan query
 */
class ScanQuery {
public:
    ScanQuery(ScanQueryType queryType, std::unique_ptr<char[]> selectionData, size_t selectionLength,
            std::unique_ptr<char[]> queryData, size_t queryLength,
            std::unique_ptr<commitmanager::SnapshotDescriptor> snapshot, const Record& record);

    virtual ~ScanQuery();

    ScanQueryType queryType() const {
        return mQueryType;
    }

    const char* selection() const {
        return mSelectionData.get();
    }

    size_t selectionLength() const {
        return mSelectionLength;
    }

    ProjectionIterator projectionBegin() const {
        LOG_ASSERT(mQueryType == ScanQueryType::PROJECTION, "Query type not projection");
        return ProjectionIterator(mQueryData.get());
    }

    ProjectionIterator projectionEnd() const {
        LOG_ASSERT(mQueryType == ScanQueryType::PROJECTION, "Query type not projection");
        return ProjectionIterator(mQueryDataEnd);
    }

    AggregationIterator aggregationBegin() const {
        LOG_ASSERT(mQueryType == ScanQueryType::AGGREGATION, "Query type not aggregation");
        return AggregationIterator(mQueryData.get());
    }

    AggregationIterator aggregationEnd() const {
        LOG_ASSERT(mQueryType == ScanQueryType::AGGREGATION, "Query type not aggregation");
        return AggregationIterator(mQueryDataEnd);
    }

    const commitmanager::SnapshotDescriptor* snapshot() const {
        return mSnapshot.get();
    }

    const Record& record() const {
        return mRecord;
    }

    uint32_t headerLength() const {
        return mHeaderLength;
    }

    uint32_t minimumLength() const {
        return mMinimumLength;
    }

    /**
     * @brief Acquires a new buffer
     */
    virtual std::tuple<char*, uint32_t> acquireBuffer() = 0;

    /**
     * @brief Flushes the tuples in the buffer to the client
     *
     * @param start Begin pointer to the buffer containing the tuples
     * @param end End pointer to the buffer containing the tuples
     * @param tupleCount Number of tuples contained in the buffer
     * @param ec Error in case the write fails
     */
    virtual void writeOngoing(const char* start, const char* end, uint16_t tupleCount, std::error_code& ec) = 0;

    /**
     * @brief Writes the last tuples to the client
     *
     * The invoking scan processor can be marked as done.
     *
     * @param start Begin pointer to the buffer containing the tuples
     * @param end End pointer to the buffer containing the tuples
     * @param tupleCount Number of tuples contained in the buffer
     * @param ec Error in case the write fails
     */
    virtual void writeLast(const char* start, const char* end, uint16_t tupleCount, std::error_code& ec) = 0;

    /**
     * @brief Writes the last tuples to the client
     *
     * The invoking scan processor can be marked as done.
     *
     * @param ec Error in case the write fails
     */
    virtual void writeLast(std::error_code& ec) = 0;

    /**
     * @brief Create a new ScanQueryProcessor associated with this scan
     */
    virtual ScanQueryProcessor createProcessor() = 0;

private:
    /// The type of the scan query
    ScanQueryType mQueryType;

    /// The selection query
    std::unique_ptr<char[]> mSelectionData;

    /// Length of the selection query string
    size_t mSelectionLength;

    /// The query data
    /// This is null when the query type is a full scan, the projection query in case the query type is a projection or
    /// an aggregation query in case the query type is an aggregation.
    std::unique_ptr<char[]> mQueryData;

    /// Pointer to the end of the query data
    const char* mQueryDataEnd;

    /// Snapshot to check the validity of tuples against
    std::unique_ptr<commitmanager::SnapshotDescriptor> mSnapshot;

    /// Record containing the target schema
    Record mRecord;

    /// Size of the header of the target schema
    uint32_t mHeaderLength;

    /// Minimum size a tuple requires (i.e. minimum static size)
    uint32_t mMinimumLength;
};

/**
 * @brief Processor building the tuples according to the data specified in the scan query
 *
 * Checks the validity of the tuple against a SnapshotDescriptor and writes all valid tuples into a buffer. Performs
 * projection or aggregation depending on the query type.
 */
class ScanQueryProcessor : crossbow::non_copyable {
public:
    ScanQueryProcessor(ScanQuery* data)
            : mData(data),
              mBuffer(nullptr),
              mBufferWriter(static_cast<char*>(nullptr), 0),
              mTupleCount(0u) {
    }

    ~ScanQueryProcessor();

    ScanQueryProcessor(ScanQueryProcessor&& other);

    ScanQueryProcessor& operator=(ScanQueryProcessor&& other);

    /**
     * @brief Process the tuple according to the query data associated with this processor
     *
     * @param record Record of the tuple
     * @param key Key of the tuple
     * @param data Pointer to the tuple's data
     * @param length Length of the tuple
     * @param validFrom Valid-From version of the tuple
     * @param validTo Valid-To version of the tuple
     */
    void processRecord(const Record& record, uint64_t key, const char* data, uint32_t length, uint64_t validFrom,
            uint64_t validTo);

private:
    /**
     * @brief Write the full tuple to the scan buffer
     *
     * @param key Key of the tuple
     * @param data Pointer to the tuple's data
     * @param length Length of the tuple
     */
    void writeFullRecord(uint64_t key, const char* data, uint32_t length);

    /**
     * @brief Write only the projected fields of the tuple to the scan buffer
     *
     * @param record Record of the tuple
     * @param key Key of the tuple
     * @param data Pointer to the tuple's data
     */
    void writeProjectionRecord(const Record& record, uint64_t key, const char* data);

    /**
     * @brief Appends the field from the tuple to the scan buffer
     *
     * @param tupleData Reference pointing to the start of the current tuple
     * @param fieldId Id of the field to write
     * @param type Type of the field to write
     * @param isNull Whether the field is NULL
     * @param data Pointer to the tuple's data
     */
    void writeProjectionField(char*& ptr, Record::id_t fieldId, FieldType type, bool isNull, const char* data);

    /**
     * @brief Write the aggregation of the tuple to the scan buffer
     *
     * Reserves space for the aggregations in the buffer on the first tuple written, subsequent calls update the
     * aggregation values in the buffer directly. The key of the resulting aggregation is always 0.
     *
     * @param record Record of the tuple
     * @param data Pointer to the tuple's data
     */
    void writeAggregationRecord(const Record& record, const char* data);

    /**
     * @brief Initializes the aggregation tuple
     *
     * Reserves space for the aggregations in the buffer and initializes them to zero.
     */
    void initAggregationRecord();

    /**
     * @brief Update the aggregation of the field to the scan buffer
     *
     * @param ptr Pointer to the start of the current tuple
     * @param fieldId Id of the field to aggregate
     * @param type Type of the field to aggregate
     * @param aggType Type of the aggregation
     * @param isNull Whether the field is NULL
     * @param data Pointer to the tuple's data
     */
    void writeAggregationField(char* ptr, Record::id_t fieldId, FieldType type, AggregationType aggType, bool isNull,
            const char* data);

    /**
     * @brief Ensures that the buffer can hold at least the number of bytes
     *
     * The buffer is written and a new buffer will beacquired in case there is no space left in the current buffer.
     *
     * @param Minimum size of the tuple to be written
     */
    void ensureBufferSpace(uint32_t length);

    /// Shared data holding information about the scan
    ScanQuery* mData;

    /// Start pointer of the current buffer
    char* mBuffer;

    /// Writer used to write the tuples into the buffer
    crossbow::buffer_writer mBufferWriter;

    /// Number of tuples written to the buffer
    uint16_t mTupleCount;
};

/**
 * @brief Processor for processing a batch of queries all at once
 *
 * A query has the following form
 * - 8 bytes for the number of columns it has to check
 * - For each column:
 *   - 2 bytes: The column id (called field id in the Record class)
 *   - 2 bytes: The number of predicates it has on the column
 *   - 4 bytes: Padding
 *   - For each predicate:
 *     - 1 byte: The type of the predicate
 *     - 1 byte: Position in the AND-list of queries
 *     - 2 bytes: The data if its size is between 1 and 2 bytes long padding otherwise
 *     - 4 bytes: The data if its size is between 2 and 4 bytes long padding otherwise
 *     - The data if it is larger than 4 bytes or if it is variable size length (padded to 8 bytes)
 * - 8 bytes: The maximum version of the snapshot
 */
class ScanQueryBatchProcessor {
public:
    /**
     * @brief Check if the selection query matches for the given tuple
     *
     * After calling this function query points to the end of the query.
     *
     * @param query Reference pointing to the current selection query
     * @param data Pointer to the tuple's data
     * @param record Record of the tuple
     */
    static bool check(const char*& query, const char* data, const Record& record);

    ScanQueryBatchProcessor(const char* queryBuffer, const std::vector<ScanQuery*>& queryData);

    /**
     * @brief Process the record with all associated scan processors
     *
     * Checks the tuple against the combined query buffer.
     *
     * @param record Record of the tuple
     * @param key Key of the tuple
     * @param data Pointer to the tuple's data
     * @param length Length of the tuple
     * @param validFrom Valid-From version of the tuple
     * @param validTo Valid-To version of the tuple
     */
    void processRecord(const Record& record, uint64_t key, const char* data, uint32_t length, uint64_t validFrom,
            uint64_t validTo);

private:
    constexpr static off_t offsetToFirstColumn() { return 8; }
    constexpr static off_t offsetToFirstPredicate() { return 8; }

    static off_t offsetToNextPredicate(const char* current, const FieldBase& f);

    static uint64_t numberOfColumns(const char* query) {
        return *reinterpret_cast<const uint64_t*>(query);
    }

    static uint16_t columnId(const char* current) {
        return *reinterpret_cast<const uint16_t*>(current);
    }

    static uint16_t predicatesCount(const char* current) {
        return *reinterpret_cast<const uint16_t*>(current + 2);
    }

    static PredicateType getTypeOfPredicate(const char* current) {
        return crossbow::from_underlying<PredicateType>(*reinterpret_cast<const uint8_t*>(current));
    }

    static uint8_t posInQuery(const char* current) {
        return *reinterpret_cast<const uint8_t*>(current + 1);
    }

    const char* mQueryBuffer;

    std::vector<ScanQueryProcessor> mQueries;
};

} //namespace store
} //namespace tell
