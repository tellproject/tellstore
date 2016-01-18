/*
 * (C) Copyright 2015 ETH Zurich Systems Group (http://www.systems.ethz.ch/) and others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Contributors:
 *     Markus Pilman <mpilman@inf.ethz.ch>
 *     Simon Loesing <sloesing@inf.ethz.ch>
 *     Thomas Etter <etterth@gmail.com>
 *     Kevin Bocksrocker <kevin.bocksrocker@gmail.com>
 *     Lucas Braun <braunl@inf.ethz.ch>
 */

#pragma once

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
 *
 * A query has the following form
 * - 4 bytes for the number of columns it has to check
 * - 2 bytes for the number of conjuncts in the query
 * - 2 bytes for the number of bits to shift the key before partitioning
 * - 4 bytes for the number of total scan partitions (or 0 if no partitioning)
 * - 4 bytes for the number of the scan partition (if partitioning)
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
     * @param ec Error in case the write fails
     */
    virtual void writeOngoing(const char* start, const char* end, std::error_code& ec) = 0;

    /**
     * @brief Writes the last tuples to the client
     *
     * The invoking scan processor can be marked as done.
     *
     * @param start Begin pointer to the buffer containing the tuples
     * @param end End pointer to the buffer containing the tuples
     * @param ec Error in case the write fails
     */
    virtual void writeLast(const char* start, const char* end, std::error_code& ec) = 0;

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
    static constexpr size_t TUPLE_OVERHEAD = sizeof(uint64_t);

    ScanQueryProcessor(ScanQuery* data)
            : mData(data),
              mBuffer(nullptr),
              mBufferWriter(static_cast<char*>(nullptr), 0),
              mTotalWritten(0u),
              mTupleCount(0u) {
    }

    ~ScanQueryProcessor();

    ScanQueryProcessor(ScanQueryProcessor&& other);

    ScanQueryProcessor& operator=(ScanQueryProcessor&& other);

    const ScanQuery* data() const {
        return mData;
    }

    /**
     * @brief Process the tuple according to the query data associated with this processor
     *
     * @param record Record of the tuple
     * @param key Key of the tuple
     * @param length Length of the tuple
     * @param validFrom Valid-From version of the tuple
     * @param validTo Valid-To version of the tuple
     * @param fun Function to serialize the record
     */
    template <typename Fun>
    void writeRecord(uint64_t key, uint32_t length, uint64_t validFrom, uint64_t validTo, Fun fun);

    /**
     * @brief Initializes the aggregation tuple
     *
     * Reserves space for the aggregations in the buffer and initializes them to zero.
     */
    void initAggregationRecord();

//private:
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

    /// Total number of bytes written
    size_t mTotalWritten;

    /// Number of tuples written to the buffer
    uint16_t mTupleCount;
};

template <typename Fun>
void ScanQueryProcessor::writeRecord(uint64_t key, uint32_t length, uint64_t validFrom, uint64_t validTo, Fun fun) {
    auto snapshot = mData->snapshot();
    if (snapshot && !snapshot->inReadSet(validFrom, validTo)) {
        return;
    }

    if (mData->queryType() == ScanQueryType::AGGREGATION) {
        fun(mBuffer + 8);
    } else {
        ensureBufferSpace(length + TUPLE_OVERHEAD);

        // Write key
        mBufferWriter.write<uint64_t>(key);

        // Write complete tuple
        auto bytesWritten = fun(mBufferWriter.data());
        LOG_ASSERT(bytesWritten <= length, "Bytes written must be smaller than the length");

        mBufferWriter.advance(crossbow::align(bytesWritten, 8u));

        ++mTupleCount;
    }
}

} //namespace store
} //namespace tell
