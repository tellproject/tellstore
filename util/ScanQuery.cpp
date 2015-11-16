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

#include "ScanQuery.hpp"

#include <crossbow/alignment.hpp>

namespace tell {
namespace store {
namespace {

const uint16_t gMaxTupleCount = 250;

Record buildScanRecord(ScanQueryType queryType, const char* queryData, const char* queryDataEnd, const Record& record) {
    switch (queryType) {
    case ScanQueryType::FULL: {
        return record;
    } break;

    case ScanQueryType::PROJECTION: {
        Schema schema(TableType::UNKNOWN);
        ProjectionIterator end(queryDataEnd);
        for (ProjectionIterator i(queryData); i != end; ++i) {
            auto& field = record.getFieldMeta(*i).first;
            schema.addField(field.type(), field.name(), field.isNotNull());
        }
        return Record(std::move(schema));
    } break;
    case ScanQueryType::AGGREGATION: {
        Schema schema(TableType::UNKNOWN);
        Record::id_t fieldId = 0u;
        AggregationIterator end(queryDataEnd);
        for (AggregationIterator i(queryData); i != end; ++i) {
            auto& field = record.getFieldMeta(std::get<0>(*i)).first;
            schema.addField(field.type(), crossbow::to_string(fieldId), field.isNotNull());
            ++fieldId;
        }
        return Record(std::move(schema));
    } break;
    default: {
        LOG_ASSERT(false, "Unknown scan query type");
        return Record();
    } break;
    }
}

} // anonymous namespace

ScanQuery::ScanQuery(ScanQueryType queryType, std::unique_ptr<char[]> selectionData, size_t selectionLength,
        std::unique_ptr<char[]> queryData, size_t queryLength,
        std::unique_ptr<commitmanager::SnapshotDescriptor> snapshot, const Record& record)
        : mQueryType(queryType),
          mSelectionData(std::move(selectionData)),
          mSelectionLength(selectionLength),
          mQueryData(std::move(queryData)),
          mQueryDataEnd(mQueryData.get() + queryLength),
          mSnapshot(std::move(snapshot)),
          mRecord(buildScanRecord(mQueryType, mQueryData.get(), mQueryDataEnd, record)),
          mHeaderLength(mRecord.headerSize()),
          mMinimumLength(mRecord.minimumSize() + ScanQueryProcessor::TUPLE_OVERHEAD) {
}

ScanQuery::~ScanQuery() = default;

ScanQueryProcessor::~ScanQueryProcessor() {
    if (!mData) {
        return;
    }

    std::error_code ec;
    if (mBuffer) {
        mData->writeLast(mBuffer, mBufferWriter.data(), ec);
    } else {
        LOG_ASSERT(mTupleCount == 0, "Invalid buffer containing tuples");
        mData->writeLast(ec);
    }

    if (ec) {
        // TODO FIXME This leads to a leak (the ServerSocket does not notice that the scan has finished)
        LOG_ERROR("Error while flushing buffer [error = %1% %2%]", ec, ec.message());
        return;
    }
}

ScanQueryProcessor::ScanQueryProcessor(ScanQueryProcessor&& other)
        : mData(other.mData),
          mBuffer(std::move(other.mBuffer)),
          mBufferWriter(other.mBufferWriter),
          mTupleCount(other.mTupleCount) {
    other.mData = nullptr;
    other.mBuffer = nullptr;
    other.mTupleCount = 0u;
}

ScanQueryProcessor& ScanQueryProcessor::operator=(ScanQueryProcessor&& other) {
    mData = other.mData;
    other.mData = nullptr;

    mBuffer = other.mBuffer;
    other.mBuffer = nullptr;

    mBufferWriter = std::move(other.mBufferWriter);

    mTupleCount = other.mTupleCount;
    other.mTupleCount = 0u;

    return *this;
}

void ScanQueryProcessor::processRecord(const Record& record, uint64_t key, const char* data, uint32_t length,
        uint64_t validFrom, uint64_t validTo) {
    auto snapshot = mData->snapshot();
    if (snapshot && !snapshot->inReadSet(validFrom, validTo)) {
        return;
    }

    switch (mData->queryType()) {
    case ScanQueryType::FULL: {
        writeFullRecord(key, data, length);
    } break;

    case ScanQueryType::PROJECTION: {
        writeProjectionRecord(record, key, data);
    } break;

    case ScanQueryType::AGGREGATION: {
        writeAggregationRecord(record, data);
    } break;

    default: {
        LOG_ASSERT(false, "Unknown Scan Query Type");
    } break;
    }
}

void ScanQueryProcessor::writeFullRecord(uint64_t key, const char* data, uint32_t length) {
    ensureBufferSpace(length + TUPLE_OVERHEAD);

    // Write key
    mBufferWriter.write<uint64_t>(key);

    // Copy complete tuple
    mBufferWriter.write(data, length);

    ++mTupleCount;
}

void ScanQueryProcessor::writeProjectionRecord(const Record& record, uint64_t key, const char* data) {
    ensureBufferSpace(mData->minimumLength());

    // Write key
    mBufferWriter.write<uint64_t>(key);
    auto tupleData = mBufferWriter.data();
    mBufferWriter.set(0, mData->headerLength());

    Record::id_t fieldId = 0u;
    auto end = mData->projectionEnd();
    for (auto i = mData->projectionBegin(); i != end; ++i) {
        bool isNull;
        FieldType type;
        auto field = record.data(data, *i, isNull, &type);
        writeProjectionField(tupleData, fieldId, type, isNull, field);
        ++fieldId;
    }

    // Align the tuple to 8 byte
    // No need to check for space as the buffers are required to be a multiple of 8 bytes
    mBufferWriter.align(8);

    ++mTupleCount;
}

void ScanQueryProcessor::writeProjectionField(char*& ptr, Record::id_t fieldId, FieldType type, bool isNull,
        const char* data) {
    FieldBase field(type);
    auto fieldLength = field.sizeOf(data);
    if (!field.isFixedSized()) {
        mBufferWriter.align(4u);
    }
    if (!mBufferWriter.canWrite(fieldLength)) {
        char* newBuffer;
        uint32_t newBufferLength;
        std::tie(newBuffer, newBufferLength) = mData->acquireBuffer();
        if (!newBuffer) {
            // TODO Handle error
            throw std::runtime_error("Error acquiring buffer");
        }
        crossbow::buffer_writer newBufferWriter(newBuffer, newBufferLength);

        if (mBuffer) {
            // Copy the incomplete tuple into the new buffer (including the preceding key)
            auto dataStart = ptr - TUPLE_OVERHEAD;
            auto dataLength = static_cast<size_t>(mBufferWriter.data() - dataStart);
            newBufferWriter.write(dataStart, dataLength);

            // Send the old buffer up to the incomplete tuple
            std::error_code ec;
            mData->writeOngoing(mBuffer, dataStart, ec);
            if (ec) {
                // TODO Handle error (Release buffer)
                throw std::system_error(ec);
            }
        }

        mBuffer = newBuffer;
        mBufferWriter = std::move(newBufferWriter);
        ptr = mBuffer + TUPLE_OVERHEAD;
        mTupleCount = 0u;

        if (!field.isFixedSized()) {
            mBufferWriter.align(4u);
        }

        if (!mBufferWriter.canWrite(fieldLength)) {
            // TODO Handle error
            throw std::runtime_error("Trying to write too much data into the buffer");
        }
    }

    if (isNull) {
        auto& record = mData->record();
        record.setFieldNull(ptr, fieldId, true);
    }

    mBufferWriter.write(data, fieldLength);
}

void ScanQueryProcessor::writeAggregationRecord(const Record& record, const char* data) {
    if (mTupleCount == 0u) {
        initAggregationRecord();
    }

    auto tupleData = mBuffer + TUPLE_OVERHEAD;

    Record::id_t fieldId = 0u;
    auto end = mData->aggregationEnd();
    for (auto i = mData->aggregationBegin(); i != end; ++i) {
        Record::id_t id;
        AggregationType aggType;
        std::tie(id, aggType) = *i;

        bool isNull;
        FieldType fieldType;
        auto field = record.data(data, id, isNull, &fieldType);
        writeAggregationField(tupleData, fieldId, fieldType, aggType, isNull, field);
        ++fieldId;
    }

    mTupleCount = 1u;
}

void ScanQueryProcessor::initAggregationRecord() {
    ensureBufferSpace(mData->minimumLength());

    mBufferWriter.write<uint64_t>(0u);
    auto tupleData = mBufferWriter.data();
    mBufferWriter.set(0, mData->minimumLength() - TUPLE_OVERHEAD);

    auto& record = mData->record();
    auto aggIter = mData->aggregationBegin();
    for (Record::id_t i = 0; i < record.fieldCount(); ++i, ++aggIter) {
        auto& metadata = record.getFieldMeta(i);
        auto& field = metadata.first;
        auto aggType = std::get<1>(*aggIter);
        field.initAgg(aggType, tupleData + metadata.second);

        // Set all fields that can be NULL to NULL
        // Whenever the first value is written the field will be marked as non-NULL
        if (!field.isNotNull()) {
            record.setFieldNull(tupleData, i, true);
        }
    }
}

void ScanQueryProcessor::writeAggregationField(char* ptr, Record::id_t fieldId, FieldType type, AggregationType aggType,
        bool isNull, const char* data) {
    if (isNull) {
        return;
    }

    FieldBase field(type);
    auto& record = mData->record();
    auto& metadata = record.getFieldMeta(fieldId);
    // If the schema can contain NULL values then check if this is the first time we encountered a non NULL value for
    // the field - Initialize the aggregation with the field data
    if (!metadata.first.isNotNull()) {
        record.setFieldNull(ptr, fieldId, false);
    }
    field.agg(aggType, ptr + metadata.second, data);
}

void ScanQueryProcessor::ensureBufferSpace(uint32_t length) {
    if (mTupleCount < gMaxTupleCount && mBufferWriter.canWrite(length)) {
        return;
    }

    if (mBuffer) {
        std::error_code ec;
        mData->writeOngoing(mBuffer, mBufferWriter.data(), ec);
        if (ec) {
            // TODO Handle error (Release buffer)
            throw std::system_error(ec);
        }
    }

    uint32_t bufferLength;
    std::tie(mBuffer, bufferLength) = mData->acquireBuffer();
    if (!mBuffer) {
        // TODO Handle error
        throw std::runtime_error("Error acquiring buffer");
    }
    mBufferWriter = crossbow::buffer_writer(mBuffer, bufferLength);
    mTupleCount = 0u;

    if (!mBufferWriter.canWrite(length)) {
        // TODO Handle error
        throw std::runtime_error("Trying to write too much data into the buffer");
    }
}

} // namespace store
} // namespace tell
