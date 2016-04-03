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

const uint16_t gMaxTupleCount = 4u * 1024u;

Record buildScanRecord(ScanQueryType queryType, const char* queryData, const char* queryDataEnd, const Record& record) {
    switch (queryType) {
    case ScanQueryType::FULL: {
        return record;
    } break;

    case ScanQueryType::PROJECTION: {
        Schema schema(TableType::UNKNOWN);
        ProjectionIterator end(queryDataEnd);
        for (ProjectionIterator i(queryData); i != end; ++i) {
            auto& field = record.getFieldMeta(*i).field;
            schema.addField(field.type(), field.name(), field.isNotNull());
        }
        return Record(std::move(schema));
    } break;
    case ScanQueryType::AGGREGATION: {
        Schema schema(TableType::UNKNOWN);
        Record::id_t fieldId = 0u;
        AggregationIterator end(queryDataEnd);
        for (AggregationIterator i(queryData); i != end; ++i, ++fieldId) {
            Record::id_t id;
            AggregationType aggType;
            std::tie(id, aggType) = *i;

            bool notNull;
            switch (aggType) {
            case AggregationType::MIN:
            case AggregationType::MAX:
            case AggregationType::SUM: {
                notNull = false;
            } break;

            case AggregationType::CNT: {
                notNull = true;
            } break;

            default: {
                LOG_ASSERT(false, "Unknown aggregation type");
                notNull = false;
            } break;
            }

            auto& field = record.getFieldMeta(id).field;
            schema.addField(field.aggType(aggType), crossbow::to_string(fieldId), notNull);
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
          mQueryLength(queryLength),
          mSnapshot(std::move(snapshot)),
          mRecord(buildScanRecord(mQueryType, mQueryData.get(), mQueryData.get() + mQueryLength, record)),
          mMinimumLength(mRecord.staticSize() + ScanQueryProcessor::TUPLE_OVERHEAD) {
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
    }

    LOG_DEBUG("Scan processor done [totalWritten = %1%]", mTotalWritten);
}

ScanQueryProcessor::ScanQueryProcessor(ScanQueryProcessor&& other)
        : mData(other.mData),
          mBuffer(std::move(other.mBuffer)),
          mBufferWriter(other.mBufferWriter),
          mTotalWritten(other.mTotalWritten),
          mTupleCount(other.mTupleCount) {
    other.mData = nullptr;
    other.mBuffer = nullptr;
    other.mTotalWritten = 0u;
    other.mTupleCount = 0u;
}

ScanQueryProcessor& ScanQueryProcessor::operator=(ScanQueryProcessor&& other) {
    mData = other.mData;
    other.mData = nullptr;

    mBuffer = other.mBuffer;
    other.mBuffer = nullptr;

    mBufferWriter = std::move(other.mBufferWriter);

    mTotalWritten = other.mTotalWritten;
    other.mTotalWritten = 0u;

    mTupleCount = other.mTupleCount;
    other.mTupleCount = 0u;

    return *this;
}

void ScanQueryProcessor::initAggregationRecord() {
    ensureBufferSpace(mData->minimumLength());

    mBufferWriter.write<uint64_t>(0u);
    auto tupleData = mBufferWriter.data();
    mBufferWriter.set(0, mData->minimumLength() - TUPLE_OVERHEAD);

    auto& record = mData->record();
    auto aggIter = mData->aggregationBegin();
    for (Record::id_t i = 0; i < record.fieldCount(); ++i, ++aggIter) {
        uint16_t destFieldIdx;
        record.idOf(crossbow::to_string(i), destFieldIdx);
        auto& metadata = record.getFieldMeta(destFieldIdx);
        auto& field = metadata.field;
        auto aggType = std::get<1>(*aggIter);
        field.initAgg(aggType, tupleData + metadata.offset);

        // Set all fields that can be NULL to NULL
        // Whenever the first value is written the field will be marked as non-NULL
        if (!field.isNotNull()) {
            record.setFieldNull(tupleData, metadata.nullIdx, true);
        }
    }
}

void ScanQueryProcessor::ensureBufferSpace(uint32_t length) {
    if (mTupleCount < gMaxTupleCount && mBufferWriter.canWrite(length)) {
        return;
    }

    if (mBuffer) {
        mTotalWritten += static_cast<uint64_t>(mBufferWriter.data() - mBuffer);

        std::error_code ec;
        mData->writeOngoing(mBuffer, mBufferWriter.data(), ec);
        if (ec) {
            LOG_ERROR("Error while sending buffer [error = %1% %2%]", ec, ec.message());
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
