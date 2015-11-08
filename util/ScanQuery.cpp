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

    // Set all fields that can be NULL to NULL
    // Whenever the first value is written the field will be marked as non-NULL
    auto& record = mData->record();
    if (!record.schema().allNotNull()) {
        for (Record::id_t i = 0; i < record.fieldCount(); ++i) {
            auto& metadata = record.getFieldMeta(i);
            if (!metadata.first.isNotNull()) {
                record.setFieldNull(tupleData, i, true);
            }
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
    auto offset = record.getFieldMeta(fieldId).second;
    if (record.schema().allNotNull()) {
        // If the schema is all non NULL then initialize the aggregation on the first field written
        if (mTupleCount == 0u) {
            field.initAgg(aggType, ptr + offset);
        }
    } else {
        // If the schema can contain NULL values then check if this is the first time we encountered a non NULL value
        // for the field - Initialize the aggregation with the field data
        if (record.isFieldNull(ptr, fieldId)) {
            field.initAgg(aggType, ptr + offset);
            record.setFieldNull(ptr, fieldId, false);
        }
    }
    field.agg(aggType, ptr + offset, data);
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

bool ScanQueryBatchProcessor::check(const char*& query, const char* data, const Record& record) {
    PredicateBitmap bitmap;
    auto numberOfCols = numberOfColumns(query);
    auto current = query + offsetToFirstColumn();
    for (uint64_t i = 0; i < numberOfCols; ++i) {
        auto cId = columnId(current);
        auto predCnt = predicatesCount(current);

        bool isNull;
        FieldType fType;
        const char* field = record.data(data, cId, isNull, &fType);
        FieldBase f(fType);

        current += offsetToFirstPredicate();
        for (decltype(predCnt) i = 0; i < predCnt; ++i) {
            auto bitmapPos = posInQuery(current);
            if (bitmap.test(bitmapPos)) {
                // if one of several ORs is true, we don't need to check the others
                current += offsetToNextPredicate(current, f);
                continue;
            }
            auto type = getTypeOfPredicate(current);

            // we need to handle NULL special
            if (isNull) {
                if (type == PredicateType::IS_NULL) {
                    bitmap.set(bitmapPos);
                }
                current += offsetToNextPredicate(current, f);
                continue;
            }
            // Note: at this point we know that bitmap[bitmapPos] is false
            // we use this fact quite often: instead of using ||, we just assign
            // to the bitmap.
            // Furthermore we do know, that both values are not null
            switch (type) {
            case PredicateType::IS_NOT_NULL:
                bitmap.set(bitmapPos);
            case PredicateType::IS_NULL:
                current += offsetToNextPredicate(current, f);
                break;
            default:
                if (f.queryCmp(type, field, current)) {
                    bitmap.set(bitmapPos);
                }
            }
        }
    }
    query = current;
    return bitmap.all();
}

ScanQueryBatchProcessor::ScanQueryBatchProcessor(const char* queryBuffer, const std::vector<ScanQuery*>& queryData)
        : mQueryBuffer(queryBuffer) {
    mQueries.reserve(queryData.size());
    for (auto q : queryData) {
        mQueries.emplace_back(q->createProcessor());
    }
}

void ScanQueryBatchProcessor::processRecord(const Record& record, uint64_t key, const char* data, uint32_t length,
        uint64_t validFrom, uint64_t validTo) {
    auto query = mQueryBuffer;
    for (auto& impl : mQueries) {
        // Check if the selection string matches the record
        if (!check(query, data, record)) {
            continue;
        }

        // Check if the max version in the query buffer matches
        auto maxVersion = *reinterpret_cast<const uint64_t*>(query);
        query += sizeof(uint64_t);
        if (validFrom > maxVersion || validTo <= maxVersion) {
            continue;
        }

        impl.processRecord(record, key, data, length, validFrom, validTo);
    }
}

off_t ScanQueryBatchProcessor::offsetToNextPredicate(const char* current, const FieldBase& f) {
    auto type = getTypeOfPredicate(current);
    switch (type) {
    case PredicateType::IS_NOT_NULL:
    case PredicateType::IS_NULL:
        return 8;
    default:
        return f.sizeOfPredicate(current);
    }
}

} // namespace store
} // namespace tell
