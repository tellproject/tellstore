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

#include "QueryBufferScan.hpp"

#include "PredicateBitmap.hpp"

namespace tell {
namespace store {

QueryBufferScanBase::QueryBufferScanBase(std::vector<ScanQuery*> queries)
        : mQueries(std::move(queries)) {
    prepareQueryBuffer();
}

void QueryBufferScanBase::prepareQueryBuffer() {
    if (mQueries.size() == 0) {
        return;
    }
    size_t queryBufferSize = 0;
    for (auto q : mQueries) {
        queryBufferSize += q->selectionLength() + 2 * sizeof(uint64_t);
    }
    mQueryBuffer.reset(new char[queryBufferSize]);

    auto result = mQueryBuffer.get();
    for (auto q : mQueries) {
        // Copy the selection query into the qbuffer
        memcpy(result, q->selection(), q->selectionLength());
        result += q->selectionLength();

        // Write base version field in selection query
        *reinterpret_cast<uint64_t*>(result) = q->snapshot()->baseVersion();
        result += sizeof(uint64_t);

        // Write version field in selection query
        *reinterpret_cast<uint64_t*>(result) = q->snapshot()->version();
        result += sizeof(uint64_t);
    }
}

bool QueryBufferScanProcessorBase::check(const char*& query, uint64_t key, const char* data, const Record& record) {
    PredicateBitmap bitmap;
    auto numberOfCols = numberOfColumns(query);
    auto moduloK = moduloKey(query);
    auto moduloV = moduloValue(query);
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
                if (f.queryCmp(type, data, field, current)) {
                    bitmap.set(bitmapPos);
                }
            }
        }
    }
    query = current;

    if (moduloK != 0) {
        if (key % moduloK != moduloV) {
            return false;
        }
    }

    return bitmap.all();
}

QueryBufferScanProcessorBase::QueryBufferScanProcessorBase(const Record& record, const std::vector<ScanQuery*>& queries,
        const char* queryBuffer)
        : mRecord(record),
          mQueryBuffer(queryBuffer) {
    mQueries.reserve(queries.size());
    for (auto q : queries) {
        mQueries.emplace_back(q->createProcessor());
    }
}

void QueryBufferScanProcessorBase::processRowRecord(uint64_t key, uint64_t validFrom, uint64_t validTo,
        const char* data, uint32_t length) {
    auto query = mQueryBuffer;
    for (auto& impl : mQueries) {
        // Check if the selection string matches the record
        if (!check(query, key, data, mRecord)) {
            query += 2 * sizeof(uint64_t);
            continue;
        }

        // Check if the max version in the query buffer matches
        auto baseVersion = *reinterpret_cast<const uint64_t*>(query);
        query += sizeof(uint64_t);
        auto version = *reinterpret_cast<const uint64_t*>(query);
        query += sizeof(uint64_t);
        if (validFrom > version || validTo <= baseVersion) {
            continue;
        }

        impl.processRecord(mRecord, key, data, length, validFrom, validTo);
    }
}

off_t QueryBufferScanProcessorBase::offsetToNextPredicate(const char* current, const FieldBase& f) {
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
