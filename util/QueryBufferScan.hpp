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

#include <util/ScanQuery.hpp>

#include <tellstore/Record.hpp>

#include <commitmanager/SnapshotDescriptor.hpp>

#include <crossbow/enum_underlying.hpp>
#include <crossbow/logger.hpp>
#include <crossbow/non_copyable.hpp>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

namespace tell {
namespace store {

class QueryBufferScanBase {
protected:
    QueryBufferScanBase(std::vector<ScanQuery*> queries);

    ~QueryBufferScanBase() = default;

    std::vector<ScanQuery*> mQueries;

    std::unique_ptr<char[]> mQueryBuffer;

private:
    void prepareQueryBuffer();
};

/**
 * @brief Processor for processing a batch of queries all at once
 */
class QueryBufferScanProcessorBase {
public:
    /**
     * @brief Check if the selection query matches for the given tuple
     *
     * After calling this function query points to the end of the query.
     *
     * @param query Reference pointing to the current selection query
     * @param key Key of the tuple
     * @param data Pointer to the tuple's data
     * @param record Record of the tuple
     */
    static bool check(const char*& query, uint64_t key, const char* data, const Record& record);

protected:
    QueryBufferScanProcessorBase(const Record& record, const std::vector<ScanQuery*>& queries, const char* queryBuffer);

    /**
     * @brief Process the record with all associated scan processors
     *
     * Checks the tuple against the combined query buffer.
     *
     * @param key Key of the tuple
     * @param validFrom Valid-From version of the tuple
     * @param validTo Valid-To version of the tuple
     * @param data Pointer to the tuple's data
     * @param length Length of the tuple
     */
    void processRowRecord(uint64_t key, uint64_t validFrom, uint64_t validTo, const char* data, uint32_t length);

    const Record& mRecord;

    std::vector<ScanQueryProcessor> mQueries;

private:
    constexpr static off_t offsetToFirstColumn() { return 16; }
    constexpr static off_t offsetToFirstPredicate() { return 8; }

    static off_t offsetToNextPredicate(const char* current, const FieldBase& f);

    static uint32_t numberOfColumns(const char* query) {
        return *reinterpret_cast<const uint32_t*>(query);
    }

    static uint32_t numberOfConjuncts(const char* query) {
        return *reinterpret_cast<const uint32_t*>(query + 4);
    }

    static uint32_t moduloKey(const char* query) {
        return *reinterpret_cast<const uint32_t*>(query + 8);
    }

    static uint32_t moduloValue(const char* query) {
        return *reinterpret_cast<const uint32_t*>(query + 12);
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
};

} //namespace store
} //namespace tell
