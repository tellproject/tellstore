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
#include <cstdint>

namespace tell {
namespace store {

enum class TableType : uint8_t {
    UNKNOWN,
    TRANSACTIONAL,
    NON_TRANSACTIONAL,
};

enum class FieldType
    : uint16_t {
    NOTYPE = 0,
    NULLTYPE = 1,
    SMALLINT = 2,
    INT,
    BIGINT,
    FLOAT,
    DOUBLE,
    TEXT, // this is used for CHAR and VARCHAR as well
    BLOB
};

enum class PredicateType : uint8_t {
    EQUAL = 1,
    NOT_EQUAL,
    LESS,
    LESS_EQUAL,
    GREATER,
    GREATER_EQUAL,
    LIKE,
    NOT_LIKE,
    IS_NULL,
    IS_NOT_NULL
};

enum class AggregationType : uint8_t {
    MIN = 1,
    MAX,
    SUM,
    CNT,
};

enum ScanQueryType : uint8_t {
    FULL = 0x1u,
    PROJECTION,
    AGGREGATION,
};

} // namespace store
} // namespace tell
