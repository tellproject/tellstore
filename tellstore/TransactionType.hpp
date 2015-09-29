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

/**
 * @brief Type of a transaction
 *
 *  - A READ_WRITE transaction is a default transaction, allowed
 *    to execute read as well as write operations.
 *  - A READ_ONLY transaction will not be allowed to issue any write
 *    requests. This allows Tell to do several optimizations. The most
 *    important is, that other transactions might have its version
 *    number in their read sets (since there won't be any tuples with
 *    that version, they don't have to exclude this version from their
 *    read set).
 *  - An ANALYTICAL transaction is a special case of a READ_WRITE
 *    transaction. Its read set will only consist of one base version.
 *    Therefore it won't see the newest data. On the other hand, this
 *    will allow for faster scans (and they won't violate any transacional
 *    guarantees).
 */
enum class TransactionType : uint8_t {
    READ_WRITE,
    READ_ONLY,
    ANALYTICAL,
};

} // namespace store
} // namespace tell
