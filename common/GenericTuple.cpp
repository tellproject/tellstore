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

#include <tellstore/GenericTuple.hpp>

#include <tellstore/Record.hpp>

namespace tell {
namespace store {

AbstractTuple::~AbstractTuple() = default;

GenericTupleSerializer::GenericTupleSerializer(const Record& record, GenericTuple tuple)
    : mRecord(record)
    , mTuple(std::move(tuple))
    , mSize(mRecord.sizeOfTuple(mTuple))
{}

GenericTupleSerializer::~GenericTupleSerializer() = default;

size_t GenericTupleSerializer::size() const {
    return mSize;
}

void GenericTupleSerializer::serialize(char* dest) const {
    mRecord.create(dest, mTuple, mSize);
}

} // namespace store
} // namespace tell
