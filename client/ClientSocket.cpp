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
#include <tellstore/ClientSocket.hpp>
#include <tellstore/AbstractTuple.hpp>

#include <commitmanager/SnapshotDescriptor.hpp>

#include <crossbow/infinio/Endpoint.hpp>
#include <crossbow/infinio/InfinibandBuffer.hpp>
#include <crossbow/logger.hpp>

namespace tell {
namespace store {

AbstractTuple::~AbstractTuple() {}

namespace {

uint32_t messageAlign(uint32_t size, uint32_t alignment) {
    return ((size % alignment != 0) ? (alignment - (size % alignment)) : 0);
}

void writeSnapshot(crossbow::buffer_writer& message, const commitmanager::SnapshotDescriptor& snapshot) {
    // TODO Implement snapshot caching
    message.write<uint8_t>(0x0u); // Cached
    message.write<uint8_t>(0x1u); // HasDescriptor
    message.align(sizeof(uint64_t));
    snapshot.serialize(message);
}

} // anonymous namespace

void CreateTableResponse::processResponse(crossbow::buffer_reader& message) {
    auto tableId = message.read<uint64_t>();
    setResult(tableId);
}

void GetTableResponse::processResponse(crossbow::buffer_reader& message) {
    auto tableId = message.read<uint64_t>();
    auto schema = Schema::deserialize(message);

    setResult(tableId, std::move(schema));
}

void GetResponse::processResponse(crossbow::buffer_reader& message) {
    setResult(Tuple::deserialize(message));
}

void ModificationResponse::processResponse(crossbow::buffer_reader& message) {
    auto succeeded = (message.read<uint8_t>() != 0x0u);
    setResult(succeeded);
}

ScanResponse::ScanResponse(crossbow::infinio::Fiber& fiber, std::shared_ptr<ScanIterator> iterator,
        ClientSocket& socket, ScanMemory memory, uint16_t scanId)
        : crossbow::infinio::RpcResponse(fiber),
          mIterator(std::move(iterator)),
          mSocket(socket),
          mMemory(std::move(memory)),
          mScanId(scanId),
          mOffsetRead(0u),
          mOffsetWritten(0u) {
    LOG_ASSERT(mMemory.valid(), "Memory not valid");
}

std::tuple<const char*, const char*> ScanResponse::nextChunk() {
    LOG_ASSERT(available(), "No data available");

    auto start = reinterpret_cast<const char*>(mMemory.data()) + mOffsetRead;
    auto end = reinterpret_cast<const char*>(mMemory.data()) + mOffsetWritten;
    mOffsetRead = mOffsetWritten;

    if (!done()) {
        mSocket.scanProgress(mScanId, shared_from_this(), mOffsetRead);
    }

    return std::make_tuple(start, end);
}

void ScanResponse::onResponse(uint32_t messageType, crossbow::buffer_reader& message) {
    if (messageType == std::numeric_limits<uint32_t>::max()) {
        onAbort(std::error_code(message.read<uint64_t>(), error::get_error_category()));
        return;
    }
    if (messageType != crossbow::to_underlying(ResponseType::SCAN)) {
        onAbort(crossbow::infinio::error::wrong_type);
        return;
    }

    // The scan already completed successfully
    if (done()) {
        return;
    }
    auto scanDone = (message.read<uint8_t>() != 0u);

    message.advance(sizeof(size_t) - sizeof(uint8_t));
    auto offset = message.read<size_t>();
    if (mOffsetWritten < offset) {
        mOffsetWritten = offset;
    }

    if (scanDone) {
        complete();
        mSocket.scanComplete(mScanId);
    }

    if (auto it = mIterator.lock()) {
        it->notify();
    }
}

void ScanResponse::onAbort(std::error_code ec) {
    // The scan already completed successfully
    if (done()) {
        return;
    }
    complete();
    mSocket.scanComplete(mScanId);

    if (auto it = mIterator.lock()) {
        it->abort(std::move(ec));
    }
}

ScanIterator::ScanIterator(crossbow::infinio::Fiber& fiber, Record record, size_t shardSize)
        : mFiber(fiber),
          mRecord(std::move(record)),
          mWaiting(false),
          mChunkPos(nullptr),
          mChunkEnd(nullptr) {
    mScans.reserve(shardSize);
}

bool ScanIterator::hasNext() {
    while (mChunkPos == nullptr) {
        auto done = true;
        for (auto& response : mScans) {
            done = (done && response->done());
            if (!response->available()) {
                continue;
            }
            std::tie(mChunkPos, mChunkEnd) = response->nextChunk();
            return true;
        }
        if (done) {
            return false;
        }
        mWaiting = true;
        mFiber.wait();
        mWaiting = false;
    }
    return true;
}

std::tuple<uint64_t, const char*, size_t> ScanIterator::next() {
    if (!hasNext()) {
        throw std::out_of_range("Can not iterate past the last element");
    }

    auto key = *reinterpret_cast<const uint64_t*>(mChunkPos);
    mChunkPos += sizeof(uint64_t);

    auto data = mChunkPos;
    auto length = mRecord.sizeOfTuple(data);
    mChunkPos += length;

    if (mChunkPos >= mChunkEnd) {
        LOG_ASSERT(mChunkPos == mChunkEnd, "Chunk pointer not pointing to the exact end of the chunk");
        mChunkPos = nullptr;
    }

    return std::make_tuple(key, data, length);
}

std::tuple<const char*, const char*> ScanIterator::nextChunk() {
    if (!hasNext()) {
        throw std::out_of_range("Can not iterate past the last element");
    }

    auto chunk = std::make_tuple(mChunkPos, mChunkEnd);
    mChunkPos = nullptr;
    return chunk;
}

void ScanIterator::wait() {
    for (auto& response : mScans) {
        while (!response->wait());
    }
}

void ScanIterator::notify() {
    if (mWaiting) {
        mFiber.resume();
    }
}

void ScanIterator::abort(std::error_code ec) {
    LOG_ERROR("Scan aborted with error [error = %1% %2%]", ec, ec.message());
    if (!mError) {
        mError = std::move(ec);
    }
    notify();
}

void ClientSocket::connect(const crossbow::infinio::Endpoint& host, uint64_t threadNum) {
    LOG_INFO("Connecting to TellStore server %1% on processor %2%", host, threadNum);

    crossbow::string data;
    data.append(reinterpret_cast<char*>(&threadNum), sizeof(uint64_t));

    crossbow::infinio::RpcClientSocket::connect(host, data);
}

void ClientSocket::shutdown() {
    LOG_INFO("Shutting down TellStore connection");
    crossbow::infinio::RpcClientSocket::shutdown();
}

std::shared_ptr<CreateTableResponse> ClientSocket::createTable(crossbow::infinio::Fiber& fiber,
        const crossbow::string& name, const Schema& schema) {
    auto response = std::make_shared<CreateTableResponse>(fiber);

    auto nameLength = name.size();
    auto schemaLength = schema.serializedLength();
    uint32_t messageLength = sizeof(uint32_t) + nameLength;
    messageLength += messageAlign(messageLength, sizeof(uint64_t));
    messageLength += schemaLength;

    sendRequest(response, RequestType::CREATE_TABLE, messageLength, [nameLength, schemaLength, &name, &schema]
            (crossbow::buffer_writer& message, std::error_code& /* ec */) {
        message.write<uint32_t>(nameLength);
        message.write(name.data(), nameLength);

        message.align(sizeof(uint64_t));
        schema.serialize(message);
    });

    return response;
}

std::shared_ptr<GetTableResponse> ClientSocket::getTable(crossbow::infinio::Fiber& fiber,
        const crossbow::string& name) {
    auto response = std::make_shared<GetTableResponse>(fiber);

    auto nameLength = name.size();
    uint32_t messageLength = sizeof(uint32_t) + nameLength;

    sendRequest(response, RequestType::GET_TABLE, messageLength, [nameLength, &name]
            (crossbow::buffer_writer& message, std::error_code& /* ec */) {
        message.write<uint32_t>(nameLength);
        message.write(name.data(), nameLength);
    });

    return response;
}

std::shared_ptr<GetResponse> ClientSocket::get(crossbow::infinio::Fiber& fiber, uint64_t tableId, uint64_t key,
        const commitmanager::SnapshotDescriptor& snapshot) {
    auto response = std::make_shared<GetResponse>(fiber);

    uint32_t messageLength = 3 * sizeof(uint64_t) + snapshot.serializedLength();

    sendRequest(response, RequestType::GET, messageLength, [tableId, key, &snapshot]
            (crossbow::buffer_writer& message, std::error_code& /* ec */) {
        message.write<uint64_t>(tableId);
        message.write<uint64_t>(key);
        writeSnapshot(message, snapshot);
    });

    return response;
}

std::shared_ptr<ModificationResponse> ClientSocket::insert(crossbow::infinio::Fiber& fiber, uint64_t tableId,
        uint64_t key, const Record& record, const AbstractTuple& tuple,
        const commitmanager::SnapshotDescriptor& snapshot, bool hasSucceeded) {
    auto response = std::make_shared<ModificationResponse>(fiber);

    auto tupleLength = tuple.size();
    uint32_t messageLength = 3 * sizeof(uint64_t) + tupleLength;
    messageLength += messageAlign(messageLength, sizeof(uint64_t));
    messageLength += sizeof(uint64_t) + snapshot.serializedLength();

    sendRequest(response, RequestType::INSERT, messageLength,
            [tableId, key, &record, tupleLength, &tuple, &snapshot, hasSucceeded]
            (crossbow::buffer_writer& message, std::error_code& ec) {
        message.write<uint64_t>(tableId);
        message.write<uint64_t>(key);
        message.write<uint8_t>(hasSucceeded ? 0x1u : 0x0u);

        message.align(sizeof(uint32_t));
        message.write<uint32_t>(tupleLength);
        tuple.serialize(message.data());
        message.advance(tupleLength);

        message.align(sizeof(uint64_t));
        writeSnapshot(message, snapshot);
    });

    return response;
}

class GenericTupleSerializer : public AbstractTuple {
    const Record& record;
    const GenericTuple& tuple;
    const size_t sz;
public:
    GenericTupleSerializer(const Record& rec, const GenericTuple& tuple)
        : record(rec)
        , tuple(tuple)
        , sz(record.sizeOfTuple(tuple))
    {}

    size_t size() const override {
        return sz;
    }

    void serialize(char* dest) const override {
        record.create(dest, tuple, sz);
    }
};

std::shared_ptr<ModificationResponse> ClientSocket::insert(crossbow::infinio::Fiber& fiber, uint64_t tableId,
        uint64_t key, const Record& record, const GenericTuple& tuple,
        const commitmanager::SnapshotDescriptor& snapshot, bool hasSucceeded) {
    GenericTupleSerializer ser(record, tuple);
    return insert(fiber, tableId, key, record, ser, snapshot, hasSucceeded);
}

std::shared_ptr<ModificationResponse> ClientSocket::update(crossbow::infinio::Fiber& fiber, uint64_t tableId,
        uint64_t key, const Record& record, const AbstractTuple& tuple,
        const commitmanager::SnapshotDescriptor& snapshot) {
    auto response = std::make_shared<ModificationResponse>(fiber);

    auto tupleLength = tuple.size();
    uint32_t messageLength = 3 * sizeof(uint64_t) + tupleLength;
    messageLength += messageAlign(messageLength, sizeof(uint64_t));
    messageLength += sizeof(uint64_t) + snapshot.serializedLength();

    sendRequest(response, RequestType::UPDATE, messageLength, [tableId, key, &record, tupleLength, &tuple, &snapshot]
            (crossbow::buffer_writer& message, std::error_code& ec) {
        message.write<uint64_t>(tableId);
        message.write<uint64_t>(key);

        message.write<uint32_t>(0x0u);
        message.write<uint32_t>(tupleLength);
        tuple.serialize(message.data());
        message.advance(tupleLength);

        message.align(sizeof(uint64_t));
        writeSnapshot(message, snapshot);
    });

    return response;
}

std::shared_ptr<ModificationResponse> ClientSocket::update(crossbow::infinio::Fiber& fiber, uint64_t tableId,
        uint64_t key, const Record& record, const GenericTuple& tuple,
        const commitmanager::SnapshotDescriptor& snapshot) {
    GenericTupleSerializer ser(record, tuple);
    return update(fiber, tableId, key, record, ser, snapshot);
}

std::shared_ptr<ModificationResponse> ClientSocket::remove(crossbow::infinio::Fiber& fiber, uint64_t tableId,
        uint64_t key, const commitmanager::SnapshotDescriptor& snapshot) {
    auto response = std::make_shared<ModificationResponse>(fiber);

    uint32_t messageLength = 3 * sizeof(uint64_t) + snapshot.serializedLength();

    sendRequest(response, RequestType::REMOVE, messageLength, [tableId, key, &snapshot]
            (crossbow::buffer_writer& message, std::error_code& /* ec */) {
        message.write<uint64_t>(tableId);
        message.write<uint64_t>(key);
        writeSnapshot(message, snapshot);
    });

    return response;
}

std::shared_ptr<ModificationResponse> ClientSocket::revert(crossbow::infinio::Fiber& fiber, uint64_t table,
        uint64_t key, const commitmanager::SnapshotDescriptor& snapshot) {
    auto response = std::make_shared<ModificationResponse>(fiber);

    uint32_t messageLength = 3 * sizeof(uint64_t) + snapshot.serializedLength();

    sendRequest(response, RequestType::REVERT, messageLength, [table, key, &snapshot]
            (crossbow::buffer_writer& message, std::error_code& /* ec */) {
        message.write<uint64_t>(table);
        message.write<uint64_t>(key);
        writeSnapshot(message, snapshot);
    });

    return response;
}

void ClientSocket::scanStart(uint16_t scanId, std::shared_ptr<ScanResponse> response, uint64_t tableId,
        ScanQueryType queryType, uint32_t selectionLength, const char* selection, uint32_t queryLength,
        const char* query, const commitmanager::SnapshotDescriptor& snapshot) {
    if (!startAsyncRequest(scanId, response)) {
        response->onAbort(error::invalid_scan);
        return;
    }

    uint32_t messageLength = 6 * sizeof(uint64_t) + selectionLength + queryLength;
    messageLength += messageAlign(messageLength, sizeof(uint64_t));
    messageLength += sizeof(uint64_t) + snapshot.serializedLength();

    sendAsyncRequest(scanId, response, RequestType::SCAN, messageLength,
            [response, tableId, queryType, selectionLength, selection, queryLength, query, &snapshot]
            (crossbow::buffer_writer& message, std::error_code& /* ec */) {
        message.write<uint64_t>(tableId);
        message.write<uint8_t>(crossbow::to_underlying(queryType));

        auto& memory = response->scanMemory();
        message.set(0, sizeof(uint64_t) - sizeof(uint8_t));
        message.write<uint64_t>(reinterpret_cast<uintptr_t>(memory.data()));
        message.write<uint64_t>(memory.length());
        message.write<uint32_t>(memory.key());

        message.write<uint32_t>(selectionLength);
        message.write(selection, selectionLength);

        message.set(0, sizeof(uint32_t));
        message.write<uint32_t>(queryLength);
        message.write(query, queryLength);

        message.align(sizeof(uint64_t));
        writeSnapshot(message, snapshot);
    });
}

void ClientSocket::scanProgress(uint16_t scanId, std::shared_ptr<ScanResponse> response, size_t offset) {
    uint32_t messageLength = sizeof(size_t);

    sendAsyncRequest(scanId, response, RequestType::SCAN_PROGRESS, messageLength, [offset]
            (crossbow::buffer_writer& message, std::error_code& /* ec */) {
        message.write<size_t>(offset);
    });
}

} // namespace store
} // namespace tell
