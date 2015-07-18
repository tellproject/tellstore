#include "ClientSocket.hpp"

#include <commitmanager/SnapshotDescriptor.hpp>

#include <crossbow/infinio/Endpoint.hpp>
#include <crossbow/infinio/InfinibandBuffer.hpp>
#include <crossbow/logger.hpp>

namespace tell {
namespace store {

namespace {

uint32_t messageAlign(uint32_t size, uint32_t alignment) {
    return ((size % alignment != 0) ? (alignment - (size % alignment)) : 0);
}

void writeSnapshot(crossbow::infinio::BufferWriter& message, const commitmanager::SnapshotDescriptor& snapshot) {
    // TODO Implement snapshot caching
    message.write<uint8_t>(0x0u); // Cached
    message.write<uint8_t>(0x1u); // HasDescriptor
    message.align(sizeof(uint64_t));
    snapshot.serialize(message);
}

} // anonymous namespace

void CreateTableResponse::processResponse(crossbow::infinio::BufferReader& message) {
    auto tableId = message.read<uint64_t>();
    setResult(new Table(tableId, std::move(mSchema)));
}

void GetTableResponse::processResponse(crossbow::infinio::BufferReader& message) {
    auto tableId = message.read<uint64_t>();

    // TODO Refactor schema serialization
    auto schemaData = message.data();
    auto schemaLength = message.read<uint32_t>();
    Schema schema(schemaData);
    message.advance(schemaLength - sizeof(uint32_t));

    setResult(new Table(tableId, std::move(schema)));
}

void GetResponse::processResponse(crossbow::infinio::BufferReader& message) {
    setResult(Tuple::deserialize(message));
}

void ModificationResponse::processResponse(crossbow::infinio::BufferReader& message) {
    auto succeeded = (message.read<uint8_t>() != 0x0u);
    setResult(succeeded);
}

bool ScanResponse::hasNext() {
    while (mTuplePending == 0x0u) {
        if (done()) {
            return false;
        }
        wait();
    }
    return true;
}

const char* ScanResponse::next() {
    if (!hasNext()) {
        throw std::out_of_range("Can not iterate past the last element");
    }

    auto tuple = mPos;
    --mTuplePending;
    mPos += mRecord.sizeOfTuple(tuple);

    return tuple;
}

void ScanResponse::processResponse(crossbow::infinio::BufferReader& /* message */) {
    mSocket.onScanComplete(mScanId);
    setResult(true);
}

void ScanResponse::notifyProgress(uint16_t tupleCount) {
    mTuplePending += tupleCount;
    notify();
}

void ClientSocket::connect(const crossbow::string& host, uint16_t port, uint64_t threadNum) {
    LOG_INFO("Connecting to TellStore server %1%:%2% on processor %3%", host, port, threadNum);

    crossbow::string data;
    data.append(reinterpret_cast<char*>(&threadNum), sizeof(uint64_t));

    crossbow::infinio::RpcClientSocket::connect(host, port, data);
}

void ClientSocket::shutdown() {
    LOG_INFO("Shutting down TellStore connection");
    crossbow::infinio::RpcClientSocket::shutdown();
}

std::shared_ptr<CreateTableResponse> ClientSocket::createTable(crossbow::infinio::Fiber& fiber,
        const crossbow::string& name, const Schema& schema) {
    auto response = std::make_shared<CreateTableResponse>(fiber, schema);

    auto nameLength = name.size();
    auto schemaLength = schema.schemaSize();
    uint32_t messageLength = sizeof(uint16_t) + nameLength;
    messageLength += messageAlign(messageLength, sizeof(uint64_t));
    messageLength += schemaLength;

    sendRequest(response, RequestType::CREATE_TABLE, messageLength, [nameLength, schemaLength, &name, &schema]
            (crossbow::infinio::BufferWriter& message, std::error_code& /* ec */) {
        message.write<uint16_t>(nameLength);
        message.write(name.data(), nameLength);

        message.align(sizeof(uint64_t));
        schema.serialize(message.data());
        message.advance(schemaLength);
    });

    return response;
}

std::shared_ptr<GetTableResponse> ClientSocket::getTable(crossbow::infinio::Fiber& fiber,
        const crossbow::string& name) {
    auto response = std::make_shared<GetTableResponse>(fiber);

    auto nameLength = name.size();
    uint32_t messageLength = sizeof(uint16_t) + nameLength;

    sendRequest(response, RequestType::GET_TABLE, messageLength, [nameLength, &name]
            (crossbow::infinio::BufferWriter& message, std::error_code& /* ec */) {
        message.write<uint16_t>(nameLength);
        message.write(name.data(), nameLength);
    });

    return response;
}

std::shared_ptr<GetResponse> ClientSocket::get(crossbow::infinio::Fiber& fiber, uint64_t tableId, uint64_t key,
        const commitmanager::SnapshotDescriptor& snapshot) {
    auto response = std::make_shared<GetResponse>(fiber);

    uint32_t messageLength = 3 * sizeof(uint64_t) + snapshot.serializedLength();

    sendRequest(response, RequestType::GET, messageLength, [tableId, key, &snapshot]
            (crossbow::infinio::BufferWriter& message, std::error_code& /* ec */) {
        message.write<uint64_t>(tableId);
        message.write<uint64_t>(key);
        writeSnapshot(message, snapshot);
    });

    return response;
}

std::shared_ptr<ModificationResponse> ClientSocket::insert(crossbow::infinio::Fiber& fiber, uint64_t tableId,
        uint64_t key, const Record& record, const GenericTuple& tuple,
        const commitmanager::SnapshotDescriptor& snapshot, bool hasSucceeded) {
    auto response = std::make_shared<ModificationResponse>(fiber);

    auto tupleLength = record.sizeOfTuple(tuple);
    uint32_t messageLength = 3 * sizeof(uint64_t) + tupleLength;
    messageLength += messageAlign(messageLength, sizeof(uint64_t));
    messageLength += sizeof(uint64_t) + snapshot.serializedLength();

    sendRequest(response, RequestType::INSERT, messageLength,
            [tableId, key, &record, tupleLength, &tuple, &snapshot, hasSucceeded]
            (crossbow::infinio::BufferWriter& message, std::error_code& ec) {
        message.write<uint64_t>(tableId);
        message.write<uint64_t>(key);
        message.write<uint8_t>(hasSucceeded ? 0x1u : 0x0u);

        message.align(sizeof(uint32_t));
        message.write<uint32_t>(tupleLength);
        if (!record.create(message.data(), tuple, tupleLength)) {
            ec = error::invalid_tuple;
            return;
        }
        message.advance(tupleLength);

        message.align(sizeof(uint64_t));
        writeSnapshot(message, snapshot);
    });

    return response;
}

std::shared_ptr<ModificationResponse> ClientSocket::update(crossbow::infinio::Fiber& fiber, uint64_t tableId,
        uint64_t key, const Record& record, const GenericTuple& tuple,
        const commitmanager::SnapshotDescriptor& snapshot) {
    auto response = std::make_shared<ModificationResponse>(fiber);

    auto tupleLength = record.sizeOfTuple(tuple);
    uint32_t messageLength = 3 * sizeof(uint64_t) + tupleLength;
    messageLength += messageAlign(messageLength, sizeof(uint64_t));
    messageLength += sizeof(uint64_t) + snapshot.serializedLength();

    sendRequest(response, RequestType::UPDATE, messageLength, [tableId, key, &record, tupleLength, &tuple, &snapshot]
            (crossbow::infinio::BufferWriter& message, std::error_code& ec) {
        message.write<uint64_t>(tableId);
        message.write<uint64_t>(key);

        message.write<uint32_t>(0x0u);
        message.write<uint32_t>(tupleLength);
        if (!record.create(message.data(), tuple, tupleLength)) {
            ec = error::invalid_tuple;
            return;
        }
        message.advance(tupleLength);

        message.align(sizeof(uint64_t));
        writeSnapshot(message, snapshot);
    });

    return response;
}

std::shared_ptr<ModificationResponse> ClientSocket::remove(crossbow::infinio::Fiber& fiber, uint64_t tableId,
        uint64_t key, const commitmanager::SnapshotDescriptor& snapshot) {
    auto response = std::make_shared<ModificationResponse>(fiber);

    uint32_t messageLength = 3 * sizeof(uint64_t) + snapshot.serializedLength();

    sendRequest(response, RequestType::REMOVE, messageLength, [tableId, key, &snapshot]
            (crossbow::infinio::BufferWriter& message, std::error_code& /* ec */) {
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
            (crossbow::infinio::BufferWriter& message, std::error_code& /* ec */) {
        message.write<uint64_t>(table);
        message.write<uint64_t>(key);
        writeSnapshot(message, snapshot);
    });

    return response;
}

std::shared_ptr<ScanResponse> ClientSocket::scan(crossbow::infinio::Fiber& fiber, uint64_t tableId,
        const Record& record, uint32_t queryLength, const char* query,
        const crossbow::infinio::LocalMemoryRegion& destRegion, const commitmanager::SnapshotDescriptor& snapshot) {
    ++mScanId;
    auto response = std::make_shared<ScanResponse>(fiber, *this, record, mScanId,
            reinterpret_cast<const char*>(destRegion.address()), destRegion.length());

    uint32_t messageLength = 5 * sizeof(uint64_t) + queryLength;
    messageLength += messageAlign(messageLength, sizeof(uint64_t));
    messageLength += sizeof(uint64_t) + snapshot.serializedLength();

    sendAsyncRequest(response, RequestType::SCAN, messageLength,
            [this, tableId, queryLength, query, &destRegion, &snapshot]
            (crossbow::infinio::BufferWriter& message, std::error_code& /* ec */) {
        message.write<uint64_t>(tableId);
        message.write<uint16_t>(mScanId);

        message.align(sizeof(uint64_t));
        message.write<uint64_t>(destRegion.address());
        message.write<uint64_t>(destRegion.length());
        message.write<uint32_t>(destRegion.rkey());

        message.write<uint32_t>(queryLength);
        message.write(query, queryLength);

        message.align(sizeof(uint64_t));
        writeSnapshot(message, snapshot);
    });

    mScans.insert(std::make_pair(mScanId, response.get()));
    return response;
}

void ClientSocket::onImmediate(uint32_t data) {
    auto scanId = static_cast<uint16_t>(data >> 16);
    auto tupleCount = static_cast<uint16_t>(data & 0xFFFFu);

    auto i = mScans.find(scanId);
    if (i == mScans.end()) {
        LOG_WARN("Scan %1%] Scan not found", scanId);
        // TODO Handle this correctly
        return;
    }
    i->second->notifyProgress(tupleCount);
}

void ClientSocket::onScanComplete(uint16_t scanId) {
    auto i = mScans.find(scanId);
    if (i == mScans.end()) {
        LOG_DEBUG("Scan %1%] Scan not found", scanId);
        return;
    }
    mScans.erase(i);
}

} // namespace store
} // namespace tell
