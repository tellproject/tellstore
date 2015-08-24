#include <tellstore/ClientSocket.hpp>

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

ScanResponse::ScanResponse(crossbow::infinio::Fiber& fiber, ClientSocket& socket, ScanMemory memory, Record record,
        uint16_t scanId)
        : Base(fiber),
          mSocket(socket),
          mMemory(std::move(memory)),
          mRecord(std::move(record)),
          mScanId(scanId),
          mPos(reinterpret_cast<const char*>(mMemory.data())),
          mTuplePending(0x0u) {
    if (!mMemory.valid()) {
        setError(std::make_error_code(std::errc::not_enough_memory));
    }
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

std::tuple<uint64_t, const char*, size_t> ScanResponse::next() {
    if (!hasNext()) {
        throw std::out_of_range("Can not iterate past the last element");
    }

    --mTuplePending;

    auto key = *reinterpret_cast<const uint64_t*>(mPos);
    mPos += sizeof(uint64_t);

    auto length = mRecord.sizeOfTuple(mPos);

    auto data = mPos;
    mPos += length;

    return std::make_tuple(key, data, length);
}

void ScanResponse::processResponse(crossbow::buffer_reader& /* message */) {
    mSocket.onScanComplete(mScanId);
    setResult(true);
}

void ScanResponse::notifyProgress(uint16_t tupleCount) {
    mTuplePending += tupleCount;
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
        uint64_t key, const Record& record, const GenericTuple& tuple,
        const commitmanager::SnapshotDescriptor& snapshot, bool hasSucceeded) {
    auto response = std::make_shared<ModificationResponse>(fiber);

    auto tupleLength = record.sizeOfTuple(tuple);
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
            (crossbow::buffer_writer& message, std::error_code& ec) {
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

std::shared_ptr<ScanResponse> ClientSocket::scan(crossbow::infinio::Fiber& fiber, uint64_t tableId,
        const Record& record, ScanMemory scanMemory, ScanQueryType queryType, uint32_t selectionLength,
        const char* selection, uint32_t queryLength, const char* query,
        const commitmanager::SnapshotDescriptor& snapshot) {
    auto scanId = ++mScanId;
    auto response = std::make_shared<ScanResponse>(fiber, *this, std::move(scanMemory), record, scanId);
    if (response->done()) {
        return response;
    }
    mScans.insert(std::make_pair(scanId, response.get()));

    uint32_t messageLength = 6 * sizeof(uint64_t) + selectionLength + queryLength;
    messageLength += messageAlign(messageLength, sizeof(uint64_t));
    messageLength += sizeof(uint64_t) + snapshot.serializedLength();

    sendAsyncRequest(response, RequestType::SCAN, messageLength,
            [this, &response, tableId, queryType, selectionLength, selection, queryLength, query, &snapshot]
            (crossbow::buffer_writer& message, std::error_code& /* ec */) {
        message.write<uint64_t>(tableId);
        message.write<uint16_t>(response->scanId());

        auto& memory = response->scanMemory();
        message.align(sizeof(uint64_t));
        message.write<uint64_t>(reinterpret_cast<uintptr_t>(memory.data()));
        message.write<uint64_t>(memory.length());
        message.write<uint32_t>(memory.key());

        message.write<uint32_t>(selectionLength);
        message.write(selection, selectionLength);

        message.write<uint8_t>(crossbow::to_underlying(queryType));

        message.align(sizeof(uint32_t));
        message.write<uint32_t>(queryLength);
        message.write(query, queryLength);

        message.align(sizeof(uint64_t));
        writeSnapshot(message, snapshot);
    });

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
