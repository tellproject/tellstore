#include "ServerSocket.hpp"

#include <network/ErrorCode.hpp>
#include <network/MessageTypes.hpp>
#include <util/PageManager.hpp>

#include <crossbow/enum_underlying.hpp>
#include <crossbow/infinio/InfinibandBuffer.hpp>
#include <crossbow/logger.hpp>

namespace tell {
namespace store {

void ServerSocket::onRequest(crossbow::infinio::MessageId messageId, uint32_t messageType,
        crossbow::infinio::BufferReader& request) {
    LOG_TRACE("MID %1%] Handling request of type %2%", messageId.userId(), messageType);
    auto startTime = std::chrono::steady_clock::now();

    switch (messageType) {

    case crossbow::to_underlying(RequestType::CREATE_TABLE): {
        handleCreateTable(messageId, request);
    } break;

    case crossbow::to_underlying(RequestType::GET_TABLE): {
        handleGetTable(messageId, request);
    } break;

    case crossbow::to_underlying(RequestType::GET): {
        handleGet(messageId, request);
    } break;

    case crossbow::to_underlying(RequestType::UPDATE): {
        handleUpdate(messageId, request);
    } break;

    case crossbow::to_underlying(RequestType::INSERT): {
        handleInsert(messageId, request);
    } break;

    case crossbow::to_underlying(RequestType::REMOVE): {
        handleRemove(messageId, request);
    } break;

    case crossbow::to_underlying(RequestType::REVERT): {
        handleRevert(messageId, request);
    } break;

    case crossbow::to_underlying(RequestType::SCAN): {
        handleScan(messageId, request);
    } break;

    case crossbow::to_underlying(RequestType::COMMIT): {
        // TODO Implement commit logic
    } break;

    default: {
        writeErrorResponse(messageId, error::unkown_request);
    } break;
    }

    auto endTime = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(endTime - startTime);
    LOG_TRACE("MID %1%] Handling request took %2%ns", messageId.userId(), duration.count());
}

void ServerSocket::handleCreateTable(crossbow::infinio::MessageId messageId, crossbow::infinio::BufferReader& request) {
    auto tableNameLength = request.read<uint16_t>();
    auto tableNameData = request.read(tableNameLength);
    request.align(sizeof(uint64_t));
    crossbow::string tableName(tableNameData, tableNameLength);

    // TODO Refactor schema serialization
    auto schemaData = request.data();
    auto schemaLength = request.read<uint32_t>();
    request.advance(schemaLength - sizeof(uint32_t));
    Schema schema(schemaData);

    uint64_t tableId = 0;
    auto succeeded = mStorage.createTable(tableName, schema, tableId);
    LOG_ASSERT((tableId != 0) || !succeeded, "Table ID of 0 does not denote failure");

    uint32_t messageLength = sizeof(uint64_t);
    writeResponse(messageId, ResponseType::CREATE_TABLE, messageLength, [succeeded, tableId]
            (crossbow::infinio::BufferWriter& message, std::error_code& /* ec */) {
        message.write<uint64_t>(succeeded ? tableId : 0x0u);
    });
}

void ServerSocket::handleGetTable(crossbow::infinio::MessageId messageId, crossbow::infinio::BufferReader& request) {
    auto tableNameLength = request.read<uint16_t>();
    auto tableNameData = request.read(tableNameLength);
    crossbow::string tableName(tableNameData, tableNameLength);

    uint64_t tableId = 0x0u;
    auto table = mStorage.getTable(tableName, tableId);

    if (!table) {
        writeErrorResponse(messageId, error::invalid_table);
        return;
    }

    auto& schema = table->schema();
    auto schemaLength = schema.schemaSize();

    uint32_t messageLength = sizeof(uint64_t) + schemaLength;
    writeResponse(messageId, ResponseType::GET_TABLE, messageLength, [tableId, schemaLength, &schema]
            (crossbow::infinio::BufferWriter& message, std::error_code& /* ec */) {
        message.write<uint64_t>(tableId);
        schema.serialize(message.data());
        message.advance(schemaLength);
    });
}

void ServerSocket::handleGet(crossbow::infinio::MessageId messageId, crossbow::infinio::BufferReader& request) {
    auto tableId = request.read<uint64_t>();
    auto key = request.read<uint64_t>();
    handleSnapshot(messageId, request, [this, messageId, tableId, key]
            (const commitmanager::SnapshotDescriptor& snapshot) {
        size_t size = 0;
        const char* data = nullptr;
        uint64_t version = 0x0u;
        bool isNewest = false;
        auto success = mStorage.get(tableId, key, size, data, snapshot, version, isNewest);

        // Message size is 8 bytes version plus 8 bytes (isNewest, success, size) and data
        uint32_t messageLength = 2 * sizeof(uint64_t) + size;
        writeResponse(messageId, ResponseType::GET, messageLength, [version, isNewest, success, size, data]
                (crossbow::infinio::BufferWriter& message, std::error_code& /* ec */) {
            message.write<uint64_t>(version);
            message.write<uint8_t>(isNewest ? 0x1u : 0x0u);
            message.write<uint8_t>(success ? 0x1u : 0x0u);
            if (success) {
                message.align(sizeof(uint32_t));
                message.write<uint32_t>(size);
                message.write(data, size);
            }
        });
    });
}

void ServerSocket::handleUpdate(crossbow::infinio::MessageId messageId, crossbow::infinio::BufferReader& request) {
    auto tableId = request.read<uint64_t>();
    auto key = request.read<uint64_t>();

    request.read<uint32_t>();
    auto dataLength = request.read<uint32_t>();
    auto data = request.read(dataLength);

    request.align(sizeof(uint64_t));
    handleSnapshot(messageId, request, [this, messageId, tableId, key, dataLength, data]
            (const commitmanager::SnapshotDescriptor& snapshot) {
        auto succeeded = mStorage.update(tableId, key, dataLength, data, snapshot);

        // Message size is 1 byte (succeeded)
        uint32_t messageLength = sizeof(uint8_t);
        writeResponse(messageId, ResponseType::MODIFICATION, messageLength, [succeeded]
                (crossbow::infinio::BufferWriter& message, std::error_code& /* ec */) {
            message.write<uint8_t>(succeeded ? 0x1u : 0x0u);
        });
    });
}

void ServerSocket::handleInsert(crossbow::infinio::MessageId messageId, crossbow::infinio::BufferReader& request) {
    auto tableId = request.read<uint64_t>();
    auto key = request.read<uint64_t>();
    bool wantsSucceeded = request.read<uint8_t>();

    request.align(sizeof(uint32_t));
    auto dataLength = request.read<uint32_t>();
    auto data = request.read(dataLength);

    request.align(sizeof(uint64_t));
    handleSnapshot(messageId, request, [this, messageId, tableId, key, wantsSucceeded, dataLength, data]
            (const commitmanager::SnapshotDescriptor& snapshot) {
        bool succeeded = false;
        mStorage.insert(tableId, key, dataLength, data, snapshot, (wantsSucceeded ? &succeeded : nullptr));

        // Message size is 1 byte (succeeded)
        uint32_t messageLength = sizeof(uint8_t);
        writeResponse(messageId, ResponseType::MODIFICATION, messageLength, [succeeded]
                (crossbow::infinio::BufferWriter& message, std::error_code& /* ec */) {
            message.write<uint8_t>(succeeded ? 0x1u : 0x0u);
        });
    });
}

void ServerSocket::handleRemove(crossbow::infinio::MessageId messageId, crossbow::infinio::BufferReader& request) {
    auto tableId = request.read<uint64_t>();
    auto key = request.read<uint64_t>();

    handleSnapshot(messageId, request, [this, messageId, tableId, key]
            (const commitmanager::SnapshotDescriptor& snapshot) {
        auto succeeded = mStorage.remove(tableId, key, snapshot);

        // Message size is 1 byte (succeeded)
        uint32_t messageLength = sizeof(uint8_t);
        writeResponse(messageId, ResponseType::MODIFICATION, messageLength, [succeeded]
                (crossbow::infinio::BufferWriter& message, std::error_code& /* ec */) {
            message.write<uint8_t>(succeeded ? 0x1u : 0x0u);
        });
    });
}

void ServerSocket::handleRevert(crossbow::infinio::MessageId messageId, crossbow::infinio::BufferReader& request) {
    auto tableId = request.read<uint64_t>();
    auto key = request.read<uint64_t>();

    handleSnapshot(messageId, request, [this, messageId, tableId, key]
            (const commitmanager::SnapshotDescriptor& snapshot) {
        auto succeeded = mStorage.revert(tableId, key, snapshot);

        // Message size is 1 byte (succeeded)
        uint32_t messageLength = sizeof(uint8_t);
        writeResponse(messageId, ResponseType::MODIFICATION, messageLength, [succeeded]
                (crossbow::infinio::BufferWriter& message, std::error_code& /* ec */) {
            message.write<uint8_t>(succeeded ? 0x1u : 0x0u);
        });
    });
}

void ServerSocket::handleScan(crossbow::infinio::MessageId messageId, crossbow::infinio::BufferReader& request) {
    auto tableId = request.read<uint64_t>();
    auto scanId = request.read<uint16_t>();

    request.align(sizeof(uint64_t));
    auto remoteAddress = request.read<uint64_t>();
    auto remoteLength = request.read<uint64_t>();
    auto remoteKey = request.read<uint32_t>();
    crossbow::infinio::RemoteMemoryRegion remoteRegion(remoteAddress, remoteLength, remoteKey);

    auto queryLength = request.read<uint32_t>();
    auto queryData = request.read(queryLength);
    std::unique_ptr<char[]> query(new char[queryLength]);
    memcpy(query.get(), queryData, queryLength);

    request.align(sizeof(uint64_t));
    handleSnapshot(messageId, request, [this, messageId, tableId, scanId, &remoteRegion, queryLength, &query]
            (const commitmanager::SnapshotDescriptor& snapshot) {
        auto scanSnapshot = commitmanager::SnapshotDescriptor::create(snapshot.lowestActiveVersion(),
                snapshot.baseVersion(), snapshot.version(), snapshot.data());

        std::unique_ptr<ClientScanQueryData> scanData(new ClientScanQueryData(messageId, std::move(scanSnapshot),
                manager().pageRegion(), remoteRegion, mSocket));
        auto scanDataPtr = scanData.get();
        auto res = mScans.emplace(scanId, std::move(scanData));
        if (!res.second) {
            writeErrorResponse(messageId, error::invalid_scan);
            return;
        }

        std::vector<ScanQueryImpl*> impls;
        auto numScans = mStorage.numScanThreads();
        for (int i = 0; i < numScans; ++i) {
            impls.push_back(new ClientScanQuery(scanId, scanDataPtr));
        }

        auto succeeded = mStorage.scan(tableId, query.release(), queryLength, impls);
        if (!succeeded) {
            writeErrorResponse(messageId, error::server_overlad);
            for (auto impl: impls) {
                delete impl;
            }
            mScans.erase(res.first);
        }
    });
}

void ServerSocket::onWrite(uint32_t userId, uint16_t bufferId, const std::error_code& ec) {
    // TODO We have to propagate the error to the ClientScanQuery so we can detach the scan
    if (ec) {
        handleSocketError(ec);
        return;
    }

    if (userId == 0x0u || userId > crossbow::to_underlying(ClientScanQueryData::ScanStatus::LAST)) {
        LOG_ERROR("Scan progress with invalid userid");
        return;
    }

    switch (crossbow::from_underlying<ClientScanQueryData::ScanStatus>(userId)) {

    case ClientScanQueryData::ScanStatus::ONGOING: {
        // Nothing to do
    } break;

    case ClientScanQueryData::ScanStatus::DONE: {
        auto i = mScans.find(bufferId);
        if (i == mScans.end()) {
            return;
        }
        if (!i->second->decreaseActive()) {
            return;
        }

        LOG_DEBUG("Scan with ID %1% finished", bufferId);
        size_t messageLength = sizeof(uint16_t);
        writeResponse(i->second->messageId(), ResponseType::SCAN, messageLength, [bufferId]
                (crossbow::infinio::BufferWriter& message, std::error_code& /* ec */) {
            message.write<uint16_t>(bufferId);
        });
        mScans.erase(i);
    } break;

    default:
        break;
    }
}

template <typename Fun>
void ServerSocket::handleSnapshot(crossbow::infinio::MessageId messageId, crossbow::infinio::BufferReader& message,
        Fun f) {
    bool cached = (message.read<uint8_t>() != 0x0u);
    bool hasDescriptor = (message.read<uint8_t>() != 0x0u);
    message.align(sizeof(uint64_t));
    if (cached) {
        typename decltype(mSnapshots)::iterator i;
        if (!hasDescriptor) {
            // The client did not send a snapshot so it has to be in the cache
            auto version = message.read<uint64_t>();
            i = mSnapshots.find(version);
            if (i == mSnapshots.end()) {
                writeErrorResponse(messageId, error::invalid_snapshot);
                return;
            }
        } else {
            // The client send a snapshot so we have to add it to the cache (it must not already be there)
            auto snapshot = commitmanager::SnapshotDescriptor::deserialize(message);
            auto res = mSnapshots.emplace(snapshot->version(), std::move(snapshot));
            if (!res.second) { // Snapshot descriptor already is in the cache
                writeErrorResponse(messageId, error::invalid_snapshot);
                return;
            }
            i = res.first;
        }
        f(*i->second);
    } else if (hasDescriptor) {
        auto snapshot = commitmanager::SnapshotDescriptor::deserialize(message);
        f(*snapshot);
    } else {
        writeErrorResponse(messageId, error::invalid_snapshot);
    }
}

void ServerSocket::removeSnapshot(uint64_t version) {
    auto i = mSnapshots.find(version);
    if (i == mSnapshots.end()) {
        return;
    }
    mSnapshots.erase(i);
}

ServerManager::ServerManager(crossbow::infinio::InfinibandService& service, Storage& storage,
        const ServerConfig& config)
        : Base(service, config.port),
          mStorage(storage),
          mPageRegion(service.registerMemoryRegion(mStorage.pageManager().data(), mStorage.pageManager().size(), 0)) {
    for (decltype(config.numNetworkThreads) i = 0; i < config.numNetworkThreads; ++i) {
        mProcessors.emplace_back(service.createProcessor());
    }
}

ServerSocket* ServerManager::createConnection(crossbow::infinio::InfinibandSocket socket,
        const crossbow::string& data) {
    if (data.size() < sizeof(uint64_t)) {
        throw std::logic_error("Client did not send enough data in connection attempt");
    }
    auto thread = *reinterpret_cast<const uint64_t*>(&data.front());
    auto& processor = *mProcessors.at(thread % mProcessors.size());

    return new ServerSocket(*this, mStorage, processor, std::move(socket));
}

} // namespace store
} // namespace tell
