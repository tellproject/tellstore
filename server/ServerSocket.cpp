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
#include "ServerSocket.hpp"

#include <util/PageManager.hpp>

#include <tellstore/ErrorCode.hpp>
#include <tellstore/MessageTypes.hpp>

#include <crossbow/enum_underlying.hpp>
#include <crossbow/infinio/InfinibandBuffer.hpp>
#include <crossbow/logger.hpp>

namespace tell {
namespace store {

void ServerSocket::writeScanProgress(uint16_t scanId, bool done, size_t offset) {
    uint32_t messageLength = 2 * sizeof(size_t);
    writeResponse(crossbow::infinio::MessageId(scanId, true), ResponseType::SCAN, messageLength, [done, offset]
            (crossbow::buffer_writer& message, std::error_code& /* ec */) {
        message.write<uint8_t>(done ? 0x1u : 0x0u);
        message.set(0, sizeof(size_t) - sizeof(uint8_t));
        message.write<size_t>(offset);
    });
}

void ServerSocket::onRequest(crossbow::infinio::MessageId messageId, uint32_t messageType,
        crossbow::buffer_reader& request) {
#ifdef NDEBUG
#else
    LOG_TRACE("MID %1%] Handling request of type %2%", messageId.userId(), messageType);
    auto startTime = std::chrono::steady_clock::now();
#endif

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

    case crossbow::to_underlying(RequestType::SCAN_PROGRESS): {
        handleScanProgress(messageId, request);
    } break;

    case crossbow::to_underlying(RequestType::COMMIT): {
        // TODO Implement commit logic
    } break;

    default: {
        writeErrorResponse(messageId, error::unkown_request);
    } break;
    }

#ifdef NDEBUG
#else
    auto endTime = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(endTime - startTime);
    LOG_TRACE("MID %1%] Handling request took %2%ns", messageId.userId(), duration.count());
#endif
}

void ServerSocket::handleCreateTable(crossbow::infinio::MessageId messageId, crossbow::buffer_reader& request) {
    auto tableNameLength = request.read<uint32_t>();
    crossbow::string tableName(request.read(tableNameLength), tableNameLength);

    request.align(sizeof(uint64_t));
    auto schema = Schema::deserialize(request);

    uint64_t tableId = 0;
    auto succeeded = mStorage.createTable(tableName, schema, tableId);
    LOG_ASSERT((tableId != 0) || !succeeded, "Table ID of 0 does not denote failure");

    if (!succeeded) {
        writeErrorResponse(messageId, error::invalid_table);
        return;
    }

    uint32_t messageLength = sizeof(uint64_t);
    writeResponse(messageId, ResponseType::CREATE_TABLE, messageLength, [tableId]
            (crossbow::buffer_writer& message, std::error_code& /* ec */) {
        message.write<uint64_t>(tableId);
    });
}

void ServerSocket::handleGetTable(crossbow::infinio::MessageId messageId, crossbow::buffer_reader& request) {
    auto tableNameLength = request.read<uint32_t>();
    crossbow::string tableName(request.read(tableNameLength), tableNameLength);

    uint64_t tableId = 0x0u;
    auto table = mStorage.getTable(tableName, tableId);

    if (!table) {
        writeErrorResponse(messageId, error::invalid_table);
        return;
    }

    auto& schema = table->schema();

    uint32_t messageLength = sizeof(uint64_t) + schema.serializedLength();
    writeResponse(messageId, ResponseType::GET_TABLE, messageLength, [tableId, &schema]
            (crossbow::buffer_writer& message, std::error_code& /* ec */) {
        message.write<uint64_t>(tableId);
        schema.serialize(message);
    });
}

void ServerSocket::handleGet(crossbow::infinio::MessageId messageId, crossbow::buffer_reader& request) {
    auto tableId = request.read<uint64_t>();
    auto key = request.read<uint64_t>();
    handleSnapshot(messageId, request, [this, messageId, tableId, key]
            (const commitmanager::SnapshotDescriptor& snapshot) {
        size_t size = 0;
        const char* data = nullptr;
        uint64_t version = 0x0u;
        bool isNewest = false;
        __attribute__((unused)) auto success = mStorage.get(tableId, key, size, data, snapshot, version, isNewest);
        LOG_ASSERT(success || (size == 0x0u), "Size of 0 does not indicate element-not-found");

        // Message size is 8 bytes version plus 8 bytes (isNewest, success, size) and data
        uint32_t messageLength = 2 * sizeof(uint64_t) + size;
        writeResponse(messageId, ResponseType::GET, messageLength, [version, isNewest, size, data]
                (crossbow::buffer_writer& message, std::error_code& /* ec */) {
            message.write<uint64_t>(version);
            message.write<uint8_t>(isNewest ? 0x1u : 0x0u);
            message.align(sizeof(uint32_t));
            message.write<uint32_t>(size);
            if (size > 0) {
                message.write(data, size);
            }
        });
    });
}

void ServerSocket::handleUpdate(crossbow::infinio::MessageId messageId, crossbow::buffer_reader& request) {
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
                (crossbow::buffer_writer& message, std::error_code& /* ec */) {
            message.write<uint8_t>(succeeded ? 0x1u : 0x0u);
        });
    });
}

void ServerSocket::handleInsert(crossbow::infinio::MessageId messageId, crossbow::buffer_reader& request) {
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
                (crossbow::buffer_writer& message, std::error_code& /* ec */) {
            message.write<uint8_t>(succeeded ? 0x1u : 0x0u);
        });
    });
}

void ServerSocket::handleRemove(crossbow::infinio::MessageId messageId, crossbow::buffer_reader& request) {
    auto tableId = request.read<uint64_t>();
    auto key = request.read<uint64_t>();

    handleSnapshot(messageId, request, [this, messageId, tableId, key]
            (const commitmanager::SnapshotDescriptor& snapshot) {
        auto succeeded = mStorage.remove(tableId, key, snapshot);

        // Message size is 1 byte (succeeded)
        uint32_t messageLength = sizeof(uint8_t);
        writeResponse(messageId, ResponseType::MODIFICATION, messageLength, [succeeded]
                (crossbow::buffer_writer& message, std::error_code& /* ec */) {
            message.write<uint8_t>(succeeded ? 0x1u : 0x0u);
        });
    });
}

void ServerSocket::handleRevert(crossbow::infinio::MessageId messageId, crossbow::buffer_reader& request) {
    auto tableId = request.read<uint64_t>();
    auto key = request.read<uint64_t>();

    handleSnapshot(messageId, request, [this, messageId, tableId, key]
            (const commitmanager::SnapshotDescriptor& snapshot) {
        auto succeeded = mStorage.revert(tableId, key, snapshot);

        // Message size is 1 byte (succeeded)
        uint32_t messageLength = sizeof(uint8_t);
        writeResponse(messageId, ResponseType::MODIFICATION, messageLength, [succeeded]
                (crossbow::buffer_writer& message, std::error_code& /* ec */) {
            message.write<uint8_t>(succeeded ? 0x1u : 0x0u);
        });
    });
}

void ServerSocket::handleScan(crossbow::infinio::MessageId messageId, crossbow::buffer_reader& request) {
    auto tableId = request.read<uint64_t>();
    auto queryType = crossbow::from_underlying<ScanQueryType>(request.read<uint8_t>());

    request.advance(sizeof(uint64_t) - sizeof(uint8_t));
    auto remoteAddress = request.read<uint64_t>();
    auto remoteLength = request.read<uint64_t>();
    auto remoteKey = request.read<uint32_t>();
    crossbow::infinio::RemoteMemoryRegion remoteRegion(remoteAddress, remoteLength, remoteKey);

    auto selectionLength = request.read<uint32_t>();
    if (selectionLength % 8u != 0u || selectionLength < 8u) {
        writeErrorResponse(messageId, error::invalid_scan);
        return;
    }
    auto selectionData = request.read(selectionLength);
    std::unique_ptr<char[]> qbuffer(new char[selectionLength + sizeof(uint64_t)]);
    memcpy(qbuffer.get(), selectionData, selectionLength);

    request.advance(sizeof(uint32_t));
    auto queryLength = request.read<uint32_t>();
    auto queryData = request.read(queryLength);
    std::unique_ptr<char[]> query(new char[queryLength]);
    memcpy(query.get(), queryData, queryLength);

    request.align(sizeof(uint64_t));
    handleSnapshot(messageId, request,
            [this, messageId, tableId, &remoteRegion, selectionLength, &qbuffer, queryType, queryLength, &query]
            (const commitmanager::SnapshotDescriptor& snapshot) {
        auto scanId = static_cast<uint16_t>(messageId.userId() & 0xFFFFu);

        // Set maximum version field in selection query
        *reinterpret_cast<uint64_t*>(qbuffer.get() + selectionLength) = snapshot.version();
        auto qbufferLength = selectionLength + sizeof(uint64_t);

        // Copy snapshot descriptor if the snapshot is not completely described by the maximum version (i.e. version
        // bitmap is not empty)
        std::unique_ptr<commitmanager::SnapshotDescriptor> scanSnapshot;
        if (snapshot.baseVersion() != snapshot.version()) {
            scanSnapshot = commitmanager::SnapshotDescriptor::create(snapshot.lowestActiveVersion(),
                    snapshot.baseVersion(), snapshot.version(), snapshot.data());
        }

        auto table = mStorage.getTable(tableId);

        std::unique_ptr<ServerScanQuery> scanData(new ServerScanQuery(scanId, queryType, std::move(qbuffer),
                qbufferLength, std::move(query), queryLength, std::move(scanSnapshot), table->record(),
                manager().scanBufferManager(), std::move(remoteRegion), *this));
        auto scanDataPtr = scanData.get();
        auto res = mScans.emplace(scanId, std::move(scanData));
        if (!res.second) {
            writeErrorResponse(messageId, error::invalid_scan);
            return;
        }

        auto succeeded = mStorage.scan(tableId, scanDataPtr);
        if (!succeeded) {
            writeErrorResponse(messageId, error::server_overlad);
            mScans.erase(res.first);
        }
    });
}

void ServerSocket::handleScanProgress(crossbow::infinio::MessageId messageId, crossbow::buffer_reader& request) {
    auto scanId = static_cast<uint16_t>(messageId.userId() & 0xFFFFu);

    auto offsetRead = request.read<size_t>();

    auto i = mScans.find(scanId);
    if (i == mScans.end()) {
        LOG_DEBUG("Scan progress with invalid scan ID");
        writeErrorResponse(messageId, error::invalid_scan);
        return;
    }

    i->second->requestProgress(offsetRead);
}

void ServerSocket::onWrite(uint32_t userId, uint16_t bufferId, const std::error_code& ec) {
    // TODO We have to propagate the error to the ServerScanQuery so we can detach the scan
    if (ec) {
        handleSocketError(ec);
        return;
    }

    if (bufferId != crossbow::infinio::InfinibandBuffer::INVALID_ID) {
        auto& scanBufferManager = manager().scanBufferManager();
        scanBufferManager.releaseBuffer(bufferId);
    }

    mInflightScanBuffer -= 1u;

    auto status = static_cast<uint16_t>(userId & 0xFFFFu);
    switch (status) {
    case crossbow::to_underlying(ScanStatusIndicator::ONGOING): {
        // Nothing to do
    } break;

    case crossbow::to_underlying(ScanStatusIndicator::DONE): {
        auto scanId = static_cast<uint16_t>((userId >> 16) & 0xFFFFu);
        auto i = mScans.find(scanId);
        if (i == mScans.end()) {
            LOG_ERROR("Scan progress with invalid scan ID");
            return;
        }

        LOG_DEBUG("Scan with ID %1% finished", scanId);
        i->second->completeScan();
        mScans.erase(i);
    } break;

    default: {
        LOG_ERROR("Scan progress with invalid status");
    } break;
    }
}

template <typename Fun>
void ServerSocket::handleSnapshot(crossbow::infinio::MessageId messageId, crossbow::buffer_reader& message, Fun f) {
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
          mMaxBatchSize(config.maxBatchSize),
          mScanBufferManager(service, config),
          mMaxInflightScanBuffer(config.maxInflightScanBuffer) {
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

    return new ServerSocket(*this, mStorage, processor, std::move(socket), mMaxBatchSize, mMaxInflightScanBuffer);
}

} // namespace store
} // namespace tell
