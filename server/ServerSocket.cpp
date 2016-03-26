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
#include <boost/format.hpp>

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

namespace {

std::atomic<unsigned> requests(0);
std::chrono::steady_clock::time_point lastMeasurement = std::chrono::steady_clock::now();

}

void ServerSocket::onRequest(crossbow::infinio::MessageId messageId, uint32_t messageType,
        crossbow::buffer_reader& request) {
#ifdef NDEBUG
#else
    LOG_TRACE("MID %1%] Handling request of type %2%", messageId.userId(), messageType);
    auto startTime = std::chrono::steady_clock::now();
#endif
    {
        auto reqs = requests.fetch_add(1) + 1;
        auto n = std::chrono::steady_clock::now();
        auto l = lastMeasurement;
        if (n > l
                && std::chrono::duration_cast<std::chrono::seconds>(n - l).count() >= 60
                && requests.compare_exchange_strong(reqs, 0))
        {
            lastMeasurement = n;
            std::cout << boost::format("%1% requests") % reqs  << std::endl;
        }
    }

    MemoryConsumerLock memoryLock(mMemoryConsumer);

    switch (messageType) {

    case crossbow::to_underlying(RequestType::CREATE_TABLE): {
        handleCreateTable(messageId, request);
    } break;

    case crossbow::to_underlying(RequestType::GET_TABLES): {
        handleGetTables(messageId, request);
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

void ServerSocket::handleGetTables(crossbow::infinio::MessageId messageId, crossbow::buffer_reader& request) {
    auto tables = mStorage.getTables();

    uint32_t messageLength = sizeof(uint64_t);
    for (auto table : tables) {
        messageLength += sizeof(uint64_t) + sizeof(uint32_t);
        messageLength += table->tableName().size();
        messageLength = crossbow::align(messageLength, 8u);
        messageLength += table->schema().serializedLength();
    }

    writeResponse(messageId, ResponseType::GET_TABLES, messageLength, [&tables]
            (crossbow::buffer_writer& message, std::error_code& /* ec */) {
        message.write<uint64_t>(tables.size());
        for (auto table : tables) {
            message.write<uint64_t>(table->tableId());

            auto& tableName = table->tableName();
            message.write<uint32_t>(tableName.size());
            message.write(tableName.data(), tableName.size());
            message.align(8u);

            auto& schema = table->schema();
            schema.serialize(message);
        }
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
        auto ec = mStorage.get(tableId, key, snapshot, [this, messageId]
                (size_t size, uint64_t version, bool isNewest) {
            char* data = nullptr;
            // Message size is 8 bytes version plus 8 bytes (isNewest, size) and data
            uint32_t messageLength = 2 * sizeof(uint64_t) + size;
            writeResponse(messageId, ResponseType::GET, messageLength, [size, version, isNewest, &data]
                    (crossbow::buffer_writer& message, std::error_code& /* ec */) {
                message.write<uint64_t>(version);
                message.write<uint8_t>(isNewest ? 0x1u : 0x0u);
                message.set(0, sizeof(uint32_t) - sizeof(uint8_t));
                message.write<uint32_t>(size);
                data = message.data();
            });
            return data;
        });

        if (ec) {
            writeErrorResponse(messageId, static_cast<error::errors>(ec));
            return;
        }
    });
}

void ServerSocket::handleUpdate(crossbow::infinio::MessageId messageId, crossbow::buffer_reader& request) {
    auto tableId = request.read<uint64_t>();
    auto key = request.read<uint64_t>();

    request.advance(sizeof(uint32_t));
    auto dataLength = request.read<uint32_t>();
    auto data = request.read(dataLength);
    request.align(8u);

    handleSnapshot(messageId, request, [this, messageId, tableId, key, dataLength, data]
            (const commitmanager::SnapshotDescriptor& snapshot) {
        auto ec = mStorage.update(tableId, key, dataLength, data, snapshot);
        writeModificationResponse(messageId, ec);
    });
}

void ServerSocket::handleInsert(crossbow::infinio::MessageId messageId, crossbow::buffer_reader& request) {
    auto tableId = request.read<uint64_t>();
    auto key = request.read<uint64_t>();

    request.advance(sizeof(uint32_t));
    auto dataLength = request.read<uint32_t>();
    auto data = request.read(dataLength);
    request.align(8u);

    handleSnapshot(messageId, request, [this, messageId, tableId, key, dataLength, data]
            (const commitmanager::SnapshotDescriptor& snapshot) {
        auto ec = mStorage.insert(tableId, key, dataLength, data, snapshot);
        writeModificationResponse(messageId, ec);
    });
}

void ServerSocket::handleRemove(crossbow::infinio::MessageId messageId, crossbow::buffer_reader& request) {
    auto tableId = request.read<uint64_t>();
    auto key = request.read<uint64_t>();

    handleSnapshot(messageId, request, [this, messageId, tableId, key]
            (const commitmanager::SnapshotDescriptor& snapshot) {
        auto ec = mStorage.remove(tableId, key, snapshot);
        writeModificationResponse(messageId, ec);
    });
}

void ServerSocket::handleRevert(crossbow::infinio::MessageId messageId, crossbow::buffer_reader& request) {
    auto tableId = request.read<uint64_t>();
    auto key = request.read<uint64_t>();

    handleSnapshot(messageId, request, [this, messageId, tableId, key]
            (const commitmanager::SnapshotDescriptor& snapshot) {
        auto ec = mStorage.revert(tableId, key, snapshot);
        writeModificationResponse(messageId, ec);
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
    if (selectionLength % 8u != 0u || selectionLength < 16u) {
        writeErrorResponse(messageId, error::invalid_scan);
        return;
    }
    auto selectionData = request.read(selectionLength);
    std::unique_ptr<char[]> selection(new char[selectionLength]);
    memcpy(selection.get(), selectionData, selectionLength);

    request.advance(sizeof(uint32_t));
    auto queryLength = request.read<uint32_t>();
    auto queryData = request.read(queryLength);
    std::unique_ptr<char[]> query(new char[queryLength]);
    memcpy(query.get(), queryData, queryLength);

    request.align(sizeof(uint64_t));
    handleSnapshot(messageId, request,
            [this, messageId, tableId, &remoteRegion, selectionLength, &selection, queryType, queryLength, &query]
            (const commitmanager::SnapshotDescriptor& snapshot) {
        auto scanId = static_cast<uint16_t>(messageId.userId() & 0xFFFFu);

        // Copy snapshot descriptor
        auto scanSnapshot = commitmanager::SnapshotDescriptor::create(snapshot.lowestActiveVersion(),
                snapshot.baseVersion(), snapshot.version(), snapshot.data());

        auto table = mStorage.getTable(tableId);

        std::unique_ptr<ServerScanQuery> scanData(new ServerScanQuery(scanId, queryType, std::move(selection),
                selectionLength, std::move(query), queryLength, std::move(scanSnapshot), table->record(),
                manager().scanBufferManager(), std::move(remoteRegion), *this));
        auto scanDataPtr = scanData.get();
        auto res = mScans.emplace(scanId, std::move(scanData));
        if (!res.second) {
            writeErrorResponse(messageId, error::invalid_scan);
            return;
        }

        auto ec = mStorage.scan(tableId, scanDataPtr);
        if (ec) {
            writeErrorResponse(messageId, static_cast<error::errors>(ec));
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

void ServerSocket::writeModificationResponse(crossbow::infinio::MessageId messageId, int ec) {
    if (ec) {
        writeErrorResponse(messageId, static_cast<error::errors>(ec));
    } else {
        writeResponse(messageId, ResponseType::MODIFICATION, 0, []
                (crossbow::buffer_writer& /* message */, std::error_code& /* ec */) {
        });
    }
}

ServerManager::ServerManager(crossbow::infinio::InfinibandService& service, Storage& storage,
        const ServerConfig& config)
        : Base(service, config.port),
          mStorage(storage),
          mMaxBatchSize(config.maxBatchSize),
          mScanBufferManager(service, config),
          mMaxInflightScanBuffer(config.maxInflightScanBuffer) {
    for (decltype(config.numNetworkThreads) i = 0; i < config.numNetworkThreads; ++i) {
        mProcessors.emplace_back(service.createProcessor(), storage.createMemoryConsumer());
    }
}

ServerSocket* ServerManager::createConnection(crossbow::infinio::InfinibandSocket socket,
        const crossbow::string& data) {
    // The length of the data field may be larger than the actual data sent (behavior of librdma_cm) so check the
    // handshake against the substring
    auto& handshake = handshakeString();
    if (data.size() < (handshake.size() + sizeof(uint64_t)) || data.substr(0, handshake.size()) != handshake) {
        LOG_ERROR("Connection handshake failed");
        return nullptr;
    }
    auto thread = *reinterpret_cast<const uint64_t*>(&data[handshake.size()]);
    auto& processor = mProcessors.at(thread % mProcessors.size());

    LOG_INFO("%1%] New client connection on processor %2%", socket->remoteAddress(), thread);
    return new ServerSocket(*this, mStorage, *processor.eventProcessor, processor.memoryConsumer.get(),
            std::move(socket), mMaxBatchSize, mMaxInflightScanBuffer);
}

} // namespace store
} // namespace tell
