#include "ClientConnection.hpp"

#include "ConnectionManager.hpp"

#include <network/ErrorCode.hpp>
#include <util/helper.hpp>

#include <crossbow/infinio/InfinibandBuffer.hpp>
#include <crossbow/logger.hpp>

namespace tell {
namespace store {

namespace {

/**
 * @brief Reads the snapshot descriptor from the message
 */
SnapshotDescriptor readSnapshot(uint64_t version, crossbow::infinio::BufferReader& request) {
    request.align(sizeof(uint32_t));
    auto descriptorLength = request.read<uint32_t>();
    auto descriptorData = request.read(descriptorLength);

    std::unique_ptr<unsigned char[]> dataBuffer(new unsigned char[descriptorLength]);
    memcpy(dataBuffer.get(), descriptorData, descriptorLength);
    return SnapshotDescriptor(dataBuffer.release(), descriptorLength, version);
}

} // anonymous namespace

ClientConnection::~ClientConnection() {
    try {
        mSocket->close();
    } catch (std::system_error& e) {
        LOG_ERROR("Error while closing socket [error = %1% %2%]", e.code(), e.what());
    }
}

void ClientConnection::shutdown() {
    mSocket->disconnect();
}

void ClientConnection::onConnected(const crossbow::string& data, const std::error_code& ec) {
    if (ec) {
        LOG_ERROR("Failure while establishing client connection [error = %1% %2%]", ec, ec.message());
        mManager.removeConnection(this);
    }
}

void ClientConnection::onMessage(uint64_t transactionId, uint32_t messageType,
        crossbow::infinio::BufferReader& request) {
    LOG_TRACE("TID %1%] Handling request of type %2%", transactionId, messageType);
    auto startTime = std::chrono::steady_clock::now();

    if (messageType > to_underlying(RequestType::LAST)) {
        messageType = to_underlying(RequestType::UNKOWN);
    }

    switch (from_underlying<RequestType>(messageType)) {

    /**
     * The create table request has the following format:
     * - 2 bytes: Length of the table name string
     * - x bytes: The table name string
     * - y bytes: Variable padding to make message 8 byte aligned
     * - 4 bytes: Length of the schema field
     * - x bytes: The table schema
     *
     * The response consists of the following format:
     * - 8 bytes: The table ID of the newly created table or 0 when the table already exists
     */
    case RequestType::CREATE_TABLE: {
        auto tableNameSize = request.read<uint16_t>();
        auto tableNameData = request.read(tableNameSize);
        request.align(sizeof(uint64_t));
        crossbow::string tableName(tableNameData, tableNameSize);

        // TODO Refactor schema serialization
        auto schemaData = request.data();
        auto schemaSize = request.read<uint32_t>();
        request.advance(schemaSize - sizeof(uint32_t));
        Schema schema(schemaData);

        uint64_t tableId = 0;
        auto succeeded = mStorage.createTable(tableName, schema, tableId);
        LOG_ASSERT((tableId != 0) || !succeeded, "Table ID of 0 does not denote failure");

        size_t messageSize = sizeof(uint64_t);
        std::error_code ec;
        auto response = writeResponse(transactionId, ResponseType::CREATE_TABLE, messageSize, ec);
        if (ec) {
            LOG_ERROR("Error while handling create table request [error = %1% %2%]", ec, ec.message());
            break;
        }
        response.write<uint64_t>(succeeded ? tableId : 0x0u);
    } break;

    /**
     * The get table ID request has the following format:
     * - 2 bytes: Length of the table name string
     * - x bytes: The table name string
     *
     * The response consists of the following format:
     * - 8 bytes: The table ID of the table or 0 when the table does not exist
     */
    case RequestType::GET_TABLEID: {
        auto tableNameSize = request.read<uint16_t>();
        auto tableNameData = request.read(tableNameSize);
        crossbow::string tableName(tableNameData, tableNameSize);

        uint64_t tableId = 0;
        auto succeeded = mStorage.getTableId(tableName, tableId);
        LOG_ASSERT((tableId == 0) ^ succeeded, "Table ID of 0 does not denote failure");

        size_t messageSize = sizeof(uint64_t);
        std::error_code ec;
        auto response = writeResponse(transactionId, ResponseType::GET_TABLEID, messageSize, ec);
        if (ec) {
            LOG_ERROR("Error while handling get table ID request [error = %1% %2%]", ec, ec.message());
            break;
        }
        response.write<uint64_t>(tableId);
    } break;

    /**
     * The get request has the following format:
     * - 8 bytes: The table ID of the requested tuple
     * - 8 bytes: The key of the requested tuple
     * - x bytes: Snapshot descriptor
     *
     * The response consists of the following format:
     * - 8 bytes: The version of the tuple (0 in this case, ony used for get newest)
     * - 1 byte:  Whether the tuple is the newest one
     * - 1 byte:  Whether the tuple was found
     * If the tuple was found:
     * - 2 bytes: Padding
     * - 4 bytes: Length of the tuple's data field
     * - x bytes: The tuple's data
     */
    case RequestType::GET: {
        auto tableId = request.read<uint64_t>();
        auto key = request.read<uint64_t>();
        handleSnapshot(transactionId, request,
                [this, &transactionId, &tableId, &key] (const SnapshotDescriptor& snapshot) {
            size_t size = 0;
            const char* data = nullptr;
            bool isNewest = false;
            auto success = mStorage.get(tableId, key, size, data, snapshot, isNewest);

            // Message size is 8 bytes version plus 8 bytes (isNewest, success, size) and data
            size_t messageSize = 2 * sizeof(uint64_t) + size;
            std::error_code ec;
            auto response = writeResponse(transactionId, ResponseType::GET, messageSize, ec);
            if (ec) {
                LOG_ERROR("Error while handling get request [error = %1% %2%]", ec, ec.message());
                return;
            }
            response.write<uint64_t>(0x0u);
            response.write<uint8_t>(isNewest ? 0x1u : 0x0u);
            response.write<uint8_t>(success ? 0x1u : 0x0u);
            if (success) {
                response.align(sizeof(uint32_t));
                response.write<uint32_t>(size);
                response.write(data, size);
            }
        });
    } break;

    /**
     * The get newest request has the following format:
     * - 8 bytes: The table ID of the requested tuple
     * - 8 bytes: The key of the requested tuple
     *
     * The response consists of the following format:
     * - 8 bytes: The version of the tuple
     * - 1 byte:  Whether the tuple is the newest one (always true in the case of get newest)
     * - 1 byte:  Whether the tuple was found
     * If the tuple was found:
     * - 2 bytes: Padding
     * - 4 bytes: Length of the tuple's data field
     * - x bytes: The tuple's data
     */
    case RequestType::GET_NEWEST: {
        auto tableId = request.read<uint64_t>();
        auto key = request.read<uint64_t>();

        size_t size = 0;
        const char* data = nullptr;
        uint64_t version = 0x0u;
        auto success = mStorage.getNewest(tableId, key, size, data, version);

        // Message size is 8 bytes version plus 8 bytes (isNewest, success, size) and data
        size_t messageSize = 2 * sizeof(uint64_t) + size;
        std::error_code ec;
        auto response = writeResponse(transactionId, ResponseType::GET, messageSize, ec);
        if (ec) {
            LOG_ERROR("Error while handling get newest request [error = %1% %2%]", ec, ec.message());
            break;
        }
        response.write<uint64_t>(version);
        response.write<uint8_t>(0x1u); // isNewest
        response.write<uint8_t>(success ? 0x1u : 0x0u);
        if (success) {
            response.align(sizeof(uint32_t));
            response.write<uint32_t>(size);
            response.write(data, size);
        }
    } break;

    /**
     * The update request has the following format:
     * - 8 bytes: The table ID of the requested tuple
     * - 8 bytes: The key of the requested tuple
     * - 4 bytes: Padding
     * - 4 bytes: Length of the tuple's data field
     * - x bytes: The tuple's data
     * - y bytes: Variable padding to make message 8 byte aligned
     * - x bytes: Snapshot descriptor
     *
     * The response consists of the following format:
     * - 1 byte:  Whether the update was successfull
     */
    case RequestType::UPDATE: {
        auto tableId = request.read<uint64_t>();
        auto key = request.read<uint64_t>();

        request.align(sizeof(uint32_t));
        auto dataLength = request.read<uint32_t>();
        auto data = request.read(dataLength);

        request.align(sizeof(uint64_t));
        handleSnapshot(transactionId, request,
                [this, &transactionId, &tableId, &key, &dataLength, &data] (const SnapshotDescriptor& snapshot) {
            auto succeeded = mStorage.update(tableId, key, dataLength, data, snapshot);

            // Message size is 1 byte (succeeded)
            size_t messageSize = sizeof(uint8_t);
            std::error_code ec;
            auto response = writeResponse(transactionId, ResponseType::MODIFICATION, messageSize, ec);
            if (ec) {
                LOG_ERROR("Error while handling update request [error = %1% %2%]", ec, ec.message());
                return;
            }
            response.write<uint8_t>(succeeded ? 0x1u : 0x0u);
        });
    } break;

    /**
     * The insert request has the following format:
     * - 8 bytes: The table ID of the requested tuple
     * - 8 bytes: The key of the requested tuple
     * - 1 byte:  Whether we want to know if the operation was successful or not
     * - 3 bytes: Padding
     * - 4 bytes: Length of the tuple's data field
     * - x bytes: The tuple's data
     * - y bytes: Variable padding to make message 8 byte aligned
     * - x bytes: Snapshot descriptor
     *
     * The response consists of the following format:
     * - 1 byte:  Whether the insert was successfull
     */
    case RequestType::INSERT: {
        auto tableId = request.read<uint64_t>();
        auto key = request.read<uint64_t>();
        bool wantsSucceeded = request.read<uint8_t>();

        request.align(sizeof(uint32_t));
        auto dataLength = request.read<uint32_t>();
        auto data = request.read(dataLength);

        request.align(sizeof(uint64_t));
        handleSnapshot(transactionId, request,
                [this, &transactionId, &tableId, &key, &wantsSucceeded, &dataLength, &data]
                (const SnapshotDescriptor& snapshot) {
            bool succeeded = false;
            mStorage.insert(tableId, key, dataLength, data, snapshot, (wantsSucceeded ? &succeeded : nullptr));

            // Message size is 1 byte (succeeded)
            size_t messageSize = sizeof(uint8_t);
            std::error_code ec;
            auto response = writeResponse(transactionId, ResponseType::MODIFICATION, messageSize, ec);
            if (ec) {
                LOG_ERROR("Error while handling insert request [error = %1% %2%]", ec, ec.message());
                return;
            }
            response.write<uint8_t>(succeeded ? 0x1u : 0x0u);
        });
    } break;

    /**
     * The remove request has the following format:
     * - 8 bytes: The table ID of the requested tuple
     * - 8 bytes: The key of the requested tuple
     * - x bytes: Snapshot descriptor
     *
     * The response consists of the following format:
     * - 1 byte:  Whether the remove was successfull
     */
    case RequestType::REMOVE: {
        auto tableId = request.read<uint64_t>();
        auto key = request.read<uint64_t>();

        handleSnapshot(transactionId, request,
                [this, &transactionId, &tableId, &key] (const SnapshotDescriptor& snapshot) {
            auto succeeded = mStorage.remove(tableId, key, snapshot);

            // Message size is 1 byte (succeeded)
            size_t messageSize = sizeof(uint8_t);
            std::error_code ec;
            auto response = writeResponse(transactionId, ResponseType::MODIFICATION, messageSize, ec);
            if (ec) {
                LOG_ERROR("Error while handling remove request [error = %1% %2%]", ec, ec.message());
                return;
            }
            response.write<uint8_t>(succeeded ? 0x1u : 0x0u);
        });
    } break;

    /**
     * The revert request has the following format:
     * - 8 bytes: The table ID of the requested tuple
     * - 8 bytes: The key of the requested tuple
     * - x bytes: Snapshot descriptor
     *
     * The response consists of the following format:
     * - 1 byte:  Whether the revert was successfull
     */
    case RequestType::REVERT: {
        auto tableId = request.read<uint64_t>();
        auto key = request.read<uint64_t>();

        handleSnapshot(transactionId, request,
                [this, &transactionId, &tableId, &key] (const SnapshotDescriptor& snapshot) {
            auto succeeded = mStorage.revert(tableId, key, snapshot);

            // Message size is 1 byte (succeeded)
            size_t messageSize = sizeof(uint8_t);
            std::error_code ec;
            auto response = writeResponse(transactionId, ResponseType::MODIFICATION, messageSize, ec);
            if (ec) {
                LOG_ERROR("Error while handling revert request [error = %1% %2%]", ec, ec.message());
                return;
            }
            response.write<uint8_t>(succeeded ? 0x1u : 0x0u);
        });
    } break;

    /**
     * The scan request has the following format:
     * - 8 bytes: The table ID of the requested tuple
     * - 2 bytes: The ID associated with this scan
     * - 6 bytes: Padding
     * - 8 bytes: The address of the remote memory region
     * - 8 bytes: Length of the remote memory region
     * - 4 bytes: The access key of the remote memory region
     * - 4 bytes: Length of the query buffer's data field
     * - x bytes: The query buffer's data
     * - y bytes: Variable padding to make message 8 byte aligned
     * - x bytes: Snapshot descriptor
     */
    case RequestType::SCAN: {
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
        handleSnapshot(transactionId, request,
                [this, &transactionId, &tableId, &scanId, &remoteRegion, &queryLength, &query]
                (const SnapshotDescriptor& snapshot) {
            auto descriptorLength = snapshot.length();
            std::unique_ptr<unsigned char[]> dataBuffer(new unsigned char[descriptorLength]);
            memcpy(dataBuffer.get(), snapshot.descriptor(), descriptorLength);
            SnapshotDescriptor scanSnapshot(dataBuffer.release(), descriptorLength, snapshot.version());

            std::unique_ptr<ClientScanQueryData> scanData(new ClientScanQueryData(transactionId,
                    std::move(scanSnapshot), mManager.pageRegion(), remoteRegion, mSocket));
            auto scanDataPtr = scanData.get();
            auto res = mScans.emplace(scanId, std::move(scanData));
            if (!res.second) {
                writeErrorResponse(transactionId, error::invalid_id);
                return;
            }

            std::vector<ScanQueryImpl*> impls;
            auto numScans = mStorage.numScanThreads();
            for (int i = 0; i < numScans; ++i) {
                impls.push_back(new ClientScanQuery(scanId, scanDataPtr));
            }

            auto succeeded = mStorage.scan(tableId, query.release(), queryLength, impls);
            if (!succeeded) {
                writeErrorResponse(transactionId, error::server_overlad);
                for (auto impl: impls) {
                    delete impl;
                }
                mScans.erase(res.first);
            }
        });
    } break;

    case RequestType::COMMIT: {
        // TODO Implement commit logic
    } break;

    default: {
        writeErrorResponse(transactionId, error::unkown_request);
    } break;
    }

    auto endTime = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(endTime - startTime);
    LOG_DEBUG("Handling request took %1%ns", duration.count());
}

void ClientConnection::onSocketError(const std::error_code& ec) {
    LOG_ERROR("Error during socket operation [error = %1% %2%]", ec, ec.message());
    shutdown();
}

void ClientConnection::onWrite(uint32_t userId, uint16_t bufferId, const std::error_code& ec) {
    // TODO We have to propagate the error to the ClientScanQuery so we can detach the scan
    if (ec) {
        onSocketError(ec);
        return;
    }

    if (userId == 0x0u || userId > to_underlying(ClientScanQueryData::ScanStatus::LAST)) {
        LOG_ERROR("Scan progress with invalid userid");
        return;
    }

    switch (from_underlying<ClientScanQueryData::ScanStatus>(userId)) {

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
        size_t messageSize = sizeof(uint16_t);
        std::error_code ec2;
        auto response = writeResponse(i->second->transactionId(), ResponseType::SCAN, messageSize, ec2);
        if (ec2) {
            LOG_ERROR("Error while writing scan response [error = %1% %2%]", ec2, ec2.message());
            return;
        }
        response.write<uint16_t>(bufferId);
        mScans.erase(i);
    } break;

    default:
        break;
    }
}

void ClientConnection::onDisconnect() {
    shutdown();
}

void ClientConnection::onDisconnected() {
    // Clear snapshot cache - No more handlers are active so we do not need to synchronize
    mSnapshots.clear();

    mManager.removeConnection(this);
}

template <typename Fun>
void ClientConnection::handleSnapshot(uint64_t transactionId, crossbow::infinio::BufferReader& request, Fun f) {
    /**
     * The snapshot descriptor has the following format:
     * - 8 bytes: The version of the snapshot
     * - 1 byte:  Whether we want to get / put the snapshot descriptor from / into the cache
     * - 1 byte:  Whether we sent the full descriptor
     * If the message contains a full descriptor:
     * - 2 bytes: Padding
     * - 4 bytes: Length of the descriptor
     * - x bytes: The descriptor data
     */
    auto version = request.read<uint64_t>();
    bool cached = request.read<uint8_t>();
    bool hasDescriptor = request.read<uint8_t>();
    if (cached) {
        auto i = mSnapshots.find(version);

        // Either we already have the snapshot in our cache or the client send it to us
        auto found = (i != mSnapshots.end());
        if (found ^ hasDescriptor) {
            writeErrorResponse(transactionId, error::invalid_snapshot);
            return;
        }

        if (!found) {
            // We have to add the snapshot to the cache
            auto res = mSnapshots.emplace(version, readSnapshot(version, request));
            if (!res.second) { // Element was inserted by another thread
                writeErrorResponse(transactionId, error::invalid_snapshot);
                return;
            }
            i = res.first;
        }

        f(i->second);
    } else {
        if (!hasDescriptor) {
            writeErrorResponse(transactionId, error::invalid_snapshot);
            return;
        }
        f(readSnapshot(version, request));
    }
}

void ClientConnection::removeSnapshot(uint64_t version) {
    auto i = mSnapshots.find(version);
    if (i == mSnapshots.end()) {
        return;
    }
    mSnapshots.erase(i);
}

void ClientConnection::writeErrorResponse(uint64_t transactionId, error::server_errors error) {
    std::error_code ec;
    auto message = writeResponse(transactionId, ResponseType::ERROR, sizeof(uint64_t), ec);
    if (ec) {
        LOG_ERROR("Error while acquiring message buffer [error = %1% %2%]", ec, ec.message());
        return;
    }

    message.write<uint64_t>(static_cast<uint64_t>(error));
}

} // namespace store
} // namespace tell
