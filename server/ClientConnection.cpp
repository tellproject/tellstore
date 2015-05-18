#include "ClientConnection.hpp"

#include "ConnectionManager.hpp"

#include <util/Epoch.hpp>
#include <util/ErrorCode.hpp>
#include <util/Logging.hpp>
#include <util/MessageTypes.hpp>
#include <util/MessageWriter.hpp>

namespace tell {
namespace store {

namespace {

/**
 * @brief Reads the snapshot descriptor from the message
 */
SnapshotDescriptor readSnapshot(uint64_t version, BufferReader& request) {
    request.align(sizeof(uint32_t));
    auto descriptorLength = request.read<uint32_t>();
    auto descriptorData = request.read(descriptorLength);

    std::unique_ptr<unsigned char[]> dataBuffer(new unsigned char[descriptorLength]);
    memcpy(dataBuffer.get(), descriptorData, descriptorLength);
    return SnapshotDescriptor(dataBuffer.release(), descriptorLength, version);
}

} // anonymous namespace

ClientConnection::~ClientConnection() {
    boost::system::error_code ec;
    mSocket.close(ec);
    if (ec) {
        // TODO Handle this situation somehow (this should probably not happen at this point)
    }
}

void ClientConnection::init() {
    mSocket.setHandler(this);
}

void ClientConnection::shutdown() {
    boost::system::error_code ec;
    mSocket.disconnect(ec);
    if (ec) {
        LOG_ERROR("Error disconnecting [error = %1% %2%]", ec, ec.message());
        // TODO Handle this situation somehow - Can this even happen?
    }
}

void ClientConnection::onConnected(const boost::system::error_code& ec) {
    if (ec) {
        LOG_ERROR("Failure while establishing client connection [error = %1% %2%]", ec, ec.message());
        mManager.removeConnection(this);
    }
}

void ClientConnection::onReceive(const void* buffer, size_t length, const boost::system::error_code& ec) {
    if (ec) {
        LOG_ERROR("Error receiving message [error = %1% %2%]", ec, ec.message());
        // TODO Handle this situation somehow
        return;
    }

    auto startTime = std::chrono::high_resolution_clock::now();

    BufferReader request(reinterpret_cast<const char*>(buffer), length);
    MessageWriter writer(mSocket);
    while (!request.exhausted()) {
        auto transactionId = request.read<uint64_t>();
        auto requestType = request.read<uint64_t>();
        if (requestType > static_cast<uint64_t>(RequestType::LAST)) {
            requestType = static_cast<uint64_t>(RequestType::UNKOWN);
        }

        LOG_TRACE("TID %1%] Handling request of type %2%", transactionId, requestType);

        switch (static_cast<RequestType>(requestType)) {

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
            boost::system::error_code ec;
            auto response = writer.writeResponse(transactionId, ResponseType::CREATE_TABLE, messageSize, ec);
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
            boost::system::error_code ec;
            auto response = writer.writeResponse(transactionId, ResponseType::GET_TABLEID, messageSize, ec);
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
            handleSnapshot(transactionId, request, writer,
                    [this, &writer, &transactionId, &tableId, &key] (const SnapshotDescriptor& snapshot) {
                size_t size = 0;
                const char* data = nullptr;
                bool isNewest = false;
                auto success = mStorage.get(tableId, key, size, data, snapshot, isNewest);

                // Message size is 8 bytes version plus 8 bytes (isNewest, success, size) and data
                size_t messageSize = 2 * sizeof(uint64_t) + size;
                boost::system::error_code ec;
                auto response = writer.writeResponse(transactionId, ResponseType::GET, messageSize, ec);
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
            boost::system::error_code ec;
            auto response = writer.writeResponse(transactionId, ResponseType::GET, messageSize, ec);
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
            handleSnapshot(transactionId, request, writer,
                    [this, &writer, &transactionId, &tableId, &key, &dataLength, &data]
                    (const SnapshotDescriptor& snapshot) {
                auto succeeded = mStorage.update(tableId, key, dataLength, data, snapshot);

                // Message size is 1 byte (succeeded)
                size_t messageSize = sizeof(uint8_t);
                boost::system::error_code ec;
                auto response = writer.writeResponse(transactionId, ResponseType::MODIFICATION, messageSize, ec);
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
            handleSnapshot(transactionId, request, writer,
                    [this, &writer, &transactionId, &tableId, &key, &wantsSucceeded, &dataLength, &data]
                    (const SnapshotDescriptor& snapshot) {
                bool succeeded = false;
                mStorage.insert(tableId, key, dataLength, data, snapshot, (wantsSucceeded ? &succeeded : nullptr));

                // Message size is 1 byte (succeeded)
                size_t messageSize = sizeof(uint8_t);
                boost::system::error_code ec;
                auto response = writer.writeResponse(transactionId, ResponseType::MODIFICATION, messageSize, ec);
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

            handleSnapshot(transactionId, request, writer,
                    [this, &writer, &transactionId, &tableId, &key] (const SnapshotDescriptor& snapshot) {
                auto succeeded = mStorage.remove(tableId, key, snapshot);

                // Message size is 1 byte (succeeded)
                size_t messageSize = sizeof(uint8_t);
                boost::system::error_code ec;
                auto response = writer.writeResponse(transactionId, ResponseType::MODIFICATION, messageSize, ec);
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

            handleSnapshot(transactionId, request, writer,
                    [this, &writer, &transactionId, &tableId, &key] (const SnapshotDescriptor& snapshot) {
                auto succeeded = mStorage.revert(tableId, key, snapshot);

                // Message size is 1 byte (succeeded)
                size_t messageSize = sizeof(uint8_t);
                boost::system::error_code ec;
                auto response = writer.writeResponse(transactionId, ResponseType::MODIFICATION, messageSize, ec);
                if (ec) {
                    LOG_ERROR("Error while handling revert request [error = %1% %2%]", ec, ec.message());
                    return;
                }
                response.write<uint8_t>(succeeded ? 0x1u : 0x0u);
            });
        } break;

        case RequestType::COMMIT: {
            // TODO Implement commit logic
        } break;

        default: {
            boost::system::error_code ec;
            writer.writeErrorResponse(transactionId, error::unkown_request, ec);
            if (ec) {
                LOG_ERROR("Error while handling an unknown request [error = %1% %2%]", ec, ec.message());
                break;
            }
        } break;
        }

        request.align(sizeof(uint64_t));
    }

    // Send remaining response batch
    boost::system::error_code ec2;
    writer.flush(ec2);
    if (ec2) {
        LOG_ERROR("Error while flushing response batch [error = %1% %2%]", ec2, ec2.message());
    }

    auto endTime = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(endTime - startTime);
    LOG_DEBUG("Handling request took %1%ns", duration.count());
}

void ClientConnection::onSend(uint32_t userId, const boost::system::error_code& ec) {
    if (ec) {
        LOG_ERROR("Error sending message [error = %1% %2%]", ec, ec.message());
        // TODO Handle this situation somehow
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
void ClientConnection::handleSnapshot(uint64_t transactionId, BufferReader& request, MessageWriter& writer, Fun f) {
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
        tbb::queuing_rw_mutex::scoped_lock lock(mSnapshotsMutex, false);
        auto i = mSnapshots.find(version);

        // Either we already have the snapshot in our cache or the client send it to us
        auto found = (i != mSnapshots.end());
        if (found ^ hasDescriptor) {
            boost::system::error_code ec;
            writer.writeErrorResponse(transactionId, error::invalid_snapshot, ec);
            if (ec) {
                LOG_ERROR("Error while writing error response [error = %1% %2%]", ec, ec.message());
            }
            return;
        }

        if (!found) {
            // We have to add the snapshot to the cache
            auto res = mSnapshots.insert(std::make_pair(version, readSnapshot(version, request)));
            if (!res.second) { // Element was inserted by another thread
                boost::system::error_code ec;
                writer.writeErrorResponse(transactionId, error::invalid_snapshot, ec);
                if (ec) {
                    LOG_ERROR("Error while writing error response [error = %1% %2%]", ec, ec.message());
                }
                return;
            }
            i = res.first;
        }

        f(i->second);
    } else {
        if (!hasDescriptor) {
            boost::system::error_code ec;
            writer.writeErrorResponse(transactionId, error::invalid_snapshot, ec);
            if (ec) {
                LOG_ERROR("Error while writing error response [error = %1% %2%]", ec, ec.message());
            }
            return;
        }
        f(readSnapshot(version, request));
    }
}

void ClientConnection::removeSnapshot(uint64_t version) {
    tbb::queuing_rw_mutex::scoped_lock lock(mSnapshotsMutex, true);
    mSnapshots.unsafe_erase(version);
}

} // namespace store
} // namespace tell
