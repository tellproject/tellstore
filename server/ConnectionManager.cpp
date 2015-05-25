#include "ConnectionManager.hpp"

#include "ClientConnection.hpp"

#include <crossbow/infinio/Endpoint.hpp>

#include <util/Epoch.hpp>
#include <util/Logging.hpp>

namespace tell {
namespace store {

void ConnectionManager::init(std::error_code& ec) {
    LOG_INFO("Initializing the connection manager");

    // Open socket
    mAcceptor->open(ec);
    if (ec) {
        return;
    }
    mAcceptor->setHandler(this);

    // Bind socket
    crossbow::infinio::Endpoint ep(crossbow::infinio::Endpoint::ipv4(), mConfig.port);
    mAcceptor->bind(ep, ec);
    if (ec) {
        return;
    }

    // Start listening
    mAcceptor->listen(10, ec);
    if (ec) {
        return;
    }

    LOG_INFO("TellStore listening on port %1%", mConfig.port);
    mService.run();
}

void ConnectionManager::shutdown() {
    LOG_INFO("Shutting down the connection manager");

    std::error_code ec;
    mAcceptor->close(ec);
    if (ec) {
        // TODO Handle this situation somehow
    }

    for (auto& con : mConnections) {
        con->shutdown();
    }
}

void ConnectionManager::onConnection(crossbow::infinio::InfinibandSocket socket, const crossbow::string& data) {
    LOG_INFO("New incoming connection");
    auto con = new (allocator::malloc(sizeof(ClientConnection))) ClientConnection(*this, mStorage, socket);
    con->init();

    std::error_code ec;
    socket->accept(crossbow::string(), 0, ec);
    if (ec) {
        LOG_ERROR("Error accepting connection [error = %1% %2%]", ec, ec.message());
        socket->close(ec);
        if (ec) {
            LOG_ERROR("Error closing failed connection [error = %1% %2%]", ec, ec.message());
        }
        con->~ClientConnection();
        allocator::free_now(con);
        return;
    }

    tbb::queuing_rw_mutex::scoped_lock lock(mConnectionsMutex, false);
    auto res = mConnections.insert(con);
    LOG_ASSERT(res.second, "New connection already in connection set");
}

void ConnectionManager::removeConnection(ClientConnection* con) {
    LOG_INFO("Removing connection");
    {
        tbb::queuing_rw_mutex::scoped_lock lock(mConnectionsMutex, true);
        mConnections.unsafe_erase(con);
    }

    allocator::free(con, [con] () {
        con->~ClientConnection();
    });
}

} // namespace store
} // namespace tell
