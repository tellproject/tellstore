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
    mAcceptor.open(ec);
    if (ec) {
        return;
    }
    mAcceptor.setHandler(this);

    // Bind socket
    mAcceptor.bind(crossbow::infinio::Endpoint(crossbow::infinio::Endpoint::ipv4(), mConfig.port), ec);
    if (ec) {
        return;
    }

    // Start listening
    mAcceptor.listen(10, ec);
    if (ec) {
        return;
    }

    LOG_INFO("TellStore listening on port %1%", mConfig.port);
}

void ConnectionManager::shutdown() {
    LOG_INFO("Shutting down the connection manager");

    std::error_code ec;
    mAcceptor.close(ec);
    if (ec) {
        // TODO Handle this situation somehow
    }

    for (auto& con : mConnections) {
        con->shutdown();
    }
}

bool ConnectionManager::onConnection(crossbow::infinio::InfinibandSocket socket) {
    LOG_INFO("New incoming connection");
    auto con = new (allocator::malloc(sizeof(ClientConnection))) ClientConnection(*this, mStorage, std::move(socket));
    con->init();

    tbb::queuing_rw_mutex::scoped_lock lock(mConnectionsMutex, false);
    auto res = mConnections.insert(con);
    LOG_ASSERT(res.second, "New connection already in connection set");

    return true;
}

void ConnectionManager::removeConnection(ClientConnection* con) {
    LOG_INFO("Removing connection");
    {
        tbb::queuing_rw_mutex::scoped_lock lock(mConnectionsMutex, true);
        mConnections.unsafe_erase(con);
    }

    allocator::free(con);
}

} // namespace store
} // namespace tell
