#include "ConnectionManager.hpp"

#include "ClientConnection.hpp"

#include <util/PageManager.hpp>

#include <crossbow/allocator.hpp>
#include <crossbow/infinio/Endpoint.hpp>
#include <crossbow/logger.hpp>

namespace tell {
namespace store {

ConnectionManager::ConnectionManager(Storage& storage, const ServerConfig& config)
        : mConfig(config),
          mStorage(storage),
          mService(mConfig.infinibandLimits),
          mAcceptor(mService.createAcceptor()),
          mPageRegion(mService.registerMemoryRegion(mStorage.pageManager().data(), mStorage.pageManager().size(), 0)) {
    LOG_INFO("Initializing the connection manager");

    // Open socket
    crossbow::infinio::Endpoint ep(crossbow::infinio::Endpoint::ipv4(), mConfig.port);
    mAcceptor->open();
    mAcceptor->setHandler(this);
    mAcceptor->bind(ep);
    mAcceptor->listen(10);

    LOG_INFO("TellStore listening on port %1%", mConfig.port);
    mService.run();
}

void ConnectionManager::shutdown() {
    LOG_INFO("Shutting down the connection manager");

    mAcceptor->close();

    for (auto& con : mConnections) {
        con->shutdown();
        crossbow::allocator::destroy(con);
    }
}

void ConnectionManager::onConnection(crossbow::infinio::InfinibandSocket socket, const crossbow::string& data) {
    if (data.size() < sizeof(uint64_t)) {
        LOG_ERROR("Client did not send enough data in connection attempt");
        return;
    }
    auto thread = *reinterpret_cast<const uint64_t*>(&data.front());

    LOG_INFO("New incoming connection [address = %1%, thread = %2%]", socket->remoteAddress(), thread);

    auto con = crossbow::allocator::construct<ClientConnection>(*this, mStorage, socket);

    try {
        socket->accept(crossbow::string(), thread);
    } catch (std::system_error& e) {
        LOG_ERROR("Error accepting connection [error = %1% %2%]", e.code(), e.what());
        crossbow::allocator::destroy_now(con);
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

    crossbow::allocator::destroy(con);
}

} // namespace store
} // namespace tell
