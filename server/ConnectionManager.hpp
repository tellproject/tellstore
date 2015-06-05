#pragma once

#include "ServerConfig.hpp"

#include <tellstore.hpp>

#include <crossbow/infinio/InfinibandBuffer.hpp>
#include <crossbow/infinio/InfinibandService.hpp>
#include <crossbow/infinio/InfinibandSocket.hpp>

#include <tbb/queuing_rw_mutex.h>
#include <tbb/concurrent_unordered_set.h>

#include <system_error>

namespace tell {
namespace store {

class ClientConnection;

class ConnectionManager : private crossbow::infinio::InfinibandAcceptorHandler {
public:
    ConnectionManager(Storage& storage, const ServerConfig& config)
            : mConfig(config),
              mStorage(storage),
              mService(mConfig.infinibandLimits),
              mAcceptor(mService.createAcceptor()) {
    }

    void init(std::error_code& ec);

    void shutdown();


private:
    friend class ClientConnection;

    virtual void onConnection(crossbow::infinio::InfinibandSocket socket, const crossbow::string& data) final override;

    void removeConnection(ClientConnection* con);

    crossbow::infinio::LocalMemoryRegion& pageRegion() {
        return mPageRegion;
    }

    ServerConfig mConfig;

    Storage& mStorage;

    crossbow::infinio::InfinibandService mService;
    crossbow::infinio::InfinibandAcceptor mAcceptor;
    crossbow::infinio::LocalMemoryRegion mPageRegion;

    tbb::queuing_rw_mutex mConnectionsMutex;
    tbb::concurrent_unordered_set<ClientConnection*> mConnections;
};

} // namespace store
} // namespace tell
