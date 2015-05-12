#pragma once

#include "ClientConfig.hpp"
#include "ServerConnection.hpp"

#include <crossbow/infinio/InfinibandService.hpp>

namespace crossbow {
namespace infinio {
class EventDispatcher;
} // namespace infinio
} // namespace crossbow

namespace tell {
namespace store {

class Client {
public:
    Client(crossbow::infinio::EventDispatcher& dispatcher, const ClientConfig& config)
            : mConfig(config),
              mService(dispatcher),
              mConnection(mService) {
    }

    void init();

    void shutdown();

private:
    ClientConfig mConfig;

    crossbow::infinio::InfinibandService mService;

    ServerConnection mConnection;
};

} // namespace store
} // namespace tell
