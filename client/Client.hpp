#pragma once

#include "ClientConfig.hpp"
#include "TransactionManager.hpp"

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
              mManager(dispatcher) {
    }

    void init();

    void shutdown();

private:
    void executeTransaction(Transaction& transaction);

    ClientConfig mConfig;

    TransactionManager mManager;
};

} // namespace store
} // namespace tell
