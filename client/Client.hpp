#pragma once

#include "ClientConfig.hpp"
#include "ClientManager.hpp"

#include <util/GenericTuple.hpp>

#include <crossbow/infinio/InfinibandService.hpp>

#include <array>
#include <cstdint>
#include <memory>

namespace tell {
namespace store {

class Client {
public:
    Client(const ClientConfig& config);

    void init();

    void shutdown();

private:
    void addTable(ClientHandle& client);

    void executeTransaction(ClientHandle& client, uint64_t startKey, uint64_t endKey);

    void doScan(ClientTransaction& transaction, const Table& record, float selectivity);

    ClientConfig mConfig;

    crossbow::infinio::InfinibandService mService;

    ClientManager mManager;

    std::atomic<size_t> mActiveTransactions;

    uint64_t mTupleSize;
    std::array<GenericTuple, 4> mTuple;
};

} // namespace store
} // namespace tell
