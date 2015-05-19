#pragma once

#include "ClientConfig.hpp"
#include "TransactionManager.hpp"

#include <util/CommitManager.hpp>
#include <util/Record.hpp>

#include <crossbow/infinio/EventDispatcher.hpp>
#include <crossbow/infinio/InfinibandService.hpp>

#include <tbb/spin_mutex.h>

#include <cstdint>
#include <memory>
#include <vector>

namespace tell {
namespace store {

class Client {
public:
    Client(crossbow::infinio::EventDispatcher& dispatcher, const ClientConfig& config)
            : mDispatcher(dispatcher),
              mService(dispatcher, config.infinibandLimits),
              mConfig(config),
              mManager(mService),
              mTableId(0x0u) {
        mSchema.addField(FieldType::INT, "foo", true);
    }

    void init();

    void shutdown();

private:
    void addTable(Transaction& transaction);

    void executeTransaction(Transaction& transaction, uint64_t startKey, uint64_t endKey);

    crossbow::infinio::EventDispatcher& mDispatcher;
    crossbow::infinio::InfinibandService mService;

    ClientConfig mConfig;

    TransactionManager mManager;

    CommitManager mCommitManager;
    Schema mSchema;

    uint64_t mTableId;

    tbb::spin_mutex mTransMutex;
    std::vector<std::unique_ptr<Transaction>> mTrans;
};

} // namespace store
} // namespace tell
