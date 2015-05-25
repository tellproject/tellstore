#pragma once

#include "ClientConfig.hpp"
#include "TransactionManager.hpp"

#include <util/CommitManager.hpp>
#include <util/Record.hpp>

#include <crossbow/infinio/InfinibandService.hpp>

#include <tbb/spin_mutex.h>

#include <cstdint>
#include <memory>
#include <vector>

namespace tell {
namespace store {

class Client {
public:
    Client(const ClientConfig& config)
            : mService(config.infinibandLimits),
              mConfig(config),
              mManager(mService),
              mTableId(0x0u) {
        mSchema.addField(FieldType::INT, "number", true);
        mSchema.addField(FieldType::TEXT, "text1", true);
        mSchema.addField(FieldType::BIGINT, "largenumber", true);
        mSchema.addField(FieldType::TEXT, "text2", true);
    }

    void init();

    void shutdown();

private:
    void addTable(Transaction& transaction);

    void executeTransaction(Transaction& transaction, uint64_t startKey, uint64_t endKey);

    const char* getTupleData(const char* data, Record& record, const crossbow::string& name);

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
