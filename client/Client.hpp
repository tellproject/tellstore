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
    Client(const ClientConfig& config);

    void init();

    void shutdown();

private:
    void addTable(Transaction& transaction);

    void executeTransaction(Transaction& transaction, uint64_t startKey, uint64_t endKey);

    void doScan(Transaction& transaction, size_t querySize, const char* query, const SnapshotDescriptor& snapshot);

    const char* getTupleData(const char* data, Record& record, const crossbow::string& name);

    crossbow::infinio::InfinibandService mService;

    ClientConfig mConfig;

    TransactionManager mManager;

    CommitManager mCommitManager;
    Schema mSchema;

    uint64_t mTableId;

    std::atomic<size_t> mActiveTransactions;

    uint64_t mTupleSize;
    std::unique_ptr<char[]> mTuple1;
    std::unique_ptr<char[]> mTuple2;
};

} // namespace store
} // namespace tell
