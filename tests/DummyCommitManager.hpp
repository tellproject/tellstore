#pragma once

#include <commitmanager/CommitManager.hpp>

#include <crossbow/non_copyable.hpp>

#include <cstdint>
#include <mutex>

namespace tell {
namespace store {

class Transaction;

class DummyCommitManager {
public:
    Transaction startTx(bool readonly = false);

private:
    friend class Transaction;

    void abortTx(uint64_t version) {
        commitTx(version);
    }

    void commitTx(uint64_t version);

    commitmanager::CommitManager mManager;

    mutable std::mutex mMutex;
    using Lock = std::lock_guard<std::mutex>;
};

using CommitManager = DummyCommitManager;

/**
 * @brief Simple snapshot holder
 *
 * The idea behind this object is, to make sure that a user never forgets to commit or abort a transaction. Objects of
 * this class will simply hold a snapshot descriptor and will commit a transaction when the object gets destroyed (if
 * the user did not do so manually).
 */
class Transaction : crossbow::non_copyable {
    DummyCommitManager* mManager;
    std::unique_ptr<commitmanager::SnapshotDescriptor> mDescriptor;
    bool mCommitted = false;
public:
    Transaction(DummyCommitManager& manager, std::unique_ptr<commitmanager::SnapshotDescriptor> snapshot)
        : mManager(&manager),
          mDescriptor(std::move(snapshot)) {
    }

    ~Transaction();

    Transaction(Transaction&& other);
    Transaction& operator=(Transaction&& other);

    const commitmanager::SnapshotDescriptor& operator*() const {
        return *operator->();
    }

    const commitmanager::SnapshotDescriptor* operator->() const {
        return mDescriptor.get();
    }

public:
    operator const commitmanager::SnapshotDescriptor&() const {
        return *mDescriptor;
    }

    const commitmanager::SnapshotDescriptor& descriptor() const {
        return *mDescriptor;
    }

    void commit();

    void abort();
};

} // namespace store
} // namespace tell
