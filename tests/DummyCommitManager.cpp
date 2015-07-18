#include "DummyCommitManager.hpp"

#include <stdexcept>

namespace tell {
namespace store {

Transaction DummyCommitManager::startTx() {
    Lock _(mMutex);
    if (!mManager.startTransaction()) {
        throw std::logic_error("Error starting transaction");
    }
    return {*this, mManager.createSnapshot()};
}

void DummyCommitManager::commitTx(uint64_t version) {
    Lock _(mMutex);
    if (!mManager.commitTransaction(version)) {
        throw std::logic_error("Error committing transaction");
    }
}

Transaction::~Transaction() {
    if (!mCommitted)
        mManager->abortTx(mDescriptor->version());
}

Transaction::Transaction(Transaction&& other)
        : mManager(other.mManager),
          mDescriptor(std::move(other.mDescriptor)),
          mCommitted(other.mCommitted) {
    other.mCommitted = true;
}

Transaction& Transaction::operator=(Transaction&& other) {
    // First we need to destroy ourself
    if (!mCommitted)
        mManager->abortTx(mDescriptor->version());
    mManager = other.mManager;
    mDescriptor = std::move(other.mDescriptor);
    mCommitted = other.mCommitted;
    other.mCommitted = true;
    return *this;
}

void Transaction::commit() {
    if (mCommitted)
        return;
    mManager->commitTx(mDescriptor->version());
    mCommitted = true;
}

void Transaction::abort() {
    if (mCommitted)
        return;
    mManager->abortTx(mDescriptor->version());
    mCommitted = true;
}

} // namespace store
} // namespace tell
