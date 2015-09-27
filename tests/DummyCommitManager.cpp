/*
 * (C) Copyright 2015 ETH Zurich Systems Group (http://www.systems.ethz.ch/) and others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Contributors:
 *     Markus Pilman <mpilman@inf.ethz.ch>
 *     Simon Loesing <sloesing@inf.ethz.ch>
 *     Thomas Etter <etterth@gmail.com>
 *     Kevin Bocksrocker <kevin.bocksrocker@gmail.com>
 *     Lucas Braun <braunl@inf.ethz.ch>
 */
#include "DummyCommitManager.hpp"

#include <stdexcept>

namespace tell {
namespace store {

Transaction DummyCommitManager::startTx(bool readonly /* = false */) {
    Lock _(mMutex);
    if (!mManager.startTransaction(readonly)) {
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
