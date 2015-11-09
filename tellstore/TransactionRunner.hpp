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
#pragma once

#include <tellstore/ClientManager.hpp>

#include <crossbow/infinio/Fiber.hpp>
#include <crossbow/non_copyable.hpp>

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <queue>
#include <stdexcept>

namespace tell {
namespace store {

template <typename Runner, typename Fun>
class TransactionWrapper {
public:
    TransactionWrapper(Runner& runner, Fun fun)
            : mRunner(runner),
              mFun(std::move(fun)) {
    }

    template <typename... Args>
    void operator()(ClientHandle& handle, Args&&... args) {
        mRunner.startTransaction(handle);
        try {
            mFun(handle, std::forward<Args>(args)...);
        } catch (...) {
            mRunner.handleException(std::current_exception());
        }
        mRunner.completeTransaction();
    }

private:
    Runner& mRunner;
    Fun mFun;
};

/**
 * @brief Helper class to run and wait for one single transaction from outside the processing thread
 */
template<typename Context>
class SingleTransactionRunner : crossbow::non_copyable, crossbow::non_movable {
public:
    SingleTransactionRunner(ClientManager<Context>& client)
            : mClient(client),
              mState(TransactionState::DONE),
              mFiber(nullptr) {
    }

    /**
     * @brief Execute the transaction in the runner
     *
     * No other transaction must be active at the same time.
     */
    template <typename Fun>
    void execute(Fun fun);

    /**
     * @brief Execute the transaction in the runner on the specific processing thread
     *
     * No other transaction must be active at the same time.
     */
    template <typename Fun>
    void execute(size_t num, Fun fun);

    /**
     * @brief Wait until the transaction completes or blocks
     *
     * Must only be called from outside the transaction thread.
     *
     * @return True if the transaction completed, false if it blocked
     */
    bool wait();

    /**
     * @brief Blocks the transaction (and notifies the issuing thread)
     *
     * Must only be called from inside the transaction.
     */
    void block() {
        {
            std::unique_lock<decltype(mWaitMutex)> waitLock(mWaitMutex);
            mState.store(TransactionState::BLOCKED);
            mWaitCondition.notify_one();
        }
        mFiber->wait();
    }

    /**
     * @brief Unblocks the blocked transaction
     *
     * Must only be called from outside the transaction.
     *
     * @return Whether the transaction was unblocked
     */
    bool unblock();

private:
    enum class TransactionState {
        RUNNING,
        BLOCKED,
        DONE
    };

    template<typename Runner, typename Fun>
    friend class TransactionWrapper;

    void startTransaction(ClientHandle& handle) {
        mFiber = &handle.fiber();
    }

    void handleException(std::exception_ptr exception) {
        mException = std::move(exception);
    }

    void completeTransaction() {
        std::unique_lock<decltype(mWaitMutex)> waitLock(mWaitMutex);
        mFiber = nullptr;
        mState.store(TransactionState::DONE);
        mWaitCondition.notify_one();
    }

    ClientManager<Context>& mClient;

    std::atomic<TransactionState> mState;

    crossbow::infinio::Fiber* mFiber;

    std::exception_ptr mException;

    std::mutex mWaitMutex;
    std::condition_variable mWaitCondition;
};

template <typename Context>
template <typename Fun>
void SingleTransactionRunner<Context>::execute(Fun fun) {
    auto expected = TransactionState::DONE;
    if (!mState.compare_exchange_strong(expected, TransactionState::RUNNING)) {
        throw std::runtime_error("Another transaction is already running");
    }

    mClient.execute(TransactionWrapper<SingleTransactionRunner<Context>, Fun>(*this, std::move(fun)));
}

template <typename Context>
template <typename Fun>
void SingleTransactionRunner<Context>::execute(size_t num, Fun fun) {
    auto expected = TransactionState::DONE;
    if (!mState.compare_exchange_strong(expected, TransactionState::RUNNING)) {
        throw std::runtime_error("Another transaction is already running");
    }

    mClient.execute(num, TransactionWrapper<SingleTransactionRunner<Context>, Fun>(*this, std::move(fun)));
}

template <typename Context>
bool SingleTransactionRunner<Context>::wait() {
    std::unique_lock<decltype(mWaitMutex)> waitLock(mWaitMutex);
    mWaitCondition.wait(waitLock, [this] () {
        return (mState != TransactionState::RUNNING);
    });

    if (mException) {
        std::rethrow_exception(mException);
    }
    return (mState == TransactionState::DONE);
}

template <typename Context>
bool SingleTransactionRunner<Context>::unblock() {
    auto expected = TransactionState::BLOCKED;
    if (!mState.compare_exchange_strong(expected, TransactionState::RUNNING)) {
        return false;
    }
    mFiber->unblock();
    return true;
}

/**
 * @brief Helper class to run and wait for transactions from outside processing threads
 */
template<typename Context>
class MultiTransactionRunner : crossbow::non_copyable, crossbow::non_movable {
public:
    MultiTransactionRunner(ClientManager<Context>& client)
            : mClient(client),
              mActive(0u) {
    }

    size_t active() const {
        return mActive.load();
    }

    bool running() const {
        return (mActive.load() != 0);
    }

    template<typename Fun>
    void execute(Fun fun);

    template<typename Fun>
    void execute(size_t num, Fun fun);

    void wait();

private:
    template<typename Runner, typename Fun>
    friend class TransactionWrapper;

    void startTransaction(ClientHandle& /* handle */) {
    }

    void handleException(std::exception_ptr exception) {
        std::unique_lock<decltype(mExceptionMutex)> exceptionLock(mExceptionMutex);
        mException.emplace(exception);
    }

    void completeTransaction() {
        if (--mActive == 0u) {
            std::unique_lock<decltype(mWaitMutex)> waitLock(mWaitMutex);
            mWaitCondition.notify_one();
        }
    }

    ClientManager<Context>& mClient;

    std::atomic<size_t> mActive;

    std::mutex mExceptionMutex;
    std::queue<std::exception_ptr> mException;

    std::mutex mWaitMutex;
    std::condition_variable mWaitCondition;
};

template <typename Context>
template <typename Fun>
void MultiTransactionRunner<Context>::execute(Fun fun) {
    ++mActive;
    mClient.execute(TransactionWrapper<MultiTransactionRunner<Context>, Fun>(*this, std::move(fun)));
}

template <typename Context>
template <typename Fun>
void MultiTransactionRunner<Context>::execute(size_t num, Fun fun) {
    ++mActive;
    mClient.execute(num, TransactionWrapper<MultiTransactionRunner<Context>, Fun>(*this, std::move(fun)));
}

template <typename Context>
void MultiTransactionRunner<Context>::wait() {
    std::unique_lock<decltype(mWaitMutex)> waitLock(mWaitMutex);
    mWaitCondition.wait(waitLock, [this] () {
        return (mActive == 0u);
    });
    waitLock.unlock();

    std::unique_lock<decltype(mExceptionMutex)> exceptionLock(mExceptionMutex);
    if (!mException.empty()) {
        auto ptr = mException.front();
        mException.pop();
        std::rethrow_exception(ptr);
    }
}

namespace TransactionRunner {

/**
 * @brief Execute the transaction and block until it completes
 */
template <typename Context, typename Fun>
void executeBlocking(ClientManager<Context>& client, Fun fun) {
    SingleTransactionRunner<Context> runner(client);
    runner.execute(std::move(fun));
    while (!runner.wait()) {
        runner.unblock();
    }
}

/**
 * @brief Execute the transaction and block until it completes on the specific processing thread
 */
template <typename Context, typename Fun>
void executeBlocking(ClientManager<Context>& client, size_t num, Fun fun) {
    SingleTransactionRunner<Context> runner(client);
    runner.execute(num, std::move(fun));
    while (!runner.wait()) {
        runner.unblock();
    }
}

} // namespace TransactionRunner
} // namespace store
} // namespace tell
