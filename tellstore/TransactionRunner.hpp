#pragma once

#include <tellstore/ClientManager.hpp>

#include <crossbow/non_copyable.hpp>

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <queue>

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
    void operator()(Args&&... args) {
        try {
            mFun(std::forward<Args>(args)...);
        } catch (...) {
            mRunner.handleException(std::current_exception());
        }
        mRunner.completeTransaction();
    }

private:
    Runner& mRunner;
    Fun mFun;
};

class SingleTransactionRunner {
public:
    SingleTransactionRunner()
            : mDone(false) {
    }

    void wait();

private:
    template<typename Runner, typename Fun>
    friend class TransactionWrapper;

    void handleException(std::exception_ptr exception) {
        mException = std::move(exception);
    }

    void completeTransaction() {
        mDone.store(true);
        mWaitCondition.notify_all();
    }

    std::atomic<bool> mDone;

    std::exception_ptr mException;

    std::condition_variable mWaitCondition;
};

/**
 * @brief Helper class to run and wait for transactions from outside processing threads
 */
template<typename Context>
class TransactionRunner : crossbow::non_copyable, crossbow::non_movable {
public:
    TransactionRunner(ClientManager<Context>& client)
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

    template<typename Fun>
    void executeBlocking(Fun fun);

    template<typename Fun>
    void executeBlocking(size_t num, Fun fun);

    void wait();

private:
    template<typename Runner, typename Fun>
    friend class TransactionWrapper;

    void handleException(std::exception_ptr exception) {
        std::unique_lock<decltype(mExceptionMutex)> exceptionLock(mExceptionMutex);
        mException.emplace(exception);
    }

    void completeTransaction() {
        if (--mActive == 0u) {
            mWaitCondition.notify_all();
        }
    }

    ClientManager<Context>& mClient;

    std::atomic<size_t> mActive;

    std::mutex mExceptionMutex;
    std::queue<std::exception_ptr> mException;

    std::condition_variable mWaitCondition;
};

template <typename Context>
template <typename Fun>
void TransactionRunner<Context>::execute(Fun fun) {
    ++mActive;
    mClient.execute(TransactionWrapper<TransactionRunner<Context>, Fun>(*this, std::move(fun)));
}

template <typename Context>
template <typename Fun>
void TransactionRunner<Context>::execute(size_t num, Fun fun) {
    ++mActive;
    mClient.execute(num, TransactionWrapper<TransactionRunner<Context>, Fun>(*this, std::move(fun)));
}

template <typename Context>
template <typename Fun>
void TransactionRunner<Context>::executeBlocking(Fun fun) {
    SingleTransactionRunner runner;
    mClient.execute(TransactionWrapper<SingleTransactionRunner, Fun>(runner, std::move(fun)));
    runner.wait();
}

template <typename Context>
template <typename Fun>
void TransactionRunner<Context>::executeBlocking(size_t num, Fun fun) {
    SingleTransactionRunner runner;
    mClient.execute(num, TransactionWrapper<SingleTransactionRunner, Fun>(runner, std::move(fun)));
    runner.wait();
}

template <typename Context>
void TransactionRunner<Context>::wait() {
    std::mutex waitMutex;
    std::unique_lock<decltype(waitMutex)> waitLock(waitMutex);
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

} // namespace store
} // namespace tell
