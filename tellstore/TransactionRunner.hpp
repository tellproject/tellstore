#pragma once

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

class ClientHandle;

/**
 * @brief Helper class to run and wait for transactions from outside processing threads
 */
class TransactionRunner : crossbow::non_copyable, crossbow::non_movable {
public:
    TransactionRunner()
            : mActive(0u) {
    }

    size_t active() const {
        return mActive.load();
    }

    bool running() const {
        return (mActive.load() != 0);
    }

    template<typename Fun>
    std::function<void(ClientHandle&)> wrap(Fun fun);

    void wait();

private:
    std::atomic<size_t> mActive;

    std::mutex mExceptionMutex;
    std::queue<std::exception_ptr> mException;

    std::condition_variable mWaitCondition;
};

template<typename Fun>
std::function<void(ClientHandle&)> TransactionRunner::wrap(Fun fun) {
    ++mActive;
    auto wrapper = [this, fun] (ClientHandle& handle) mutable {
        try {
            fun(handle);
        } catch (...) {
            std::unique_lock<decltype(mExceptionMutex)> exceptionLock(mExceptionMutex);
            mException.emplace(std::current_exception());
        }

        if (--mActive == 0u) {
            mWaitCondition.notify_all();
        }
    };
    return wrapper;
}

} // namespace store
} // namespace tell
