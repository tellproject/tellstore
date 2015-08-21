#include <tellstore/TransactionRunner.hpp>

namespace tell {
namespace store {

void TransactionRunner::wait() {
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
