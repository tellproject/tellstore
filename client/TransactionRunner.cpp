#include <tellstore/TransactionRunner.hpp>

namespace tell {
namespace store {

void SingleTransactionRunner::wait() {
    std::mutex waitMutex;
    std::unique_lock<decltype(waitMutex)> waitLock(waitMutex);
    mWaitCondition.wait(waitLock, [this] () {
        return mDone.load();
    });
    waitLock.unlock();

    if (mException) {
        std::rethrow_exception(mException);
    }
}

} // namespace store
} // namespace tell
