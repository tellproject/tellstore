#pragma once

#include <atomic>
#include <vector>

namespace tell {
namespace store {

template<class T>
class FixedSizeStack {
private:
    static_assert(sizeof(T) <= 8, "Only CAS with less than 8 bytes supported");
    std::vector<T> mVec;
    std::atomic<size_t> mHead;
    std::atomic<size_t> mWriteHead;
public:
    FixedSizeStack(size_t size)
            : mVec(size, nullptr),
              mHead(mVec.size()),
              mWriteHead(0)
    {}

    /**
    * \returns true if pop succeeded - result will be set
    *          to the popped element on the stack
    */
    bool pop(T& result) {
        while (true) {
            auto head = mHead.load();
            if (head == mVec.size()) {
                result = nullptr;
                return false;
            }
            result = mVec[head];
            if (mHead.compare_exchange_strong(head, head + 1))
                return true;
        }
    }

    bool push(T element) {
        while (true) {
            auto wHead = mWriteHead.load();
            if (wHead == 0) return false;
            if (!mWriteHead.compare_exchange_strong(wHead, wHead - 1))
                continue;
            mVec[--mWriteHead] = element;
            // element has been inserted, now we need to make sure,
            // the head gets forwarded to mWriteHead
            auto head = mHead.load();
            while (head < wHead) {
                if (head + 1 == wHead)
                    ++mHead;
                head = mHead.load();
            }
        }
    }

    bool empty() const {
        return mHead == mVec.size();
    }
};

} // namespace store
} // namespace tell

