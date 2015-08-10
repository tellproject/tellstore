#pragma once

#include <atomic>
#include <cassert>
#include <cstddef>
#include <vector>

namespace tell {
namespace store {

template<class T>
class FixedSizeStack {
private:
    struct alignas(8) Head {
        unsigned readHead = 0u;
        unsigned writeHead = 0u;

        Head() noexcept = default;

        Head(unsigned readHead, unsigned writeHead)
            : readHead(readHead),
              writeHead(writeHead)
        {}
    };
    static_assert(sizeof(T) <= 8, "Only CAS with less than 8 bytes supported");
    std::vector<T> mVec;
    std::atomic<Head> mHead;
public:
    FixedSizeStack() = delete;
    FixedSizeStack(size_t size, T nullValue)
        : mVec(size, nullValue)
    {
        mHead.store(Head(0u, 0u));
        assert(mHead.is_lock_free());
        assert(mVec.size() == size);
        assert(mHead.load().readHead == 0);
        assert(mHead.load().writeHead == 0);
    }
    FixedSizeStack(const FixedSizeStack&) = delete;
    FixedSizeStack(FixedSizeStack&&) = delete;
    FixedSizeStack& operator= (const FixedSizeStack&) = delete;
    FixedSizeStack& operator= (FixedSizeStack&&) = delete;

    /**
    * \returns true if pop succeeded - result will be set
    *          to the popped element on the stack
    */
    bool pop(T& result) {
        while (true) {
            auto head = mHead.load();
            if (head.writeHead != head.readHead) continue;
            if (head.readHead == 0) {
                return false;
            }
            result = mVec[head.readHead - 1];
            if (mHead.compare_exchange_strong(head, Head(head.readHead - 1, head.writeHead - 1)))
                return true;
        }
    }

    bool push(T element) {
        while (true) {
            auto head = mHead.load();
            auto wHead = head.writeHead;
            if (wHead == mVec.size()) return false;
            if (!mHead.compare_exchange_strong(head, Head(head.readHead, head.writeHead + 1)))
                continue;
            mVec[wHead] = element;
            // element has been inserted, now we need to make sure,
            // the readhead gets increased
            head = mHead.load();
            while (head.readHead <= wHead) {
                if (head.readHead == wHead) {
                    mHead.compare_exchange_strong(head, Head(wHead + 1, head.writeHead));
                }
                head = mHead.load();
            }
            return true;
        }
    }
};

} // namespace store
} // namespace tell

