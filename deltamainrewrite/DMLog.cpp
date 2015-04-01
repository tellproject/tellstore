#include "DMLog.hpp"

#include <util/Epoch.hpp>
#include <util/Logging.hpp>

namespace tell {
namespace store {
namespace dmrewrite {

static_assert(ATOMIC_POINTER_LOCK_FREE, "atomic pointer operations not supported");
static_assert(sizeof(DMLogPage*) == sizeof(std::atomic<DMLogPage*>), "atomics won't work correctly");

std::pair<DMLogPage*, DMLogEntry*> DMLogEntry::nextP(DMLogPage* page) {
    auto off = offset();
    if (size == 0 && off == 0) {
        LOG_FATAL("Tried to iterate over last page");
        assert(false);
        return std::make_pair(nullptr, nullptr);
    }
    char* nPtr = data() + size;
    DMLogEntry* next = reinterpret_cast<DMLogEntry*>(nPtr);
    if (next->size != 0u) {
        return std::make_pair(page, next);
    }
    // pointer to page
    char* p = reinterpret_cast<char*>(this) - off;
    DMLogPage* nextPage = reinterpret_cast<std::atomic<DMLogPage*>&>(p).load();
    if (nextPage) {
        return std::make_pair(nextPage, reinterpret_cast<DMLogEntry*>(nextPage->page + DMLogPage::DATA_OFFSET));
    } else {
        // we reached the end of the log
        return std::make_pair(page, next);
    }
}

bool DMLogEntry::last() const {
    return size == 0 && offset() == 0;
}

DMLogEntry* DMLogEntry::next() {
    auto off = offset();
    if (size == 0 && off == 0) {
        LOG_FATAL("Tried to iterate over last page");
        assert(false);
        return nullptr;
    }
    char* nPtr = data() + size + sizeof(DMLogEntry);
    DMLogEntry* next = reinterpret_cast<DMLogEntry*>(nPtr);
    if (next->size != 0u) {
        return next;
    }
    // pointer to page
    char* p = reinterpret_cast<char*>(this) - off - DMLogPage::LOG_HEADER_SIZE;
    DMLogPage* nextPage = reinterpret_cast<std::atomic<DMLogPage*>*>(p)->load();
    if (nextPage) {
        return reinterpret_cast<DMLogEntry*>(nextPage->page + DMLogPage::DATA_OFFSET);
    } else {
        // we reached the end of the log
        return next;
    }
}

DMLog::DMLog(PageManager& pageManager)
    : mPageManager(pageManager), mHead(
    new(allocator::malloc(sizeof(DMLogPage))) DMLogPage(reinterpret_cast<char*>(mPageManager.alloc()))), mSealHead(
    mHead.load()->begin()), mTail(std::make_pair(mHead.load(), mHead.load()->begin())) {
}

void DMLog::seal(DMLogEntry* entry) {
    entry->seal();
    auto head = mSealHead.load();
    while (!head->last() && head->sealed()) {
        auto next = head->next();
        mSealHead.compare_exchange_strong(head, next);
        head = mSealHead.load();
    }
}

DMLogEntry* DMLog::append(uint32_t size) {
    size += sizeof(DMLogEntry);
    size += size % 8 ? 8 - (size % 8) : 0;
    assert(size % 8 == 0);
    if (size > MAX_SIZE) {
        LOG_FATAL("Tried to append %d bytes but %d bytes is max", size, MAX_SIZE);
        assert(false);
        return nullptr;
    }
    while (true) {
        auto head = mHead.load();
        auto offset = head->offset().load();
        if (offset + size > MAX_SIZE) {
            // if the head already has a next pointer, we try to update the head
            auto nextPtr = head->next().load();
            if (nextPtr) {
                mHead.compare_exchange_strong(head, nextPtr);
                continue;
            }
            // we first need to make sure, that no one else will still append to this
            // page.
            if (!head->offset().compare_exchange_strong(offset, MAX_SIZE))
                continue;
            // Create a new page
            auto nPage = new(allocator::malloc(sizeof(DMLogPage))) DMLogPage(reinterpret_cast<char*>(mPageManager.alloc()));
            nPage->offset().store(size);
            // now we try to install the new page
            if (head->next().compare_exchange_strong(nextPtr, nPage)) {
                // We don't check whether this succeeds - if it does not, it means another thread updated the head
                mHead.compare_exchange_strong(head, nPage);
                return new DMLogEntry(size, uint32_t(size - sizeof(DMLogEntry)));
            } else {
                // someone else already appended a new page - retry
                mPageManager.free(nPage->page);
                allocator::free_now(nPage);
                continue;
            }
        }
        if (head->offset().compare_exchange_strong(offset, offset ? offset + size : offset + size + DMLogPage::LOG_HEADER_SIZE)) {
            // append succeeded
            offset += offset ? offset + DMLogPage::LOG_HEADER_SIZE : 0;
            return new(head->page + offset) DMLogEntry(offset, uint32_t(size - sizeof(DMLogEntry)));
        }
    }
}

DMLogEntry* DMLog::tail() {
    return mTail.second;
}

void DMLog::setTail(DMLogEntry* nTail) {
    // the algorithm for setting the new tail works like this:
    // 1. start at the current tail
    // 2. iterate through the entries
    // 3. whenever we change the page, free the old page
    while (mTail.second != nTail) {
        auto n = mTail.second->nextP(mTail.first);
        if (n.first != mTail.first) {
            auto pageToFree = mTail.first;
            auto& pageManager = mPageManager;
            allocator::free(mTail.first, [pageToFree, &pageManager]() {
                pageManager.free(pageToFree->page);
            });
        }
        mTail = n;
    }
}

} // namespace dmrewrite
} // namespace store
} // namespace tell
