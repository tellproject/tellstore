#include "Log.hpp"
#include "Epoch.hpp"
#include "Logging.hpp"

namespace tell {
namespace store {

std::pair<LogPage*, LogEntry*> LogEntry::nextP(LogPage* page) {
    auto off = offset();
    if (size == 0 && off == 0) {
        LOG_FATAL("Tried to iterate over last page");
        assert(false);
        return std::make_pair(nullptr, nullptr);
    }
    char* nPtr = data() + size;
    LogEntry* next = reinterpret_cast<LogEntry*>(nPtr);
    if (next->size != 0u) {
        return std::make_pair(page, next);
    }
    // pointer to page
    char* p = reinterpret_cast<char*>(this) - off;
    LogPage* nextPage = reinterpret_cast<std::atomic<LogPage*>*>(p)->load();
    if (nextPage) {
        return std::make_pair(nextPage, reinterpret_cast<LogEntry*>(nextPage->page + LogPage::DATA_OFFSET));
    } else {
        // we reached the end of the log
        return std::make_pair(page, next);
    }
}

LogEntry* LogEntry::next() {
    auto off = offset();
    if (size == 0 && off == 0) {
        LOG_FATAL("Tried to iterate over last page");
        assert(false);
        return nullptr;
    }
    char* nPtr = data() + size;
    LogEntry* next = reinterpret_cast<LogEntry*>(nPtr);
    if (next->size != 0u) {
        return next;
    }
    // pointer to page
    char* p = reinterpret_cast<char*>(this) - off;
    LogPage* nextPage = reinterpret_cast<std::atomic<LogPage*>*>(p)->load();
    if (nextPage) {
        return reinterpret_cast<LogEntry*>(nextPage->page + LogPage::DATA_OFFSET);
    } else {
        // we reached the end of the log
        return next;
    }
}

Log::Log(PageManager& pageManager)
        : mPageManager(pageManager),
          mHead(new (allocator::malloc(sizeof(LogPage))) LogPage(reinterpret_cast<char*>(mPageManager.alloc()))),
          mSealHead(mHead.load()->begin()),
          mTail(std::make_pair(mHead.load(), mHead.load()->begin()))
{
}

void Log::seal(LogEntry* entry) {
    entry->seal();
    auto head = mSealHead.load();
    while (head->sealed()) {
        auto next = head->next();
        mSealHead.compare_exchange_strong(head, next);
        head = mSealHead.load();
    }
}

LogEntry* Log::append(uint32_t size) {
    size += sizeof(LogEntry);
    size += 8 - (size % 8);
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
            if (head->offset().compare_exchange_strong(offset, std::numeric_limits<uint32_t>::max()))
                continue;
            // Create a new page
            auto nPage = new (allocator::malloc(sizeof(LogPage))) LogPage(reinterpret_cast<char*>(mPageManager.alloc()));
            nPage->offset().store(size);
            // now we try to install the new page
            if (head->next().compare_exchange_strong(nextPtr, nPage)) {
                // We don't check whether this succeeds - if it does not, it means another thread updated the head
                mHead.compare_exchange_strong(head, nPage);
                return new LogEntry(size, uint32_t(size - sizeof(LogEntry)));
            } else {
                // someone else already appended a new page - retry
                mPageManager.free(nPage->page);
                allocator::free_now(nPage);
                continue;
            }
        }
        if (head->offset().compare_exchange_strong(offset, offset)) {
            // append succeeded
            return new (head->page + offset) LogEntry(offset, uint32_t(size - sizeof(LogEntry)));
        }
    }
}

LogEntry* Log::tail() {
    return mTail.second;
}

void Log::setTail(LogEntry* nTail) {
    // the algorithm for setting the new tail works like this:
    // 1. start at the current tail
    // 2. iterate through the entries
    // 3. whenever we change the page, free the old page
    while (mTail.second != nTail) {
        auto n = mTail.second->nextP(mTail.first);
        if (n.first != mTail.first) {
            auto pageToFree = mTail.first;
            auto& pageManager = mPageManager;
            allocator::free(mTail.first, [pageToFree, &pageManager](){
                pageManager.free(pageToFree->page);
            });
        }
        mTail = n;
    }
}

} // namespace store
} // namespace tell
