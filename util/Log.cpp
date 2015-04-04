#include "Log.hpp"

#include "Epoch.hpp"

namespace tell {
namespace store {

static_assert(ATOMIC_POINTER_LOCK_FREE, "Atomic pointer operations not supported");
static_assert(sizeof(LogPage*) == sizeof(std::atomic<LogPage*>), "Atomics won't work correctly");
static_assert(sizeof(LogPage) <= LogPage::LOG_HEADER_SIZE, "LOG_HEADER_SIZE must be larger or equal than LogPage");

namespace {

/**
 * @brief Calculates the required size of a LogEntry
 *
 * Adds the LogEntry class size and adds padding so the size is 8 byte aligned
 */
uint32_t calculateEntrySize(uint32_t size) {
    size += sizeof(LogEntry);
    size += ((size % 8 != 0) ? (8 - (size % 8)) : 0);
    LOG_ASSERT(size % 8 == 0, "Final LogEntry size must be 8 byte aligned");
    return size;
}

} // anonymous namespace

uint32_t LogEntry::tryAcquire(uint32_t size) {
    LOG_ASSERT(size != 0x0u, "Size has to be greater than zero");
    LOG_ASSERT((size & 0x1u) == 0, "LSB has to be zero");

    uint32_t exp = 0x0u;
    mSize.compare_exchange_strong(exp, (size | 0x1u));
    return (exp & (0xFFFFFFFFu << 1));
}

LogEntry* LogPage::append(uint32_t size) {
    auto entrySize = calculateEntrySize(size);
    if (entrySize > LogPage::MAX_ENTRY_SIZE) {
        LOG_ASSERT(false, "Tried to append %d bytes but %d bytes is max", entrySize, LogPage::MAX_ENTRY_SIZE);
        return nullptr;
    }
    return appendEntry(entrySize);
}

LogEntry* LogPage::appendEntry(uint32_t size) {
    auto offset = mOffset.load();

    // Check if page is already sealed
    if ((offset & 0x1u) == 0) {
        return nullptr;
    }
    auto position = offset & (0xFFFFFFFFu << 1);

    while (true) {
        // Check if we have enough space in the log page
        if (position + size > LogPage::MAX_ENTRY_SIZE) {
            return nullptr;
        }

        // Try to acquire the space for the new entry
        auto entry = reinterpret_cast<LogEntry*>(this->data() + position);
        auto res = entry->tryAcquire(size);
        if (res != 0) {
            position += res;
            continue;
        }

        // Try to set the new offset until we succeed or another thread set a higher offset
        auto nOffset = ((position + size) | 0x1u);
        while (offset < nOffset) {
            // Set new offset, if this fails offset will contain the new offset value
            if (mOffset.compare_exchange_strong(offset, nOffset)) {
                break;
            }
            // Check if page was sealed before we completely acquired the space for the log entry
            if ((offset & 0x1u) == 0) {
                return nullptr;
            }
        }

        return entry;
    }
}

UnorderedLogImpl::UnorderedLogImpl(PageManager& pageManager)
        : mPageManager(pageManager),
          mHead(new (mPageManager.alloc()) LogPage()) {
}

LogPage* UnorderedLogImpl::createPage(LogPage* oldHead) {
    auto nPage = new(mPageManager.alloc()) LogPage();
    if (!nPage) {
        LOG_ASSERT(false, "PageManager ran out of space");
        return nullptr;
    }
    nPage->next().store(oldHead);

    // Try to set the page as new head
    // If this fails then another thread already set a new page and oldHead will point to it
    if (!mHead.compare_exchange_strong(oldHead, nPage)) {
        mPageManager.free(nPage);
        return oldHead;
    }

    return nPage;
}

OrderedLogImpl::OrderedLogImpl(PageManager& pageManager)
        : mPageManager(pageManager),
          mHead(new (mPageManager.alloc()) LogPage()),
          mTail(mHead.load()) {
    LOG_ASSERT(mHead.load() == mTail.load(), "Head and Tail do not point to the same page");
}

LogPage* OrderedLogImpl::createPage(LogPage* oldHead) {
    // Check if the old head already has a next pointer
    auto next = oldHead->next().load();
    if (next) {
        // Try to set the next page as new head
        // If this fails then another thread already set a new head and oldHead will point to it
        if (!mHead.compare_exchange_strong(oldHead, next)) {
            return oldHead;
        }
        return next;
    }

    // Not enough space left in page - acquire new page
    auto nPage = new(mPageManager.alloc()) LogPage();
    if (!nPage) {
        LOG_ASSERT(false, "PageManager ran out of space");
        return nullptr;
    }

    // Try to set the new page as next page on the old head
    // If this fails then another thread already set a new page and next will point to it
    if (!oldHead->next().compare_exchange_strong(next, nPage)) {
        mPageManager.free(nPage);
        return next;
    }

    // Set the page as new head
    // We do not care if this succeeds - if it does not, it means another thread updated the head for us
    mHead.compare_exchange_strong(oldHead, nPage);

    return nPage;
}

bool OrderedLogImpl::truncateLog(LogPage* oldTail, LogPage* newTail) {
    if (oldTail == newTail) {
        return (mTail.load() == newTail);
    }

    if (!mTail.compare_exchange_strong(oldTail, newTail)) {
        return false;
    }

    auto ptr = allocator::malloc(0);

    auto& pageManager = mPageManager;
    allocator::free(ptr, [oldTail, newTail, &pageManager]() {
        auto page = oldTail;
        while (page != newTail) {
            auto next = page->next().load();
            pageManager.free(page);
            page = next;
        }
    });
    return true;
}

template <class Impl>
Log<Impl>::~Log() {
    // Safe Memory Reclamation mechanism has to ensure that the Log class is only deleted when no one references it
    // anymore so we can safely delete all pages here
    auto page = Impl::startPage();
    while (page) {
        auto next = page->next().load();
        Impl::freePage(page);
        page = next;
    }
}

template <class Impl>
LogEntry* Log<Impl>::append(uint32_t size) {
    auto entrySize = calculateEntrySize(size);
    if (entrySize > LogPage::MAX_ENTRY_SIZE) {
        LOG_ASSERT(false, "Tried to append %d bytes but %d bytes is max", entrySize, LogPage::MAX_ENTRY_SIZE);
        return nullptr;
    }

    auto page = Impl::head();
    while (true) {
        // Try to append a new log entry to the page
        auto entry = page->appendEntry(entrySize);
        if (entry != nullptr) {
            return entry;
        }

        // The page must be full, acquire a new one
        page = Impl::createPage(page);
    }
}

template class Log<UnorderedLogImpl>;
template class Log<OrderedLogImpl>;

} // namespace store
} // namespace tell
