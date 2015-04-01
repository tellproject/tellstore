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

LogPage::LogPage(LogPage* previous)
        : mPrevious(previous),
          mOffset(0x1u) {
}

LogEntry* LogPage::append(uint32_t size) {
    auto entrySize = calculateEntrySize(size);
    if (entrySize > Log::MAX_SIZE) {
        LOG_ASSERT(false, "Tried to append %d bytes but %d bytes is max", entrySize, Log::MAX_SIZE);
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
        if (position + size > Log::MAX_SIZE) {
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

Log::Log(PageManager& pageManager)
        : mPageManager(pageManager),
          mHead(new (mPageManager.alloc()) LogPage(nullptr)) {
}

Log::~Log() {
    // TODO Free all pages from PageManager epoch
}

LogEntry* Log::append(uint32_t size) {
    auto entrySize = calculateEntrySize(size);
    if (entrySize > MAX_SIZE) {
        LOG_ASSERT(false, "Tried to append %d bytes but %d bytes is max", entrySize, MAX_SIZE);
        return nullptr;
    }

    auto head = mHead.load();
    while (true) {
        // Try to append a new log entry to the head page
        auto entry = head->appendEntry(entrySize);
        if (entry != nullptr) {
            return entry;
        }

        // Not enough space left in page - acquire new page
        auto nPage = new(mPageManager.alloc()) LogPage(head);
        if (!nPage) {
            LOG_ASSERT(false, "PageManager ran out of space");
            return nullptr;
        }

        // Try to set the page as new head
        // If this fails then another thread already set a new page and head will point to it
        if (mHead.compare_exchange_strong(head, nPage)) {
            head = nPage;
        } else {
            mPageManager.free(nPage);
        }
    }
}

} // namespace store
} // namespace tell
