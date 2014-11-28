#include <tclDecls.h>
#include "Log.hpp"
#include "Epoch.hpp"
#include "Logging.hpp"

namespace tell {
namespace store {
namespace impl {

const LogEntry* LogEntry::next() const {
    const LogEntry* n = reinterpret_cast<const LogEntry*>(data + size);
    if (n->size == 0) {
        // we reached the end of the log
        std::atomic<const LogEntry*>& atomic = *reinterpret_cast<std::atomic<const LogEntry*>*>(data + size + 8);
        return atomic.load();
    }
    return n;
}

Log::Log(PageManager& pageManager)
        : mPageManager(pageManager),
          mCurrent(new (allocator::malloc(sizeof(LogPage))) LogPage())
{
    mCurrent.load()->next = nullptr;
    mCurrent.load()->offset = 0;
    mCurrent.load()->page = reinterpret_cast<char*>(mPageManager.alloc());
}

LogEntry* Log::append(uint32_t size) {
    // align to 8 bytes
    uint32_t fullSize = uint32_t(size + sizeof(LogEntry));
    uint32_t padding = 8 - (fullSize % 8);
    fullSize += padding;
    size += padding;
    if (fullSize > PAGE_SIZE) {
        assert(false);
        LOG_FATAL("Could not append %d bytes to the log - max size is %d", size - padding, PAGE_SIZE - sizeof(LogEntry));
        return nullptr;
    }
    while (true) {
        LogPage* page = mCurrent.load();
        if (page->next != nullptr) {
            auto next = page->next;
            mCurrent.compare_exchange_strong(page, next);
            continue;
        }
        auto offset = page->offset.load();
        if (offset + size > PAGE_SIZE) {
            // TODO: Append new page here
            // We first try to set the offset to infinite, to make
            // sure that no one else will write into the page anymore
            if (!page->offset.compare_exchange_strong(offset, std::numeric_limits<uint32_t>::max()))
                continue;
            auto nPage = new (allocator::malloc(sizeof(LogPage))) LogPage();
            nPage->offset =
        }
        if (page->offset.compare_exchange_strong(offset, offset + fullSize)) {
            // CAS succeeded
            auto res = new (page->page + offset) LogEntry(size);
            return res;
        }
    }
}

void Log::seal(LogEntry& entry) {
}

const LogEntry* Log::tail() const {
    return nullptr;
}

const LogEntry* Log::head() const {
    return nullptr;
}
} // namespace tell
} // namespace store
} // namespace impl
