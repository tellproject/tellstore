#pragma once

#include "PageManager.hpp"
#include <cstddef>
#include <atomic>

namespace tell {
namespace store {
namespace impl {

struct LogEntry {
    uint32_t size;
    bool sealed = false;
    char* data;
    const LogEntry* next() const;
    LogEntry(uint32_t sz) : size(sz) {
        data = reinterpret_cast<char*>(this) + sizeof(LogEntry);
    }
    LogEntry(const LogEntry&) = delete;
    LogEntry(LogEntry&&) = delete;
    LogEntry& operator= (const LogEntry&) = delete;
    LogEntry& operator= (LogEntry&&) = delete;
};


struct LogPage {
    std::atomic<size_t> offset;
    std::atomic<LogPage*> next;
    char* page;
};

/**
* A generic log implementation which can be used
* by other components of the system.
*/
class Log {
    PageManager& mPageManager;
    std::atomic<LogPage*> mCurrent;
public:
    Log(PageManager& pageManager);
    LogEntry* append(uint32_t size);
    void seal(LogEntry& entry);
    const LogEntry* tail() const ;
    const LogEntry* head() const;
};

} // namespace tell
} // namespace store
} // namespace impl
