#include "DMRecord.hpp"
#include <util/LogOperations.hpp>
#include <util/chunk_allocator.hpp>

namespace tell {
namespace store {
namespace dmrewrite {

DMRecord::DMRecord(const Schema& schema)
    : record(schema) {
}

const char* DMRecord::getRecordData(const SnapshotDescriptor& snapshot,
                                    const char* data,
                                    bool& isNewest,
                                    std::atomic<LogEntry*>** next /*= nullptr*/) const {
    const char* res = multiVersionRecord.getRecord(snapshot, data + 8, isNewest);
    if (next) {
        *next = reinterpret_cast<std::atomic<LogEntry*>*>(const_cast<char*>(data));
    }
    if (isNewest) {
        // if the newest record is valid in the given snapshot, we need to check whether
        // there are newer versions on the log. Otherwise we are fine (even if there are
        // newer versions, we won't care).
        const LogEntry* loggedOperation = getNewest(data);
        while (loggedOperation) {
            // there are newer versions
            auto version = LoggedOperation::getVersion(loggedOperation->data());
            if (snapshot.inReadSet(version)) {
                return LoggedOperation::getRecord(loggedOperation->data());
            }
            // if we reach this code, we did not return the newest version
            isNewest = false;
            loggedOperation = LoggedOperation::getPrevious(loggedOperation->data());
        }
    }
    return res;
}

LogEntry* DMRecord::getNewest(const char* data) const {
    LogEntry* res = reinterpret_cast<std::atomic<LogEntry*>*>(const_cast<char*>(data))->load();
    unsigned long ptr = reinterpret_cast<unsigned long>(res);
    if (ptr % 2 != 0) {
        // the GC is running and set this already to the new pointer
        if (ptr % 4 != 0)
            ptr -= 4;
        char* newEntry = *reinterpret_cast<char**>(ptr + 1);
        res = reinterpret_cast<std::atomic<LogEntry*>*>(newEntry)->load();
    } else if (ptr % 4 != 0) {
        return nullptr;
    }
    return res;
}

bool DMRecord::setNewest(LogEntry* old, LogEntry* n, const char* data) {
    std::atomic<LogEntry*>* en =  reinterpret_cast<std::atomic<LogEntry*>*>(const_cast<char*>(data));
    LogEntry* logEntry = en->load();
    unsigned long ptr = reinterpret_cast<unsigned long>(logEntry);
    if (ptr % 2 != 0) {
        char* newEntry = *reinterpret_cast<char**>(ptr + 1);
        en = reinterpret_cast<std::atomic<LogEntry*>*>(newEntry);
        logEntry = en->load();
        ptr = reinterpret_cast<unsigned long>(logEntry);
    }
    if (ptr % 4 != 0) {
        if (old != nullptr) return false;
        return en->compare_exchange_strong(logEntry, n);
    }
    return en->compare_exchange_strong(old, n);
}

bool DMRecord::needGCWork(const char* data, uint64_t minVersion) const {
    return getNewest(data) != nullptr ||
        (MultiVersionRecord::getSmallestVersion(data + 8) < minVersion &&
            MultiVersionRecord::getNumberOfVersions(data) > 1);
}

using default_alloc = crossbow::copy_allocator<std::pair<uint64_t, char*>>;
template std::pair<size_t, char*> DMRecord::compactAndMerge(char* data,
                                                            uint64_t minVersion,
                                                            default_alloc& allocator) const;

} // namespace dmrewrite
} // namespace store
} // namespace tell
