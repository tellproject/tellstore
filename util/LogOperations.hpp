#pragma once

#include <cstdint>
#include <cstddef>
#include <type_traits>

namespace tell {
namespace store {
namespace dmrewrite {

struct DMLogEntry;

enum class LogOperation
    : uint8_t {
    INVALID = 0,
    INSERT,
    DELETE,
    UPDATE
};

/**
* The format of a logged operation is:
* - 1 byte for the log operation
* - 3 bytes padding
* - 8 bytes for the key, on which the operation is on
* - 8 bytes for the version of the transaction that wrote
*   the operation to the log
* - 8 byte pointer to old entry
* If applicable (Insert)
* - 8 byte for a pointer to an update entry (this
*   will be the only non-const entry in a log).
* If applicable (Update or Insert operations):
* - the tuple
*/
struct LoggedOperation {
    using LogOperation_t = std::underlying_type<LogOperation>::type;
    LogOperation operation;
    uint64_t key;
    uint64_t version;
    DMLogEntry* previous = nullptr;
    DMLogEntry* next = nullptr;
    const char* tuple;

    char* serialize(char* destination) const;

    size_t serializedSize() const;

    static LogOperation getType(const char* data);

    static uint64_t getKey(const char* data);

    static uint64_t getVersion(const char* data);

    static const char* getRecord(const char* data);

    static const DMLogEntry* getPrevious(const char* data);

    /**
    * This should only be called if the log entry is an
    * insert
    */
    static const char* getNewest(const char* data);

    static const DMLogEntry* loggedOperationFromTuple(const char* tuple);
};

} // namespace dmrewrite
} // namespace store
} // namespace tell
