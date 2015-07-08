#include "LogOperations.hpp"

#include <util/Record.hpp>

#include "Log.hpp"

#include <crossbow/enum_underlying.hpp>
#include <crossbow/logger.hpp>

namespace tell {
namespace store {
namespace dmrewrite {

char* LoggedOperation::serialize(char* destination) const {
    auto op = crossbow::to_underlying(operation);
    memcpy(destination, &op, sizeof(op));
    destination += sizeof(op);
    destination += 3;
    memcpy(destination, &key, sizeof(key));
    destination += sizeof(key);
    memcpy(destination, &version, sizeof(version));
    destination += sizeof(version);
    switch (operation) {
        case LogOperation::INSERT:
            // a null pointer with updates
            LOG_ASSERT(previous == nullptr, "Previous for new insert must always be null");
            memcpy(destination, &previous, sizeof(previous));
            destination += sizeof(previous);
            break;
        case LogOperation::UPDATE:
        case LogOperation::DELETE:
            memcpy(destination, &previous, sizeof(previous));
            destination += sizeof(previous);
            break;
        default: {
        }
    }
    auto tupleSize = *reinterpret_cast<const uint32_t*>(tuple);
    memcpy(destination, &tupleSize, sizeof(tupleSize));
    destination += sizeof(tupleSize);
    memcpy(destination, tuple, tupleSize);
    return destination + tupleSize;
}

size_t LoggedOperation::serializedSize() const {
    auto tupleSize = *reinterpret_cast<const uint32_t*>(tuple);
    tupleSize += sizeof(crossbow::to_underlying(operation));
    tupleSize += sizeof(key);
    tupleSize += sizeof(version);
    tupleSize += sizeof(previous);
    tupleSize += sizeof(tupleSize);
    return tupleSize;
}

uint64_t LoggedOperation::getVersion(const char* data) {
    return *reinterpret_cast<const uint64_t*>(data + 12);
}

const char* LoggedOperation::getRecord(const char* data) {
    auto underlyingOp = *reinterpret_cast<const typename std::underlying_type<LogOperation>::type*>(data);
    LogOperation op = crossbow::from_underlying<LogOperation>(underlyingOp);
    data += 20;
    switch (op) {
        case LogOperation::DELETE:
        case LogOperation::UPDATE:
            data += 8;
        default: {
        }
    }
    return data;
}

const DMLogEntry* LoggedOperation::getPrevious(const char* data) {
    const DMLogEntry* res;
    memcpy(&res, data + 20, sizeof(res));
    return res;
}

const char* LoggedOperation::getNewest(const char* data) {
    assert(getType(data) == LogOperation::INSERT);
    return data + 20;
}

uint64_t LoggedOperation::getKey(const char* data) {
    assert(crossbow::from_underlying<LogOperation>(data[0]) != LogOperation::INVALID);
    return *reinterpret_cast<const uint64_t*>(data + 4);
}

const DMLogEntry* LoggedOperation::loggedOperationFromTuple(char const* tuple) {
    return reinterpret_cast<const DMLogEntry*>(tuple - 28);
}

LogOperation LoggedOperation::getType(const char* data) {
    auto res = *reinterpret_cast<const LogOperation_t*>(data);
    return crossbow::from_underlying<LogOperation>(res);
}

} // namespace dmrewrite
} // namespace store
} // namespace tell
