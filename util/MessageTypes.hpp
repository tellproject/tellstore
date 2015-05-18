#pragma once

#include <cstdint>

namespace tell {
namespace store {

/**
 * @brief The possible messages types of a request
 */
enum class RequestType : uint64_t {
    UNKOWN = 0x0u,
    CREATE_TABLE,
    GET_TABLEID,
    GET,
    GET_NEWEST,
    UPDATE,
    INSERT,
    REMOVE,
    REVERT,
    COMMIT,


    LAST = RequestType::COMMIT
};

/**
 * @brief The possible messages types of a response
 */
enum class ResponseType : uint64_t {
    UNKOWN = 0x0u,
    ERROR,
    CREATE_TABLE,
    GET_TABLEID,
    GET,
    MODIFICATION,
    COMMIT,


    LAST = RequestType::COMMIT
};

} // namespace store
} // namespace tell
