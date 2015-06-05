#pragma once

#include <cstdint>

namespace tell {
namespace store {

/**
 * @brief The possible messages types of a request
 */
enum class RequestType : uint32_t {
    UNKOWN = 0x0u,
    CREATE_TABLE,
    GET_TABLEID,
    GET,
    GET_NEWEST,
    UPDATE,
    INSERT,
    REMOVE,
    REVERT,
    SCAN,
    COMMIT,


    LAST = RequestType::COMMIT
};

/**
 * @brief The possible messages types of a response
 */
enum class ResponseType : uint32_t {
    UNKOWN = 0x0u,
    ERROR,
    CREATE_TABLE,
    GET_TABLEID,
    GET,
    MODIFICATION,
    SCAN,
    COMMIT,


    LAST = ResponseType::COMMIT
};

} // namespace store
} // namespace tell
