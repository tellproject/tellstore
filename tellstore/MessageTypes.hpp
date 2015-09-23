#pragma once

#include <cstdint>

namespace tell {
namespace store {

/**
 * @brief The possible messages types of a request
 */
enum class RequestType : uint32_t {
    CREATE_TABLE = 0x1u,
    GET_TABLE,
    GET,
    UPDATE,
    INSERT,
    REMOVE,
    REVERT,
    SCAN,
    SCAN_PROGRESS,
    COMMIT,
};

/**
 * @brief The possible messages types of a response
 */
enum class ResponseType : uint32_t {
    CREATE_TABLE = 0x01u,
    GET_TABLE,
    GET,
    MODIFICATION,
    SCAN,
    COMMIT,
};

} // namespace store
} // namespace tell
