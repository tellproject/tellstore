#pragma once

#include <cstdint>
#include <system_error>
#include <type_traits>

namespace tell {
namespace store {
namespace error {

/**
 * @brief Network errors related to actions on the network
 */
enum network_errors {
    /// Network buffer was invalid.
    invalid_buffer = 1,
};

/**
 * @brief Category for network errors
 */
class network_category : public std::error_category {
public:
    const char* name() const noexcept {
        return "tell.store.network";
    }

    std::string message(int value) const {
        switch (value) {
        case error::invalid_buffer:
            return "Network buffer was invalid";

        default:
            return "tell.store.network error";
        }
    }
};

inline const std::error_category& get_network_category() {
    static network_category instance;
    return instance;
}

inline std::error_code make_error_code(network_errors e) {
    return std::error_code(static_cast<int>(e), get_network_category());
}


/**
 * @brief Server errors triggered while processing a request
 */
enum server_errors {
    /// Server received an unknown request type.
    unkown_request = 1,

    /// Client received an unkown response type.
    unkown_response = 2,

    /// Snapshot sent to the server was invalid.
    invalid_snapshot = 3,
};

/**
 * @brief Category for server errors
 */
class server_category : public std::error_category {
public:
    const char* name() const noexcept {
        return "tell.store.server";
    }

    std::string message(int value) const {
        switch (value) {
        case error::unkown_request:
            return "Server received an unknown request type";

        case error::unkown_response:
            return "Client received an unkown response type";

        case error::invalid_snapshot:
            return "Snapshot sent to the server was invalid";

        default:
            return "tell.store.server error";
        }
    }
};

inline const std::error_category& get_server_category() {
    static server_category instance;
    return instance;
}

inline std::error_code make_error_code(error::server_errors e) {
    return std::error_code(static_cast<int>(e), get_server_category());
}

} // namespace error
} // namespace store
} // namespace tell

namespace std {

template<>
struct is_error_code_enum<tell::store::error::network_errors> : public std::true_type {
};

template<>
struct is_error_code_enum<tell::store::error::server_errors> : public std::true_type {
};

} // namespace std
