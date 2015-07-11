#pragma once

#include <cstdint>
#include <system_error>
#include <type_traits>

namespace tell {
namespace store {
namespace error {

/**
 * @brief TellStore errors triggered while executing an operation
 */
enum errors {
    /// Server received an unknown request type.
    unkown_request = 1,

    /// Client received an unkown response type.
    unkown_response,

    /// Table does not exist.
    invalid_table,

    /// The tuple was invalid.
    invalid_tuple,

    /// Snapshot sent to the server was invalid.
    invalid_snapshot,

    /// ID sent to the server was invalid.
    invalid_scan,

    /// Operation failed due to server overload.
    server_overlad,
};

/**
 * @brief Category for TellStore errors
 */
class error_category : public std::error_category {
public:
    const char* name() const noexcept {
        return "tell.store";
    }

    std::string message(int value) const {
        switch (value) {
        case error::unkown_request:
            return "Server received an unknown request type";

        case error::unkown_response:
            return "Client received an unkown response type";

        case error::invalid_table:
            return "Table was invalid";

        case error::invalid_tuple:
            return "Tuple was invalid";

        case error::invalid_snapshot:
            return "Snapshot was invalid";

        case error::invalid_scan:
            return "Scan ID was invalid";

        case error::server_overlad:
            return "Operation failed due to server overload";

        default:
            return "tell.store.server error";
        }
    }
};

inline const std::error_category& get_error_category() {
    static error_category instance;
    return instance;
}

inline std::error_code make_error_code(error::errors e) {
    return std::error_code(static_cast<int>(e), get_error_category());
}

} // namespace error
} // namespace store
} // namespace tell

namespace std {

template<>
struct is_error_code_enum<tell::store::error::errors> : public std::true_type {
};

} // namespace std
