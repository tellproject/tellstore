#pragma once

#include "RpcMessages.pb.h"

#include <boost/system/error_code.hpp>

#include <cstdint>

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
class network_category : public boost::system::error_category {
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

inline const boost::system::error_category& get_network_category() {
    static network_category instance;
    return instance;
}

inline boost::system::error_code make_error_code(network_errors e) {
    return boost::system::error_code(static_cast<int>(e), get_network_category());
}


/**
 * @brief Category for network errors
 */
class server_category : public boost::system::error_category {
public:
    const char* name() const noexcept {
        return "tell.store.server";
    }

    std::string message(int value) const {
        switch (value) {
        case proto::RpcResponse::UNKNOWN_REQUEST:
            return "Unknown request";

        case proto::RpcResponse::UNKNOWN_RESPONSE:
            return "Unknown response";

        case proto::RpcResponse::INVALID_SNAPSHOT:
            return "Invalid snapshot descriptor";

        default:
            return "tell.store.server error";
        }
    }
};

inline const boost::system::error_category& get_server_category() {
    static server_category instance;
    return instance;
}

inline boost::system::error_code make_error_code(proto::RpcResponse_Error e) {
    return boost::system::error_code(static_cast<int>(e), get_server_category());
}

} // namespace error
} // namespace store
} // namespace tell

namespace boost {
namespace system {

template<>
struct is_error_code_enum<tell::store::error::network_errors> {
    static const bool value = true;
};

template<>
struct is_error_code_enum<tell::store::proto::RpcResponse_Error> {
    static const bool value = true;
};

} // namespace system
} // namespace boost
