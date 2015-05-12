#pragma once

#include <crossbow/string.hpp>

#include <cstdint>

namespace tell {
namespace store {

struct ClientConfig {
    size_t networkThreads = 2;

    crossbow::string server = "";

    uint16_t port = 7241;
};

} // namespace store
} // namespace tell
