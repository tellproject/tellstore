#pragma once
#include <type_traits>

namespace tell {
namespace store {
template<typename Enum>
constexpr auto to_underlying(Enum e) -> typename std::underlying_type<Enum>::type {
    return static_cast<typename std::underlying_type<Enum>::type>(e);
}

template<typename Enum>
constexpr Enum from_underlying(typename std::underlying_type<Enum>::type s) {
    return static_cast<Enum>(s);
}

} // namespace store
} // namespace tell

