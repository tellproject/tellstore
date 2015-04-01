#pragma once

namespace tell {
namespace store {

/**
 * @brief Dummy interface to delete all copy constructor and assignment functions
 */
class NonCopyable {
protected:
    NonCopyable() = default;
    ~NonCopyable() = default;

    // Delete Copy Constructor / Assignment
    NonCopyable(const NonCopyable&) = delete;
    NonCopyable& operator=(const NonCopyable&) = delete;
};

/**
 * @brief Dummy interface to delete all move constructor and assignment functions
 */
class NonMovable {
protected:
    NonMovable() = default;
    ~NonMovable() = default;

    // Delete Move Constructor / Assignment
    NonMovable(NonMovable&&) = delete;
    NonMovable& operator=(NonMovable&&) = delete;
};

} // namespace store
} // namespace tell
