#include <gtest/gtest.h>

#include <crossbow/allocator.hpp>

int main(int argc, char** argv) {
    crossbow::allocator::init();
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
