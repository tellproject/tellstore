#include <gtest/gtest.h>
#include <util/Epoch.hpp>

int main(int argc, char** argv) {
    tell::store::init();
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
