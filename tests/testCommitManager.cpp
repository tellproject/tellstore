#include <util/CommitManager.hpp>
#include <gtest/gtest.h>

using namespace tell::store;

namespace {

CommitManager commitManager;


TEST(commit_manager_test, simple) {
    auto oldDescriptor = commitManager.startTx();
    auto newDescriptor = commitManager.startTx();
    auto newV = newDescriptor.version();
    ASSERT_FALSE(oldDescriptor.inReadSet(newDescriptor.version()));
    ASSERT_FALSE(newDescriptor.inReadSet(oldDescriptor.version()));
    commitManager.commitTx(newDescriptor);
    newDescriptor = commitManager.startTx();
    ASSERT_FALSE(oldDescriptor.inReadSet(newDescriptor.version()));
    ASSERT_TRUE(newDescriptor.inReadSet(newV));
    ASSERT_FALSE(newDescriptor.inReadSet(oldDescriptor.version()));
}

}
