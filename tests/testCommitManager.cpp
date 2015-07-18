#include "DummyCommitManager.hpp"
#include <gtest/gtest.h>

using namespace tell;
using namespace tell::store;

namespace {

TEST(commit_manager_test, simple) {
    CommitManager commitManager;
    auto oldDescriptor = commitManager.startTx();
    auto newDescriptor = commitManager.startTx();
    auto newV = newDescriptor->version();
    EXPECT_FALSE(oldDescriptor->inReadSet(newDescriptor->version()));
    EXPECT_FALSE(newDescriptor->inReadSet(oldDescriptor->version()));
    newDescriptor.commit();
    newDescriptor = commitManager.startTx();
    EXPECT_FALSE(oldDescriptor->inReadSet(newDescriptor->version()));
    EXPECT_TRUE(newDescriptor->inReadSet(newV));
    EXPECT_FALSE(newDescriptor->inReadSet(oldDescriptor->version()));
    oldDescriptor.commit();
    newDescriptor.commit();
}

TEST(commit_manager_test, many_transactions) {
    CommitManager commitManager;
    std::vector<Transaction> descriptors;
    descriptors.reserve(1024);
    // start 1024 transactions
    std::vector<uint64_t> versions;
    versions.reserve(1024);
    for (int i = 0; i < 1024; ++i) {
        descriptors.emplace_back(commitManager.startTx());
        auto version = descriptors[i]->version();
        EXPECT_EQ(version, i + 1) << "Expected version " << i + 1 << " does not match actual version " << version;
        versions.push_back(version);
    }
    // committing every second transaction
    for (int i = 0; i < 1024; i  += 2) {
        descriptors[i].commit();
    }
    auto snapshot = commitManager.startTx();
    // now every second version should be in the read set
    for (int i = 0; i < 1024; ++i) {
        EXPECT_EQ(snapshot->inReadSet(versions[i]), i % 2 == 0) << i << "th transaction (" << versions[i] <<
            ") should " << (i % 2 == 0 ? "" : "not ") << "be in the read set";
    }
    // Commit the other transactions
    for (int i = 1; i < 1024; i += 2) {
        descriptors[i].commit();
    }
    auto allCommitted = commitManager.startTx();
    for (int i = 0; i < 1024; ++i) {
        EXPECT_EQ(snapshot->inReadSet(versions[i]), i % 2 == 0);
        EXPECT_TRUE(allCommitted->inReadSet(versions[i]));
    }
    EXPECT_EQ(allCommitted->baseVersion(), 1024);
    snapshot.commit();
}

}
