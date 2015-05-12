#include <util/CommitManager.hpp>
#include <gtest/gtest.h>

using namespace tell::store;

namespace {

TEST(commit_manager_test, simple) {
    CommitManager commitManager;
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
    commitManager.commitTx(oldDescriptor);
    commitManager.commitTx(newDescriptor);
}

TEST(commit_manager_test, many_transactions) {
    CommitManager commitManager;
    std::vector<SnapshotDescriptor> descriptors;
    descriptors.reserve(1024);
    // start 1024 transactions
    std::vector<uint64_t> versions;
    versions.reserve(1024);
    for (int i = 0; i < 1024; ++i) {
        descriptors.emplace_back(commitManager.startTx());
        versions.push_back(descriptors[i].version());
    }
    // committing every second transaction
    for (int i = 0; i < 1024; i  += 2) {
        commitManager.commitTx(descriptors[i]);
    }
    auto snapshot = commitManager.startTx();
    // now every second version should be in the read set
    for (int i = 0; i < 1024; ++i) {
        ASSERT_EQ(snapshot.inReadSet(versions[i]), i % 2 == 0) << i << "th transaction (" << versions[i] <<
            ") should " << (i % 2 == 0 ? "" : "not ") << "be in the read set";
    }
    // Commit the other transactions
    for (int i = 1; i < 1024; i += 2) {
        commitManager.commitTx(descriptors[i]);
    }
    auto allCommitted = commitManager.startTx();
    for (int i = 0; i < 1024; ++i) {
        ASSERT_EQ(snapshot.inReadSet(versions[i]), i % 2 == 0);
        ASSERT_TRUE(allCommitted.inReadSet(versions[i]));
    }
    ASSERT_EQ(allCommitted.baseVersion(), 1025);
    commitManager.commitTx(snapshot);
    ASSERT_EQ(commitManager.getLowestActiveVersion(), 1024);
}

}
