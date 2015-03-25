#pragma once

#include <algorithm>
#include <util/SnapshotDescriptor.hpp>

namespace tell {
namespace store {

/**
 * This is a simple snapshot holder.
 * The idea behind this object is, to
 * make sure that a user never forgets
 * to commit or abort a transaction.
 * Objects of this class will simply hold
 * a snapshot descriptor and will commit
 * a transaction when the object gets
 * destroyed (if the user did not do so
 * manually.
 *
 * It is important that this class is a
 * friend of the storage implementation
 * and that the commit method in storage
 * is private.
 */
template<class StorageImpl>
class TransactionImpl {
    friend StorageImpl;
    StorageImpl* mStorage;
    SnapshotDescriptor mDescriptor;
    bool mCommitted = false;
public:
    TransactionImpl(StorageImpl& storage, SnapshotDescriptor&& snapshot)
        : mStorage(&storage)
        , mDescriptor(std::move(snapshot))
    {
    }

    TransactionImpl(const TransactionImpl&) = delete;
    TransactionImpl(TransactionImpl&& other)
        : mStorage(other.mStorage)
        , mDescriptor(std::move(other.mDescriptor))
    {}

    ~TransactionImpl()
    {
        if (mCommitted)
            mStorage->commit(*this);
        mCommitted = false;
    }

    TransactionImpl operator= (const TransactionImpl&) = delete;
    TransactionImpl operator= (TransactionImpl&& other)
    {
        // First we need to destroy ourself
        if (!mCommitted)
            mStorage->commit(mDescriptor);
        mStorage = other.mStorage;
        mDescriptor = std::move(other.mDescriptor);
        mCommitted = other.mCommitted;
        other.mCommitted = true;
        return *this;
    }

public:
    operator const SnapshotDescriptor&() const {
        return mDescriptor;
    }

    const SnapshotDescriptor& descriptor() const
    {
        return mDescriptor;
    }

    void commit()
    {
        if (mCommitted)
            return;
        mStorage->commit(mDescriptor);
        mCommitted = true;
    }

    void abort()
    {
        if (mCommitted)
            return;
        mStorage->abort(mDescriptor);
        mCommitted = true;
    }
};

} // namespace store
} // namespace tell
