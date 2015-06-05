#pragma once

#include <util/Epoch.hpp>
#include <util/ScanQuery.hpp>
#include <util/SnapshotDescriptor.hpp>

#include <crossbow/infinio/InfinibandBuffer.hpp>
#include <crossbow/infinio/InfinibandSocket.hpp>

#include <tbb/spin_mutex.h>

#include <atomic>
#include <cstdint>
#include <system_error>

namespace tell {
namespace store {

/**
 * @brief Data shared between all ClientScanQuery implementations
 */
class ClientScanQueryData {
public:
    /**
     * @brief Status of the scan
     *
     * This value will be encoded with the RDMA writes as user ID so the ClientConnection knows if this was the last
     * RDMA write from a ClientScanQuery
     */
    enum class ScanStatus : uint32_t {
        /// The scan is still ongoing
        ONGOING = 0x1u,

        /// The ClientScanQuery has processed all tuples and can be marked as done
        DONE,

        LAST = ScanStatus::DONE,
    };

    ClientScanQueryData(uint64_t transactionId, SnapshotDescriptor snapshot,
            crossbow::infinio::LocalMemoryRegion& srcRegion, crossbow::infinio::RemoteMemoryRegion destRegion,
            crossbow::infinio::InfinibandSocket socket)
            : mTransactionId(transactionId),
              mSnapshot(std::move(snapshot)),
              mSrcRegion(srcRegion),
              mDestRegion(std::move(destRegion)),
              mSocket(std::move(socket)),
              mOffset(0),
              mUnsignaledCount(0),
              mActive(0) {
    }

    /**
     * @brief Checks if the tuple should be sent to the client
     *
     * @param validFrom The version the tuple is valid from
     * @param validTo The version the tuple is valid to
     */
    bool checkTuple(uint64_t validFrom, uint64_t validTo) const;

    /**
     * @brief Writes the tuples in the buffer to the client
     *
     * In case the local send queue is full the send is retried until the send queue empties. Executes a signaled RDMA
     * write if the number of tuples written since the last signaled write exceeds the maximum, else an unsignaled RDMA
     * write is started for performance reasons.
     *
     * @param buffer The buffer containing the tuples to be written
     * @param ec Error in case the write fails
     */
    void write(crossbow::infinio::ScatterGatherBuffer& buffer, std::error_code& ec);

    /**
     * @brief Writes the tuples in the buffer to the client and marks the ClientScanQuery as done
     *
     * In case the local send queue is full the send is retried until the send queue empties. Always performs a signaled
     * RDMA write.
     *
     * @param buffer The buffer containing the tuples to be written
     * @param ec Error in case the write fails
     */
    void done(crossbow::infinio::ScatterGatherBuffer& buffer, std::error_code& ec);

    /**
     * @brief The transaction ID of the starting process on the remote host
     */
    uint64_t transactionId() const {
        return mTransactionId;
    }

    /**
     * @brief The local memory region covering all tuples
     */
    const crossbow::infinio::LocalMemoryRegion& dataRegion() const {
        return mSrcRegion;
    }

    /**
     * @brief Decrease the number of active ClientScanQuery referencing the shared data
     *
     * @return Whether all scan threads finished their execution
     */
    bool decreaseActive() {
        auto active = --mActive;
        return (active == 0);
    }

    /**
     * @brief Increase the number of active ClientScanQuery referencing the shared data
     */
    void increaseActive() {
        ++mActive;
    }

private:
    /**
     * @brief Perform an unsignaled RDMA write
     *
     * The status of the write is always ONGOING.
     *
     * @param buffer The buffer containing the tuples to be written
     * @param ec Error in case the write fails
     */
    void writeUnsignaled(crossbow::infinio::ScatterGatherBuffer& buffer, std::error_code& ec);

    /**
     * @brief Perform a signaled RDMA write with immediate data containing the number of tuples written so far
     *
     * @param buffer The buffer containing the tuples to be written
     * @param status Status of the signaled RDMA write
     * @param ec Error in case the write fails
     */
    void writeSignaled(crossbow::infinio::ScatterGatherBuffer& buffer, ScanStatus status, std::error_code& ec);

    /// Transaction ID of the starting process on the remote host
    uint64_t mTransactionId;

    /// Snapshot to check the validity of tuples against
    SnapshotDescriptor mSnapshot;

    /// Local memory region covering all tuples
    crossbow::infinio::LocalMemoryRegion& mSrcRegion;

    /// Memory region on the remote host
    crossbow::infinio::RemoteMemoryRegion mDestRegion;

    /// Connection to the remote host
    crossbow::infinio::InfinibandSocket mSocket;

    /// Allocator to protect pages from recycling while the scan has outstanding writes
    allocator mAllocator;

    /// Mutex used to serialize writes over the connection
    tbb::spin_mutex mSendMutex;

    /// Current offset into the destination region
    size_t mOffset;

    /// Number of tuples sent without signaling the client
    uint16_t mUnsignaledCount;

    /// Number of currently active ClientScanQuery
    std::atomic<size_t> mActive;
};

/**
 * @brief ScanQueryImpl sending the data to a remote client
 *
 * Checks the validity of the tuple against a SnapshotDescriptor, gathers all valid tuples into a buffer and writes the
 * buffer to the remote host once the buffer filled up.
 */
class ClientScanQuery : public ScanQueryImpl {
public:
    ClientScanQuery(uint16_t scanId, ClientScanQueryData* data)
            : mBuffer(scanId),
              mData(data) {
        mData->increaseActive();
    }

    virtual ~ClientScanQuery();

    virtual void process(uint64_t validFrom, uint64_t validTo, const char* data, size_t size, const Record& record)
            final override;

private:
    /// Buffer to gather all valid tuples
    crossbow::infinio::ScatterGatherBuffer mBuffer;

    /// The shared data between the different scan threads
    ClientScanQueryData* mData;
};

} // namespace store
} // namespace tell
