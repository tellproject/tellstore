#pragma once

#include <util/Logging.hpp>

#include <atomic>
#include <cstdint>
#include <limits>

namespace tell {
namespace store {
namespace logstructured {

/**
 * @brief Types of record entries written to the log
 *
 * Currently both DATA and DELETION entries store a ChainedVersionRecord structure in the log.
 */
enum class VersionRecordType : uint32_t {
    DATA = 0x0u,
    DELETION = 0x1u,
};

/**
 * @brief Mutable data associated with a version record
 *
 * Contains the version the record expires, the pointer to the next element in the version list and a marker if the
 * element is invalid.
 */
class alignas(16) MutableRecordData {
public:
    MutableRecordData() noexcept
            : MutableRecordData(0x0u, nullptr, true) {
    }

    MutableRecordData(const MutableRecordData& other, uint64_t validTo)
            : mValidTo(validTo),
              mNext(other.mNext) {
    }

    MutableRecordData(const MutableRecordData& other, ChainedVersionRecord* ptr, bool invalid)
            : mValidTo(other.mValidTo),
              mNext(reinterpret_cast<uintptr_t>(ptr) | (invalid ? gInvalidMarker : 0x0u)) {
        LOG_ASSERT((reinterpret_cast<uintptr_t>(ptr) % 8) == 0, "Pointer not 8 byte aligned");
        LOG_ASSERT(next() == ptr, "Returned context pointer differs");
        LOG_ASSERT(isInvalid() == invalid, "Returned invalid marker differs");
    }

    MutableRecordData(uint64_t validTo, ChainedVersionRecord* ptr, bool invalid)
            : mValidTo(validTo),
              mNext(reinterpret_cast<uintptr_t>(ptr) | (invalid ? gInvalidMarker : 0x0u)) {
        LOG_ASSERT((reinterpret_cast<uintptr_t>(ptr) % 8) == 0, "Pointer not 8 byte aligned");
        LOG_ASSERT(next() == ptr, "Returned context pointer differs");
        LOG_ASSERT(isInvalid() == invalid, "Returned invalid marker differs");
    }

    uint64_t validTo() const {
        return mValidTo;
    }

    ChainedVersionRecord* next() {
        return const_cast<ChainedVersionRecord*>(const_cast<const MutableRecordData*>(this)->next());
    }

    const ChainedVersionRecord* next() const {
        return reinterpret_cast<const ChainedVersionRecord*>(mNext & ~gInvalidMarker);
    }

    bool isInvalid() const {
        return ((mNext & gInvalidMarker) != 0x0u);
    }

private:
    /**
     * @brief Marker indicating if the element is invalid (encoded in the next pointer)
     */
    static constexpr uintptr_t gInvalidMarker = 0x1u;

    /// Version this element expires
    uint64_t mValidTo;

    /// Pointer to the next element in the version list
    /// The LSB is used as a marker to indicate if the element is invalid
    uintptr_t mNext;
};

/**
 * @brief Data structure storing a record in the log
 *
 * The record entry is immutable once written except for a small portion of mutable data encapsulated in the
 * MutableRecordData class.
 *
 * The class needs an alignment of 16 byte due to the required double-width compare-and-swap.
 */
class ChainedVersionRecord {
public:
    /**
     * @brief Valid-to version marking an active (not yet expired) tuple
     */
    static constexpr uint64_t ACTIVE_VERSION = std::numeric_limits<uint64_t>::max();

    ChainedVersionRecord(uint64_t key, uint64_t validFrom)
            : mKey(key),
              mValidFrom(validFrom) {
    }

    uint64_t key() const {
        return mKey;
    }

    uint64_t validFrom() const {
        return mValidFrom;
    }

    MutableRecordData mutableData() const {
        return mData.load();
    }

    void mutableData(const MutableRecordData& data) {
        mData.store(data);
    }

    char* data() {
        return const_cast<char*>(const_cast<const ChainedVersionRecord*>(this)->data());
    }

    const char* data() const {
        return reinterpret_cast<const char*>(this) + sizeof(ChainedVersionRecord);
    }

    /**
     * @brief Expires the record in the given version
     *
     * @param expected The previous mutable data state to update
     * @param version The version in which to expire the record
     * @return Whether the record was set to expire
     */
    bool tryExpire(MutableRecordData& expected, uint64_t version) {
        LOG_ASSERT(version != ACTIVE_VERSION, "Can not expire to active version");
        LOG_ASSERT(!expected.isInvalid(), "Expiring invalid record");
        return mData.compare_exchange_strong(expected, MutableRecordData(expected, version));
    }

    /**
     * @brief Reactivates the record that was set to expire with the given version
     *
     * @param expected The previous mutable data state to update
     * @return Whether the record was reactivated
     */
    bool tryReactivate(MutableRecordData& expected) {
        LOG_ASSERT(!expected.isInvalid(), "Reactivating invalid record");
        return mData.compare_exchange_strong(expected, MutableRecordData(expected, ACTIVE_VERSION));
    }

    /**
     * @brief Force invalidates the record
     *
     * Should only be used when certain that no other thread is accessing the record concurrently
     */
    void invalidate() {
        return mData.store(MutableRecordData());
    }

    /**
     * @brief Invalidate the tuple, changing the next pointer to the given record
     *
     * @param expected The previous mutable data state to update
     * @param record The updated next record in the version list
     * @return Whether the record was invalidated
     */
    bool tryInvalidate(MutableRecordData& expected, ChainedVersionRecord* record) {
        LOG_ASSERT(!expected.isInvalid(), "Changing invalid record");
        return mData.compare_exchange_strong(expected, MutableRecordData(expected, record, true));
    }

    /**
     * @brief Change the next pointer to the given record
     *
     * @param expected The previous mutable data state to update
     * @param record The updated next record in the version list
     * @return Whether the record was updated
     */
    bool tryNext(MutableRecordData& expected, ChainedVersionRecord* record) {
        LOG_ASSERT(!expected.isInvalid(), "Changing invalid record");
        return mData.compare_exchange_strong(expected, MutableRecordData(expected, record, false));
    }

private:
    const uint64_t mKey;
    const uint64_t mValidFrom;
    std::atomic<MutableRecordData> mData;
};

} // namespace logstructured
} // namespace store
} // namespace tell

