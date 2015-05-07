#pragma once

#include <util/Logging.hpp>

#include <atomic>
#include <cstdint>
#include <limits>

namespace tell {
namespace store {
namespace logstructured {

/**
 * @brief TODO Documentation
 */
class LSMRecord {
public:
    static LSMRecord* recordFromData(char* data) {
        return const_cast<LSMRecord*>(recordFromData(const_cast<const char*>(data)));
    }

    static const LSMRecord* recordFromData(const char* data) {
        return reinterpret_cast<const LSMRecord*>(data - sizeof(LSMRecord));
    }

    LSMRecord(uint64_t key)
            : mKey(key) {
    }

    uint64_t key() const {
        return mKey.load();
    }

    char* data() {
        return const_cast<char*>(const_cast<const LSMRecord*>(this)->data());
    }

    const char* data() const {
        return reinterpret_cast<const char*>(this) + sizeof(LSMRecord);
    }

private:
    const std::atomic<uint64_t> mKey;
};

/**
 * @brief TODO Documentation
 */
class ChainedVersionRecord {
public:
    /**
     * @brief Version marking an invalid tuple
     */
    static constexpr uint64_t INVALID_VERSION = std::numeric_limits<uint64_t>::max();

    ChainedVersionRecord(uint64_t validFrom, ChainedVersionRecord* previous, bool deleted)
            : mValidFrom(validFrom),
              mPrevious(reinterpret_cast<uintptr_t>(previous) | (deleted ? 0x1u : 0x0u)) {
    }

    uint64_t validFrom() const {
        return mValidFrom.load();
    }

    uint64_t validTo() const {
        return mValidTo.load();
    }

    void invalidate() {
        mValidFrom.store(INVALID_VERSION);
    }

    bool isInvalid() const {
        return (mValidFrom.load() == INVALID_VERSION);
    }

    /**
     * @brief Set the tuple to expire with the given version
     *
     * Overrides the valid-to version even if it is already set.
     *
     * @param version Version until the tuple is valid
     */
    void expire(uint64_t version) {
        LOG_ASSERT(mValidFrom.load() <= version, "Tuple set to expire before its valid-from version");
        mValidTo.store(version);
    }

    /**
     * @brief Reactivates the tuple that was set to expire with the given version
     *
     * @param version Version the tuple was previously set to expire
     * @return Whether the tuple was reactivated
     */
    bool reactivate(uint64_t version) {
        return mValidTo.compare_exchange_strong(version, 0x0u);
    }

    ChainedVersionRecord* getPrevious() {
        return const_cast<ChainedVersionRecord*>(const_cast<const ChainedVersionRecord*>(this)->getPrevious());
    }

    const ChainedVersionRecord* getPrevious() const {
        return reinterpret_cast<ChainedVersionRecord*>(mPrevious.load() & (UINTPTR_MAX << 1));
    }

    bool wasDeleted() const {
        return ((mPrevious.load() & 0x1u) != 0x0u);
    }

    char* data() {
        return const_cast<char*>(const_cast<const ChainedVersionRecord*>(this)->data());
    }

    const char* data() const {
        return reinterpret_cast<const char*>(this) + sizeof(ChainedVersionRecord);
    }

private:
    std::atomic<uint64_t> mValidFrom;
    std::atomic<uint64_t> mValidTo;
    std::atomic<uintptr_t> mPrevious;
};

} // namespace logstructured
} // namespace store
} // namespace tell

