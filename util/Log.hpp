#pragma once

#include <config.h>

#include "Logging.hpp"
#include "NonCopyable.hpp"
#include "PageManager.hpp"

#include <atomic>
#include <cstddef>
#include <iterator>

namespace tell {
namespace store {

/**
 * @brief A single entry in the log able to store any arbitrary data
 */
class LogEntry : NonCopyable, NonMovable {
public:
    /**
     * @brief Constructor will never be called
     *
     * Everything will be zero initialized.
     */
    LogEntry() = delete;

    /**
     * @brief Destructor will never be called
     */
    ~LogEntry() = delete;

    /**
     * @brief The size of the entry in the log
     *
     * @note This is not the size of the pure data payload but the size of the whole (8 byte padded) log entry.
     */
    uint32_t size() const {
        return (mSize.load() & (0xFFFFFFFFu << 1));
    }

    char* data() {
        return reinterpret_cast<char*>(this) + sizeof(LogEntry);
    }

    const char* data() const {
        return reinterpret_cast<const char*>(this) + sizeof(LogEntry);
    }

    /**
     * @brief Whether the entry was written completely
     *
     * Checks if the LSB of size is 0.
     */
    bool sealed() const {
        return ((mSize.load() & 0x1u) == 0);
    }

    /**
     * @brief Seals the entry indicating that it has been completely written
     *
     * Sets the LSB of size to 0.
     */
    void seal() {
        LOG_ASSERT(!sealed(), "Entry is already sealed");
        mSize.fetch_and(0xFFFFFFFFu << 1);
    }

private:
    friend class LogPage;

    /**
     * @brief Tries to create a new LogEntry at this log position
     *
     * Acquires the space for this entry in the log by compare and swapping the size field from 0 to the given size.
     * Because the PageManager returns zeroed pages, the object is zero initalized (and size thus 0) if nobody has
     * written to it yet.
     *
     * @param size Size of the whole 8 byte padded log entry
     * @return 0 if the log entry was successfully acquired else the previously reserved size of this log entry
     */
    uint32_t tryAcquire(uint32_t size);

    /// Size of this LogEntry as the 8 byte aligned size of the LogEntry class plus data payload
    std::atomic<uint32_t> mSize;
};

/**
 * @brief A single page in the log containing multiple LogEntry's
 *
 * A Log-Page has the following form:
 *
 * -----------------------------------------------------------------------------------
 * | previous (8 bytes) | offset (4 bytes) | padding (4 bytes) | entry | entry | ... |
 * -----------------------------------------------------------------------------------
 */
class LogPage : NonCopyable, NonMovable {
public:
    /// Size of the LogPage data structure
    static constexpr size_t LOG_HEADER_SIZE = 16;

    /**
     * @brief Iterator for iterating over all entries in a page
     */
    class EntryIterator : public std::iterator<std::input_iterator_tag, LogEntry> {
    public:
        EntryIterator(LogPage* page, uint32_t pos)
                : mPage(page),
                  mPos(pos) {
        }

        LogPage* page() const {
            return mPage;
        }

        uint32_t offset() const {
            return mPos;
        }

        EntryIterator& operator++() {
            auto entry = reinterpret_cast<LogEntry*>(mPage->data() + mPos);
            mPos += entry->size();
            return *this;
        }

        EntryIterator operator++(int) {
            EntryIterator result(*this);
            operator++();
            return result;
        }

        bool operator==(const EntryIterator& rhs) const {
            return ((mPage == rhs.mPage) && (mPos == rhs.mPos));
        }

        bool operator!=(const EntryIterator& rhs) const {
            return !operator==(rhs);
        }

        reference operator*() {
            return *operator->();
        }

        pointer operator->() const {
            return reinterpret_cast<LogEntry*>(mPage->data() + mPos);
        }

    private:
        /// Page this iterator belongs to
        LogPage* mPage;

        /// Current offset the iterator is pointing to
        uint32_t mPos;
    };

    LogPage(LogPage* previous);

    char* data() {
        return reinterpret_cast<char*>(this) + LOG_HEADER_SIZE;
    }

    const char* data() const {
        return reinterpret_cast<const char*>(this) + LOG_HEADER_SIZE;
    }

    /**
     * @brief Appends a new entry to this log page
     *
     * @param size Size of the new entry
     * @return Pointer to allocated LogEntry or nullptr if unable to allocate the entry in this page
     */
    LogEntry* append(uint32_t size);

    /**
     * @brief Page preceeding the current page in the log
     */
    std::atomic<LogPage*>& previous() {
        return mPrevious;
    }

    /**
     * @brief Current offset into the page
     */
    uint32_t offset() const {
        return (mOffset.load() & (0xFFFFFFFFu << 1));
    }

    EntryIterator begin() {
        return EntryIterator(this, 0x0u);
    }

    EntryIterator end() {
        return EntryIterator(this, offset());
    }

    /**
     * @brief Whether the page is sealed and as such write protected
     *
     * Checks if the LSB of offset is 0.
     */
    bool sealed() const {
        return ((mOffset.load() & 0x1u) == 0);
    }

    /**
     * @brief Seals the page indicating that it is write protected
     *
     * Sets the LSB of offset to 0.
     */
    void seal() {
        LOG_ASSERT(!sealed(), "Page is already sealed");
        mOffset.fetch_and(0xFFFFFFFFu << 1);
    }

    /**
     * @brief Tries to seal the page at the given offset
     *
     * Only seals the page if the current page offset is equal to the given offset.
     *
     * Compare and swaps the given offset with LSB 1 (not sealed) to the given offset with LSB 0 (sealed).
     *
     * @param offset The offset this page should be sealed at
     * @return Whether the page was successfully sealed
     */
    bool seal(uint32_t offset) {
        LOG_ASSERT(!sealed(), "Page is already sealed");
        LOG_ASSERT((offset & 0x1u) == 0, "LSB has to be zero");
        auto exp = (offset | 0x1u);
        return mOffset.compare_exchange_strong(exp, offset);
    }

private:
    friend class Log;

    /**
     * @brief Appends a new entry to the log
     *
     * @param size Complete 8 byte padded size of the new entry
     * @return Pointer to allocated LogEntry or nullptr if unable to allocate the entry in this page
     */
    LogEntry* appendEntry(uint32_t size);

    std::atomic<LogPage*> mPrevious;
    std::atomic<uint32_t> mOffset;
};

/**
 * @brief The Log class
 */
class Log : NonCopyable {
public:
    static constexpr uint32_t MAX_SIZE = TELL_PAGE_SIZE - LogPage::LOG_HEADER_SIZE;

    /**
     * @brief Iterator for iterating over all pages in the log
     */
    class PageIterator : public std::iterator<std::input_iterator_tag, LogPage> {
    public:
        PageIterator(LogPage* page)
                : mPage(page) {
        }

        PageIterator& operator++() {
            mPage = mPage->previous().load();
            return *this;
        }

        PageIterator operator++(int) {
            PageIterator result(*this);
            operator++();
            return result;
        }

        bool operator==(const PageIterator& rhs) const {
            return (mPage == rhs.mPage);
        }

        bool operator!=(const PageIterator& rhs) const {
            return !operator==(rhs);
        }

        reference operator*() {
            return *operator->();
        }

        pointer operator->() const {
            return mPage;
        }

    private:
        /// Current page this iterator is pointing to
        LogPage* mPage;
    };

    /**
     * @brief Iterator for iterating over all entries in the log
     *
     * The iterator does not retrieve elements in insert order.
     */
    class LogIterator : public std::iterator<std::input_iterator_tag, LogEntry> {
    public:
        LogIterator(LogPage* page)
                : mPage(page),
                  mPageOffset(mPage ? mPage->offset() : 0),
                  mPos(0) {
        }

        LogPage* page() const {
            return mPage;
        }

        uint32_t offset() const {
            return mPos;
        }

        LogIterator& operator++() {
            auto entry = reinterpret_cast<LogEntry*>(mPage->data() + mPos);
            mPos += entry->size();
            if (mPos == mPageOffset) {
                mPage = mPage->previous().load();
                mPageOffset = (mPage ? mPage->offset() : 0);
                mPos = 0;
            }
            return *this;
        }

        LogIterator operator++(int) {
            LogIterator result(*this);
            operator++();
            return result;
        }

        bool operator==(const LogIterator& rhs) const {
            return ((mPage == rhs.mPage) && (mPos == rhs.mPos));
        }

        bool operator!=(const LogIterator& rhs) const {
            return !operator==(rhs);
        }

        reference operator*() {
            return *operator->();
        }

        pointer operator->() const {
            return reinterpret_cast<LogEntry*>(mPage->data() + mPos);
        }

    private:
        /// Current page this iterator operates on
        LogPage* mPage;

        /// Maximum offset of the current page
        uint32_t mPageOffset;

        /// Current offset the iterator is pointing to
        uint32_t mPos;
    };

    Log(PageManager& pageManager);

    ~Log();

    /**
     * @brief Appends a new entry to the log
     *
     * @param size Size of the new entry
     * @return Pointer to allocated LogEntry or nullptr if unable to allocate the entry
     */
    LogEntry* append(uint32_t size);

    LogPage* head() {
        return mHead.load();
    }

    PageIterator pageBegin() {
        return PageIterator(mHead.load());
    }

    PageIterator pageEnd() {
        return PageIterator(nullptr);
    }

    LogIterator begin() {
        return LogIterator(mHead.load());
    }

    LogIterator end() {
        return LogIterator(nullptr);
    }

private:
    PageManager& mPageManager;

    std::atomic<LogPage*> mHead;
};

} // namespace store
} // namespace tell
