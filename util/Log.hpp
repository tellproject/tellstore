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
    /// Size of the LogEntry data structure
    static constexpr size_t LOG_ENTRY_SIZE = 4;

    /**
     * @brief Returns the LogEntry struct associated with a given data pointer
     *
     * The data pointer has to be the pointer that was obtained from LogEntry::data().
     *
     * @param data The data pointer
     * @return The LogEntry struct associated with the data pointer
     */
    static LogEntry* entryFromData(char* data) {
        return const_cast<LogEntry*>(entryFromData(const_cast<const char*>(data)));
    }

    static const LogEntry* entryFromData(const char* data) {
        return reinterpret_cast<const LogEntry*>(data - sizeof(LogEntry));
    }

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
 * -------------------------------------------------------------------------------
 * | next (8 bytes) | offset (4 bytes) | entry | entry | ... | padding (4 bytes) |
 * -------------------------------------------------------------------------------
 *
 * Entries require 4 bytes of space followed by the associated data segment. To keep this data segment 8 byte aligned
 * the log pads the entries to a multiple of 8 bytes and writes the LogEntries at offset 4. Any subsequent entries are
 * aligned with offset 4 due to the padding.
 */
class LogPage : NonCopyable, NonMovable {
public:
    /// Size of the LogPage data structure
    static constexpr size_t LOG_HEADER_SIZE = 12;

    /// Maximum size of a log entry
    static constexpr uint32_t MAX_ENTRY_SIZE = TELL_PAGE_SIZE - (LogPage::LOG_HEADER_SIZE + 4);

    /// Maximum size of a log entries data payload
    static constexpr uint32_t MAX_DATA_SIZE = MAX_ENTRY_SIZE - LogEntry::LOG_ENTRY_SIZE;

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

    LogPage()
            : mOffset(0x1u) {
    }

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
     * @brief Appends a new entry to the log
     *
     * @param size Complete 8 byte padded size of the new entry
     * @return Pointer to allocated LogEntry or nullptr if unable to allocate the entry in this page
     */
    LogEntry* appendEntry(uint32_t size);

    /**
     * @brief Page preceeding the current page in the log
     */
    std::atomic<LogPage*>& next() {
        return mNext;
    }

    const std::atomic<LogPage*>& next() const {
        return mNext;
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
    std::atomic<LogPage*> mNext;
    std::atomic<uint32_t> mOffset;
};

/**
 * @brief Base class for Log implementations
 *
 * Provides the basic page handling mechanism.
 */
class BaseLogImpl {
protected:
    BaseLogImpl(PageManager& pageManager)
            : mPageManager(pageManager) {
    }

    /**
     * @brief Acquires an empty log page from the page manager
     */
    LogPage* acquirePage() {
        return new(mPageManager.alloc()) LogPage();
    }

    /**
     * @brief Returns an unwritten log page (no entries appended) to the page manager
     */
    void freeEmptyPageNow(LogPage* page);

    /**
     * @brief Returns a log page to the page manager
     */
    void freePageNow(LogPage* page) {
        mPageManager.free(page);
    }

    /**
     * @brief Deletes all pages between the begin page and the end page (including begin but excluding end)
     *
     * Deletes pages using the Safe Memory Reclamation mechanism.
     *
     * @param begin The first page to be deleted
     * @param end The page succeeding the last deleted pages
     */
    void freePage(LogPage* begin, LogPage* end);

private:
    PageManager& mPageManager;
};

/**
 * @brief Log implementation providing no order guarantee when iterating over the elements
 *
 * Links pages together by storing a pointer to the preceeding page (i.e. the current head contains a pointer to the
 * previous head).
 *
 * Pages are iterated from the head (newest page) to the tail (oldest page).
 */
class UnorderedLogImpl : public BaseLogImpl {
public:
    LogPage* head() {
        return mHead.load().writeHead;
    }

    /**
     * @brief Appends the given pages to the log
     *
     * @param begin The first page to append
     * @param end The last page to append
     */
    void appendPage(LogPage* begin, LogPage* end);

    void appendPage(LogPage* page) {
        appendPage(page, page);
    }

protected:
    UnorderedLogImpl(PageManager& pageManager);

    LogEntry* append(uint32_t size);

    /**
     * @brief Log iteration starts from the head
     */
    LogPage* startPage() {
        auto head = mHead.load();
        return (head.appendHead ? head.appendHead : head.writeHead);
    }

    const LogPage* startPage() const {
        auto head = mHead.load();
        return (head.appendHead ? head.appendHead : head.writeHead);
    }

private:
    /**
     * @brief Struct containing the two log heads
     *
     * The write head is used when appending entries to the log using Log::append(uint32_t size). The append head stores
     * the head page of the most recent appended page using Log::appendPage(LogPage* begin, LogPage* end). When the
     * write head page is full and the append head is not null the append head will become the new head.
     *
     * The 16 byte alignment is required on x64 for the 128 bit CAS to work correctly.
     */
    struct alignas(16) LogHead {
        LogHead() noexcept = default;

        LogHead(LogPage* write, LogPage* append)
                : writeHead(write),
                  appendHead(append) {
        }

        LogPage* writeHead;
        LogPage* appendHead;
    };

    /**
     * @brief Tries to allocate a new head page
     */
    LogHead createPage(LogHead oldHead);

    std::atomic<LogHead> mHead;
};

/**
 * @brief Log implementation to iterate over the elements in insert order
 *
 * Links pages together by storing a pointer to the following page (i.e. the previous head contains a pointer to the
 * current head).
 *
 * Pages are iterated from the tail (oldest page) to the head (newest page). This allows the log elements to be iterated
 * from oldest written to newest written.
 */
class OrderedLogImpl : public BaseLogImpl {
public:
    LogPage* head() {
        return mHead.load();
    }

    LogPage* tail() {
        return mTail.load();
    }

    /**
     * @brief Tries to set the new tail page of the log from oldTail to newTail
     *
     * After this operation the log will begin at the new tail instead from the old tail. All truncated pages will be
     * freed by the Safe Memory Reclamation mechanism.
     *
     * @param oldTail The previous tail
     * @param newTail The new tail
     * @return True if the truncation succeeded
     */
    bool truncateLog(LogPage* oldTail, LogPage* newTail);

protected:
    OrderedLogImpl(PageManager& pageManager);

    LogEntry* append(uint32_t size);

    /**
     * @brief Log iteration starts from the tail
     */
    LogPage* startPage() {
        return mTail.load();
    }

    const LogPage* startPage() const {
        return mTail.load();
    }

private:
    /**
     * @brief Tries to allocate a new head page
     */
    LogPage* createPage(LogPage* oldHead);

    std::atomic<LogPage*> mHead;
    std::atomic<LogPage*> mTail;
};

/**
 * @brief The Log class
 */
template <class Impl>
class Log : public Impl, NonCopyable {
public:
    /**
     * @brief Iterator for iterating over all pages in the log
     *
     * The order in which pages are iterated (i.e. head from tail, tail to head) is dependent on the chosen Log
     * implementation.
     */
    class PageIterator : public std::iterator<std::input_iterator_tag, LogPage> {
    public:
        PageIterator(LogPage* page)
                : mPage(page) {
        }

        PageIterator& operator++() {
            mPage = mPage->next().load();
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
     * The order in which elements are iterated is dependent on the chosen Log implementation.
     */
    template<class EntryType>
    class LogIteratorImpl : public std::iterator<std::input_iterator_tag, typename std::conditional<std::is_const<EntryType>::value, const LogEntry, LogEntry>::type> {
    public:
        static constexpr bool is_const_iterator = std::is_const<typename std::remove_pointer<EntryType>::type>::value;
        using reference = typename std::conditional<is_const_iterator, const LogEntry&, LogEntry&>::type;
        using const_reference = const LogEntry&;
        using pointer = typename std::conditional<is_const_iterator, const LogEntry*, LogEntry*>::type;
        using const_pointer = const LogEntry*;
        LogIteratorImpl(EntryType page)
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

        LogIteratorImpl& operator++() {
            auto entry = reinterpret_cast<pointer>(mPage->data() + mPos);
            mPos += entry->size();
            if (mPos == mPageOffset) {
                mPage = mPage->next().load();
                mPageOffset = (mPage ? mPage->offset() : 0);
                mPos = 0;
            }
            return *this;
        }

        LogIteratorImpl<EntryType> operator++(int) {
            LogIteratorImpl<EntryType> result(*this);
            operator++();
            return result;
        }

        bool operator==(const LogIteratorImpl<EntryType>& rhs) const {
            return ((mPage == rhs.mPage) && (mPos == rhs.mPos));
        }

        bool operator!=(const LogIteratorImpl<EntryType>& rhs) const {
            return !operator==(rhs);
        }

        reference operator*() {
            return *operator->();
        }

        const_reference operator*() const {
            return *operator->();
        }

        pointer operator->() {
            return reinterpret_cast<pointer>(mPage->data() + mPos);
        }

        const_pointer operator->() const {
            return reinterpret_cast<const_pointer>(mPage->data() + mPos);
        }

    private:
        /// Current page this iterator operates on
        EntryType mPage;

        /// Maximum offset of the current page
        uint32_t mPageOffset;

        /// Current offset the iterator is pointing to
        uint32_t mPos;
    };
    using LogIterator = LogIteratorImpl<LogPage*>;
    using ConstLogIterator = LogIteratorImpl<const LogPage*>;

    Log(PageManager& pageManager)
            : Impl(pageManager) {
    }

    ~Log();

    /**
     * @brief Appends a new entry to the log
     *
     * @param size Size of the new entry
     * @return Pointer to allocated LogEntry or nullptr if unable to allocate the entry
     */
    LogEntry* append(uint32_t size);

    /**
     * @brief Deletes all pages between the begin page and the end page (excluding both begin and end)
     *
     * After calling this function all active iterators will become invalid.
     *
     * This function is not 100 percent thread safe, multiple threads deleting overlapping regions will result in a
     * corrupted log.
     *
     * @param begin The page preceeding the first deleted page
     * @param end The page succeeding the last deleted page
     */
    void erase(LogPage* begin, LogPage* end);

    PageIterator pageBegin() {
        return PageIterator(Impl::startPage());
    }

    PageIterator pageEnd() {
        return PageIterator(nullptr);
    }

    LogIterator begin() {
        return LogIterator(Impl::startPage());
    }

    ConstLogIterator begin() const {
        return ConstLogIterator(Impl::startPage());
    }

    ConstLogIterator cbegin() const {
        return begin();
    }

    LogIterator end() {
        return LogIterator(nullptr);
    }

    ConstLogIterator end() const {
        return ConstLogIterator(nullptr);
    }

    ConstLogIterator cend() const {
        return end();
    }
};

extern template class Log<UnorderedLogImpl>;
extern template class Log<OrderedLogImpl>;

} // namespace store
} // namespace tell
