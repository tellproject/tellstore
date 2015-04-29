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
        return reinterpret_cast<const LogEntry*>(data - LOG_ENTRY_SIZE);
    }

    /**
     * @brief Calculates the entry size in the log from the data payload size
     *
     * Adds the LogEntry class size and adds padding so the size is 8 byte aligned
     */
    static uint32_t entrySizeFromSize(uint32_t size) {
        size += LOG_ENTRY_SIZE;
        size += ((size % 8 != 0) ? (8 - (size % 8)) : 0);
        LOG_ASSERT(size % 8 == 0, "Final LogEntry size must be 8 byte padded");
        return size;
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
     * @brief The size of the data payload in this entry
     */
    uint32_t size() const {
        return (mSize.load() >> 1);
    }

    /**
     * @brief The size of the entry in the log
     *
     * @note This is not the size of the pure data payload but the size of the whole (8 byte padded) log entry.
     */
    uint32_t entrySize() const {
        return entrySizeFromSize(size());
    }

    char* data() {
        return const_cast<char*>(const_cast<const LogEntry*>(this)->data());
    }

    const char* data() const {
        return reinterpret_cast<const char*>(this) + LOG_ENTRY_SIZE;
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
     * @param size Size of the data payload the entry contains
     * @return 0 if the log entry was successfully acquired else the complete entry size of this log entry
     */
    uint32_t tryAcquire(uint32_t size);

    /// Size of the data payload this entry contains shifted to bits 1-31, bit 0 indicates if the entry was sealed
    /// (if 0) or not (if 1)
    std::atomic<uint32_t> mSize;
};

/**
 * @brief A single page in the log containing multiple LogEntry's
 *
 * A Log-Page has the following form:
 *
 * ---------------------------------------------------------------------------------------------------
 * | next (8 bytes) | offset (4 bytes) | padding (8 bytes) | entry | entry | ... | padding (4 bytes) |
 * ---------------------------------------------------------------------------------------------------
 *
 * Entries require 4 bytes of space followed by the associated data segment. To keep this data segment 8 byte aligned
 * the log pads the entries to a multiple of 8 bytes and writes the LogEntries at offset 4. Any subsequent entries are
 * aligned with offset 4 due to the padding.
 */
class LogPage : NonCopyable, NonMovable {
public:
    /// Size of the LogPage data structure
    static constexpr size_t LOG_HEADER_SIZE = 20;

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
            mPos += entry->entrySize();
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

        reference operator*() const {
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
        return const_cast<char*>(const_cast<const LogPage*>(this)->data());
    }

    const char* data() const {
        return reinterpret_cast<const char*>(this) + LOG_HEADER_SIZE;
    }

    /**
     * @brief Appends a new entry to this log page
     *
     * @param size Size of the data payload of the new entry
     * @return Pointer to allocated LogEntry or nullptr if unable to allocate the entry in this page
     */
    LogEntry* append(uint32_t size);

    /**
     * @brief Appends a new entry to this log page
     *
     * @param size Size of the data payload of the new entry
     * @param entrySize Complete 8 byte padded size of the new entry
     * @return Pointer to allocated LogEntry or nullptr if unable to allocate the entry in this page
     */
    LogEntry* appendEntry(uint32_t size, uint32_t entrySize);

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
        return (mOffset.load() >> 1);
    }

    /**
     * @brief Atomically retrieves the offset and whether the page is sealed
     */
    std::pair<uint32_t, bool> offsetAndSealed() const {
        auto offset = mOffset.load();
        return std::make_pair(offset >> 1, (offset & 0x1u) == 0);
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
        LOG_ASSERT((offset >> 31) == 0, "MSB has to be zero");
        auto o = (offset << 1);
        auto exp = (o| 0x1u);
        return mOffset.compare_exchange_strong(exp, o);
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
public:
    /**
     * @brief Iterator base class for iterating over all entries in the log
     *
     * Provides common functionality shared between all Log iterator implementations.
     */
    template<class EntryType>
    class BaseLogIterator {
    public:
        static constexpr bool is_const_iterator = std::is_const<typename std::remove_pointer<EntryType>::type>::value;
        using reference = typename std::conditional<is_const_iterator, const LogEntry&, LogEntry&>::type;
        using pointer = typename std::conditional<is_const_iterator, const LogEntry*, LogEntry*>::type;

        BaseLogIterator(EntryType page, uint32_t pos)
                : mPage(page),
                  mCachedOffset(mPage->offsetAndSealed()),
                  mPos(pos) {
        }

        EntryType page() const {
            return mPage;
        }

        uint32_t offset() const {
            return mPos;
        }

        reference operator*() const {
            return *operator->();
        }

        pointer operator->() const {
            return reinterpret_cast<pointer>(mPage->data() + mPos);
        }

    protected:
        /**
         * @brief Advance to the next entry on the current page
         *
         * @return True if the pointer was successfully advanced, false if we already reached the end
         */
        bool advanceEntry() {
            LOG_ASSERT(mPos <= mCachedOffset.first, "Current position is larger than the page offset");

            // Check if we already point past the last element
            if (mPos == mCachedOffset.first) {
                return false;
            }

            auto entry = reinterpret_cast<pointer>(mPage->data() + mPos);
            mPos += entry->entrySize();
            return true;
        }

        /**
         * @brief Advance to the next page
         *
         * @return True if the page was successfully advanced, false if the next page does not exist
         */
        bool advancePage() {
            // Only advance to the next page if it is valid
            auto next = mPage->next().load();
            if (!next) {
                return false;
            }

            mPage = next;
            mCachedOffset = next->offsetAndSealed();
            mPos = 0;
            return true;
        }

        /**
         * @brief Helper function checking the BaseLogIterator for equality
         */
        bool compare(const BaseLogIterator<EntryType>& rhs) const {
            return ((mPage == rhs.mPage) && (mPos == rhs.mPos));
        }

        /// Current page this iterator operates on
        EntryType mPage;

        /// Cached offset and sealed state of the page - by caching this value we don't have to read the offset from the
        /// page - with a potential cache miss - everytime we increment the iterator.
        std::pair<uint32_t, bool> mCachedOffset;

        /// Current offset the iterator is pointing to
        uint32_t mPos;
    };

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
    /**
     * @brief Iterator for iterating over all entries in the log
     *
     * Iterates over each page's elements from oldest to newest element written from the head (newest page) to the tail
     * (oldest page). The iterator is either pointing to a valid element or the invalid position at the tail of the log.
     */
    template<class EntryType>
    class UnorderedLogIteratorImpl : public BaseLogImpl::BaseLogIterator<EntryType> {
    public:
        using Base = BaseLogImpl::BaseLogIterator<EntryType>;

        UnorderedLogIteratorImpl(EntryType page, uint32_t pos)
                : Base(page, pos) {
            // Skip empty pages
            while (Base::mCachedOffset.first == 0 && Base::advancePage()) {}
        }

        UnorderedLogIteratorImpl(EntryType page)
                : UnorderedLogIteratorImpl(page, 0) {
        }

        UnorderedLogIteratorImpl<EntryType>& operator++() {
            Base::advanceEntry();

            // Advance to the next page if the iterator points to a invalid positon (i.e. pos == offset) while skipping
            // any empty pages (pos == offset == 0)
            while (Base::mPos == Base::mCachedOffset.first && Base::advancePage()) {}

            return *this;
        }

        UnorderedLogIteratorImpl<EntryType> operator++(int) {
            UnorderedLogIteratorImpl<EntryType> result(*this);
            operator++();
            return result;
        }

        bool operator==(UnorderedLogIteratorImpl<EntryType>& rhs) const {
            return Base::compare(rhs);
        }

        bool operator!=(UnorderedLogIteratorImpl<EntryType>& rhs) const {
            return !operator==(rhs);
        }
    };

    using LogIterator = UnorderedLogIteratorImpl<LogPage*>;
    using ConstLogIterator = UnorderedLogIteratorImpl<const LogPage*>;

    LogPage* head() {
        return mHead.load().writeHead;
    }

    LogPage* tail() {
        return mTail.load();
    }

    /**
     * @brief Appends the given pages to the log
     *
     * All pages except the begin page must be sealed.
     *
     * @param begin The first page to append
     * @param end The last page to append
     */
    void appendPage(LogPage* begin, LogPage* end);

    void appendPage(LogPage* page) {
        appendPage(page, page);
    }

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

protected:
    UnorderedLogImpl(PageManager& pageManager);

    LogEntry* appendEntry(uint32_t size, uint32_t entrySize);

    /**
     * @brief Page iteration starts from the head
     */
    LogPage* pageBegin() {
        return const_cast<LogPage*>(const_cast<const UnorderedLogImpl*>(this)->pageBegin());
    }

    const LogPage* pageBegin() const {
        auto head = mHead.load();
        return (head.appendHead ? head.appendHead : head.writeHead);
    }

    /**
     * @brief Page iteration stops at the nullptr page
     */
    LogPage* pageEnd() {
        return const_cast<LogPage*>(const_cast<const UnorderedLogImpl*>(this)->pageEnd());
    }

    const LogPage* pageEnd() const {
        return nullptr;
    }

    /**
     * @brief Log iteration starts from the head
     */
    LogPage* entryBegin() {
        return const_cast<LogPage*>(const_cast<const UnorderedLogImpl*>(this)->entryBegin());
    }

    const LogPage* entryBegin() const {
        auto head = mHead.load();
        return (head.appendHead ? head.appendHead : head.writeHead);
    }

    /**
     * @brief Entry iteration stops at the tail page
     */
    LogPage* entryEnd() {
        return const_cast<LogPage*>(const_cast<const UnorderedLogImpl*>(this)->entryEnd());
    }

    const LogPage* entryEnd() const {
        return mTail.load();
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

    std::atomic<LogPage*> mTail;
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
    /**
     * @brief Iterator for iterating over all entries in the log
     *
     * Iterates through the log elements from oldest written to newest written. The iterator is either pointing to a
     * valid element or the invalid position at the head of the log. The invalid position can change as new entries are
     * appended at the head of the log. As a consequence an invalid iterator might become valid again when incrementing
     * or comparing the iterator.
     */
    template<class EntryType>
    class OrderedLogIteratorImpl : public BaseLogImpl::BaseLogIterator<EntryType> {
    public:
        using Base = BaseLogImpl::BaseLogIterator<EntryType>;

        OrderedLogIteratorImpl(EntryType page, uint32_t pos)
                : Base(page, pos) {
        }

        OrderedLogIteratorImpl(EntryType page)
                : OrderedLogIteratorImpl(page, 0) {
        }

        OrderedLogIteratorImpl<EntryType>& operator++() {
            // We might have to update the cached offset
            // This avoids a race condition when the end iterator is acquired after the begin iterator and they both
            // point to the same (head) page, the end iterator could store a larger offset as the begin iterator and as
            // such the begin iterator would skip the page before reaching the end pointer
            maybeUpdateCachedOffset();

            // Advance the actual iterator
            Base::advanceEntry();

            // Advance to the next page if the iterator points to a invalid positon (i.e. pos == offset)
            maybeAdvancePage();

            return *this;
        }

        OrderedLogIteratorImpl<EntryType> operator++(int) {
            OrderedLogIteratorImpl<EntryType> result(*this);
            operator++();
            return result;
        }

        bool operator==(OrderedLogIteratorImpl<EntryType>& rhs) {
            if (Base::compare(rhs)) {
                return true;
            }

            // The iterators could point to old invalid positions (i.e. to the offset of an old head page)
            // We might have to update the cached offsets and advance to the next page if the iterators point to a
            // invalid positon (i.e. pos == offset)
            maybeUpdateCachedOffset();
            maybeAdvancePage();

            rhs.maybeUpdateCachedOffset();
            rhs.maybeAdvancePage();

            // Note: One might think there is a race condition between lhs.maybeAdvanceToNextPage() and
            // rhs.maybeAdvanceToNextPage() - i.e. rhs advanced to the next page while lhs did not because when
            // advancing lhs its next pointer was still null. This can not happen as both of them would previously point
            // to the same invalid position on the same (head) page and as such the first comparison already evaluated
            // to true (the only invalid position is at the unique end of the log)

            return Base::compare(rhs);
        }

        bool operator!=(OrderedLogIteratorImpl<EntryType>& rhs) {
            return !operator==(rhs);
        }

    private:
        void maybeUpdateCachedOffset() {
            // Only update the cached offset if it is not yet sealed and we reached the end of the page
            if (!Base::mCachedOffset.second && Base::mPos == Base::mCachedOffset.first) {
                Base::mCachedOffset = Base::mPage->offsetAndSealed();
            }
        }

        void maybeAdvancePage() {
            // We only advance to the next page if the page is sealed and we reached the end of the page
            if (!Base::mCachedOffset.second || Base::mPos != Base::mCachedOffset.first) {
                return;
            }

            // There could be a race condition where the page was already sealed but the next page is not yet set
            Base::advancePage();
        }
    };

    using LogIterator = OrderedLogIteratorImpl<LogPage*>;
    using ConstLogIterator = OrderedLogIteratorImpl<const LogPage*>;

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

    LogEntry* appendEntry(uint32_t size, uint32_t entrySize);

    /**
     * @brief Page iteration starts from the tail
     */
    LogPage* pageBegin() {
        return const_cast<LogPage*>(const_cast<const OrderedLogImpl*>(this)->pageBegin());
    }

    const LogPage* pageBegin() const {
        return mTail.load();
    }

    /**
     * @brief Page iteration ends with a nullptr
     */
    LogPage* pageEnd() {
        return const_cast<LogPage*>(const_cast<const OrderedLogImpl*>(this)->pageEnd());
    }

    const LogPage* pageEnd() const {
        return nullptr;
    }

    /**
     * @brief Entry iteration starts from the tail
     */
    LogPage* entryBegin() {
        return const_cast<LogPage*>(const_cast<const OrderedLogImpl*>(this)->entryBegin());
    }

    const LogPage* entryBegin() const {
        return mTail.load();
    }

    /**
     * @brief Entry iteration ends at the current head
     */
    LogPage* entryEnd() {
        return const_cast<LogPage*>(const_cast<const OrderedLogImpl*>(this)->entryEnd());
    }

    const LogPage* entryEnd() const {
        return mHead.load();
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

        reference operator*() const {
            return *operator->();
        }

        pointer operator->() const {
            return mPage;
        }

    private:
        /// Current page this iterator is pointing to
        LogPage* mPage;
    };

    using LogIterator = typename Impl::LogIterator;
    using ConstLogIterator = typename Impl::ConstLogIterator;

    Log(PageManager& pageManager)
            : Impl(pageManager) {
    }

    ~Log();

    /**
     * @brief Appends a new entry to the log
     *
     * @param size Size of the data payload of the new entry
     * @return Pointer to allocated LogEntry or nullptr if unable to allocate the entry
     */
    LogEntry* append(uint32_t size);

    PageIterator pageBegin() {
        return PageIterator(Impl::pageBegin());
    }

    PageIterator pageEnd() {
        return PageIterator(Impl::pageEnd());
    }

    LogIterator begin() {
        return LogIterator(Impl::entryBegin());
    }

    ConstLogIterator begin() const {
        return cbegin();
    }

    ConstLogIterator cbegin() const {
        return ConstLogIterator(Impl::entryBegin());
    }

    LogIterator end() {
        auto page = Impl::entryEnd();
        return LogIterator(page, page->offset());
    }

    ConstLogIterator end() const {
        return cend();
    }

    ConstLogIterator cend() const {
        auto page = Impl::entryEnd();
        return ConstLogIterator(page, page->offset());
    }
};

extern template class Log<UnorderedLogImpl>;
extern template class Log<OrderedLogImpl>;

} // namespace store
} // namespace tell
