#pragma once

#include <config.h>
#include "FixedSizeStack.hpp"

#include <crossbow/allocator.hpp>
#include <crossbow/non_copyable.hpp>

#include <cstddef>
#include <memory>

namespace tell {
namespace store {

class PageManager;

struct PageManagerDeleter {
    void operator()(PageManager* pageManager) {
        crossbow::allocator::destroy_in_order(pageManager);
    }
};

/**
* This class purpose is to store all pages
* allocated. It keeps an internal list of
* free pages. All page allocations need to
* be made through this class.
*/
class PageManager: crossbow::non_copyable, crossbow::non_movable {
private:
    void* mData;
    size_t mSize;
    FixedSizeStack<void*> mPages;
public:
    using Ptr = std::unique_ptr<PageManager, PageManagerDeleter>;

    /**
     * @brief Constructs a new page manager pointer
     *
     * The resulting page manager is allocated and destroyed within the epoch.
     */
    static PageManager::Ptr construct(size_t size) {
        return PageManager::Ptr(crossbow::allocator::construct<PageManager>(size));
    }

    /**
    * This class must not instantiated more than once!
    *
    * The constructor will allocate #size number
    * of bytes. At the moment, growing and shrinking is
    * not supported. This might be implemented later.
    *
    * \pre {#size has to be a multiplication of #PAGE_SIZE}
    */
    PageManager(size_t size);

    ~PageManager();

    void* data() {
        return mData;
    }

    size_t size() const {
        return mSize;
    }

    /**
    * Allocates a new page. It is safe to call this method
    * concurrently. It will return nullptr, if there is no
    * space left.
    */
    void* alloc();

    /**
    * Returns the given page back to the pool
    */
    void free(void* page);

    /**
    * Returns the given (already zeroed) page back to the pool
    */
    void freeEmpty(void* page);
};

} // namespace store
} // namespace tell
