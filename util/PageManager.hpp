#pragma once

#include <config.h>
#include <cstddef>
#include "FixedSizeStack.hpp"
#include "NonCopyable.hpp"

namespace tell {
namespace store {

/**
* This class purpose is to store all pages
* allocated. It keeps an internal list of
* free pages. All page allocations need to
* be made through this class.
*/
class PageManager: NonCopyable {
private:
    void* mData;
    size_t mSize;
    FixedSizeStack<void*> mPages;
public:
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
