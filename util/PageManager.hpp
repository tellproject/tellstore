/*
 * (C) Copyright 2015 ETH Zurich Systems Group (http://www.systems.ethz.ch/) and others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Contributors:
 *     Markus Pilman <mpilman@inf.ethz.ch>
 *     Simon Loesing <sloesing@inf.ethz.ch>
 *     Thomas Etter <etterth@gmail.com>
 *     Kevin Bocksrocker <kevin.bocksrocker@gmail.com>
 *     Lucas Braun <braunl@inf.ethz.ch>
 */
#pragma once

#include <config.h>

#include <crossbow/allocator.hpp>
#include <crossbow/fixed_size_stack.hpp>
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
    crossbow::fixed_size_stack<void*> mPages;
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

    /**
     * Given an address that points into a random memory
     * address within a page, returns the start address
     * of this page.
     * @braunl: added as utility for colum store approaches
     */
    const char *getPageStart(const char *address) const;
};

} // namespace store
} // namespace tell
