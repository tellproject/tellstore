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

#include <atomic>
#include <config.h>
#include <functional>
#include <random>
#include <memory.h>
#include "PageManager.hpp"
#include "functional.hpp"

#include <crossbow/allocator.hpp>

namespace tell {
namespace store {

class Modifier;

/**
* This is a simple hash table that uses
* Cuckoo hashing for efficient key lookup.
*
* This hash table allows for concurrent
* lookup, but writing has to be done single
* threaded. In this COW is used, to modify
* the table. As soon as the modification
* is done, a CAS operation is used to swap
* the new with the old version.
*/
class CuckooTable {
private:
    static constexpr size_t ENTRIES_PER_PAGE = PageWrapper::ENTRIES_PER_PAGE;
    using PageT = PageWrapper*;
    using EntryT = typename PageWrapper::EntryT;
    PageManager& mPageManager;
    std::vector<PageT> mPages;
    cuckoo_hash_function hash1;
    cuckoo_hash_function hash2;
    cuckoo_hash_function hash3;
    size_t mSize;
public:
    CuckooTable(PageManager& pageManager);
    ~CuckooTable();

    /**
     * This method should be only used on
     * Shutdown: it will destroy the table,
     * therefore the caller has to know, that
     * it does not get used anymore.
     *
     * Usually the CuckooHashMap works as follow:
     * It is read-only and the Modifier does a COW
     * and will delete unused data. If the user wants
     * to delete the table itself, she has to
     * do it with this method - deleting an instance
     * of CuckooTable does not delete any data.
     */
    void destroy();

    friend class Modifier;
    friend class crossbow::allocator;

private:
    CuckooTable(PageManager& pageManager,
                std::vector<PageT>&& pages,
                cuckoo_hash_function hash1,
                cuckoo_hash_function hash2,
                cuckoo_hash_function hash3,
                size_t size);

public:
    void* get(uint64_t key) const;

    Modifier modifier();

    size_t capacity() const;

private: // helper functions
    const EntryT& at(unsigned h, size_t idx) const;
};

class Modifier {
    friend class CuckooTable;
    friend class crossbow::allocator;

    using PageT = typename CuckooTable::PageT;
    using EntryT = typename CuckooTable::EntryT;
    static constexpr size_t ENTRIES_PER_PAGE = CuckooTable::ENTRIES_PER_PAGE;
private:
    CuckooTable& mTable;
    std::vector<bool> pageWasModified;
    mutable std::vector<PageT> mPages;
    cuckoo_hash_function hash1;
    cuckoo_hash_function hash2;
    cuckoo_hash_function hash3;
    size_t mSize;
    std::vector<PageT> mToDelete;
private:
    Modifier(CuckooTable& table)
        : mTable(table),
          pageWasModified(table.mPages.size(), false),
          mPages(table.mPages),
          hash1(table.hash1),
          hash2(table.hash2),
          hash3(table.hash3),
          mSize(table.mSize)
    {
    }

public:
    ~Modifier();
    CuckooTable* done() const;

    /**
    * inserts or replaces a value. Will return true iff the key
    * did exist before in the hash table
    */
    bool insert(uint64_t key, void* value, bool replace = false);
    void* get(uint64_t key) const;

    bool remove(uint64_t key);

    EntryT& at(unsigned h, size_t idx, size_t& pageIdx);
    const EntryT& at(unsigned h, size_t idx, size_t& pageIdx) const;

    size_t capacity() const;
    size_t size() const;

    bool cow(unsigned h, size_t idx);

    void rehash();

    void resize();
};

} // namespace store
} // namespace tell
