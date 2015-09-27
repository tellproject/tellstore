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
#include "sparsehash/dense_hash_map"

namespace tell {
namespace store {

template <class Key, class T,
          class Alloc = google::libc_allocator_with_realloc<std::pair<const Key, T> > >
class DenseHashMap : public google::dense_hash_map < Key, T, std::hash<Key>, std::equal_to<Key>, Alloc>{
public:
    typedef google::dense_hash_map < Key, T, std::hash<Key>, std::equal_to<Key>, Alloc> map_type;
    
    DenseHashMap(typename map_type::size_type expected_max_items_in_table = 0,
                 const typename map_type::hasher& hf = typename map_type::hasher(),
                 const typename map_type::key_equal& eql = typename map_type::key_equal(),
                 const typename map_type::allocator_type& alloc = typename map_type::allocator_type())
        : map_type(expected_max_items_in_table, hf, eql, alloc)
    {
        this->set_empty_key(Key::empty());
        this->set_deleted_key(Key::deleted());
    }
};

}
}
