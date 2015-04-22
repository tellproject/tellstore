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
