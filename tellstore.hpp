#pragma once

#include "implementation.hpp"
#include "deltamainrewrite/dmrewrite.hpp"

namespace tell {
namespace store {

class Store {
private:
    impl::StoreImpl<impl::usedImplementation> impl;

};


} // namespace store
} // namespace crossbow
