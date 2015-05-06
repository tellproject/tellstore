#pragma once
#include <cstdint>
#include <config.h>

namespace tell {
namespace store {

class Record;

template<Implementation impl>
class IteratorEntry_t;

using IteratorEntry = IteratorEntry_t<usedImplementation>;


} // namespace store
} // namespace tell
