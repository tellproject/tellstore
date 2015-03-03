#pragma once

#include "implementation.hpp"
#include <util/TransactionImpl.hpp>
#include "deltamainrewrite/dmrewrite.hpp"
#include <util/SnapshotDescriptor.hpp>

namespace tell {
namespace store {

using Storage = StoreImpl<usedImplementation>;
using Transaction = TransactionImpl<Storage>;


} // namespace store
} // namespace crossbow
