#pragma once

#include "implementation.hpp"
#include <deltamain/Table.hpp>
#include <logstructured/Table.hpp>
#include <util/SnapshotDescriptor.hpp>
#include <util/TransactionImpl.hpp>

namespace tell {
namespace store {

using Storage = StoreImpl<usedImplementation>;
using Transaction = TransactionImpl<Storage>;


} // namespace store
} // namespace crossbow
