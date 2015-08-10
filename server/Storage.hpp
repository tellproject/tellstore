#pragma once

#include <config.h>

#include <util/StoreImpl.hpp>

#if defined USE_DELTA_MAIN_REWRITE
#include <deltamain/DeltaMainRewriteStore.hpp>
#elif defined USE_LOGSTRUCTURED_MEMORY
#include <logstructured/LogstructuredMemoryStore.hpp>
#else
#error "Unknown implementation"
#endif

namespace tell {
namespace store {

using Storage = StoreImpl<usedImplementation>;

} // namespace store
} // namespace tell
