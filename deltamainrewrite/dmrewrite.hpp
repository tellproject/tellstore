#pragma once

#include <crossbow/string.hpp>
#include <config.h>
#include <implementation.hpp>
#include <util/Record.hpp>
#include <util/PageManager.hpp>
#include <util/TableManager.hpp>
#include <util/Log.hpp>
#include <util/CuckooHash.hpp>

namespace tell {
namespace store {

namespace dmrewrite {

class Table {
    PageManager& mPageManager;
    Schema mSchema;
    Log mLog;
    CuckooTable hashMap;
public:
    Table(PageManager& pageManager, const Schema& schema);
};

class GarbageCollector {
public:
    void run(const std::vector<Table*>& tables);
};

}

template<>
struct StoreImpl<Implementation::DELTA_MAIN_REWRITE>
{
    using Table = dmrewrite::Table;
    using GC = dmrewrite::GarbageCollector;
    PageManager pageManager;
    GC gc;
    TableManager<Table, GC> tableManager;
    StoreImpl(const StorageConfig& config);
    StoreImpl(const StorageConfig& config, size_t totalMem);
};

} // namespace store
} // namespace tell
