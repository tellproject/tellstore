#include "dmrewrite.hpp"

namespace tell {
namespace store {
namespace dmrewrite {


Table::Table(PageManager& pageManager, Schema const& schema)
        : mPageManager(pageManager)
        , mSchema(schema)
        , mLog(mPageManager)
        , hashMap(mPageManager)
{
}

void GarbageCollector::run(const std::vector<Table*>& tables) {
}

} // namespace tell
StoreImpl<Implementation::DELTA_MAIN_REWRITE>::StoreImpl(const StorageConfig& config)
        : pageManager(config.totalMemory)
        , tableManager(config, gc)
{
}

StoreImpl<Implementation::DELTA_MAIN_REWRITE>::StoreImpl(const StorageConfig& config, size_t totalMem)
        : pageManager(totalMem)
        , tableManager(config, gc)
{
}

} // namespace store
} // namespace dmrewrite
