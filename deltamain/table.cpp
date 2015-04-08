#include "table.hpp"
#include "Record.hpp"

namespace tell {
namespace store {
namespace deltamain {

Table::Table(PageManager& pageManager, const Schema& schema)
    : mPageManager(pageManager)
    , mSchema(schema)
    , mHashTable(mPageManager)
    , mInsertLog(mPageManager)
    , mUpdateLog(mPageManager)
{}

bool Table::get(uint64_t key,
                size_t& size,
                const char*& data,
                const SnapshotDescriptor& snapshot,
                bool& isNewest) const {
    auto ptr = mHashTable.get(key);
    if (ptr) {
        CDMRecord rec(reinterpret_cast<char*>(ptr));
        bool wasDeleted;
        data = rec.data(snapshot, isNewest, size, &wasDeleted);
        return !wasDeleted;
    }
    // in this case we need to scan through the insert log
    for (auto iter = mInsertLog.begin(); iter != mInsertLog.end(); ++iter) {
        if (!iter->sealed()) continue;
        CDMRecord rec(iter->data());
        if (rec.key() == key) {
            bool wasDeleted;
            data = rec.data(snapshot, isNewest, size, &wasDeleted);
            return !wasDeleted;
        }
    }
    // in this case the tuple does not exist
    return false;
}

void GarbageCollector::run(const std::vector<Table*>& tables, uint64_t minVersion) {
    for (auto table : tables) {
        table->runGC(minVersion);
    }
}

} // namespace deltamain


StoreImpl<Implementation::DELTA_MAIN_REWRITE>::StoreImpl(const StorageConfig& config)
    : pageManager(config.totalMemory), tableManager(pageManager, config, gc, commitManager) {
}

StoreImpl<Implementation::DELTA_MAIN_REWRITE>::StoreImpl(const StorageConfig& config, size_t totalMem)
    : pageManager(totalMem), tableManager(pageManager, config, gc, commitManager) {
}

} // namespace store
} // namespace tell

