#pragma once

#include <util/Record.hpp>
#include <util/chunk_allocator.hpp>
#include <util/LogOperations.hpp>

#include "DMLog.hpp"

namespace tell {
namespace store {

struct LogEntry;

namespace dmrewrite {

/**
* A record in this approach has the following form:
* - 8 bytes: pointer to an entry on the log (atomic)
* - A multi-version record
*/
struct DMRecord {
    MultiVersionRecord multiVersionRecord;
    Record record;

    DMRecord(const Schema& schema);

    const char* getRecordData(const SnapshotDescriptor& snapshot,
                              const char* data,
                              bool& isNewest,
                              DMLogEntry** next = nullptr) const;
    bool needGCWork(const char* data, uint64_t minVersion) const;
    DMLogEntry* getNewest(const char* data) const;
    bool setNewest(DMLogEntry* old, DMLogEntry* n, const char* data);

    /**
    * This function will compact and merge the given tuple. It will return the following:
    * - The size of the old tuple, or 0 if the tuple can be deleted
    * - A pinter to an allocated chunk, if the tuple did not have enough room in the
    *   provided space
    */
    template<class Allocator>
    std::pair<size_t, char*> compactAndMerge(char* data, uint64_t minVersion, Allocator& allocator) const;
};

template<class Allocator>
std::pair<size_t, char*> DMRecord::compactAndMerge(char* data, uint64_t minVersion, Allocator& allocator) const {
    using allocator_t = typename Allocator::template rebind<char>;
    allocator_t alloc(allocator);
    uint32_t oldSize = multiVersionRecord.compact(data + 8, minVersion);
    //uint32_t tupleSize = multiVersionRecord.getSize(data + 8);
    if (oldSize == 0u) {
        return std::make_pair(0, nullptr);
    }
    std::pair<size_t, char*> res = std::make_pair(oldSize, nullptr);
    DMLogEntry* logEntry = getNewest(data);
    if (logEntry == nullptr) {
        return res;
    }
    auto getDataOffset = [](size_t numVersions) -> size_t {
        return size_t(8 + 8) + 8*numVersions + 4*numVersions + (numVersions % 2 == 0 ? 0 : 4);
    };
    // there are updates on this element. In that case, it could be, that we can safely
    // delete the tuple and create a new one from the updates
    if (multiVersionRecord.getBiggestVersion(data + 8) < minVersion) {
        // We can rewrite the whole tuple
        std::vector<std::pair<uint64_t, const char*>, typename Allocator::template rebind<std::pair<uint64_t, const char*>>> versions(allocator);
        const DMLogEntry* current = logEntry;
        uint32_t recordsSize = 0u;
        while (current) {
            auto recVersion = LoggedOperation::getVersion(current->data());
            auto recData = LoggedOperation::getRecord(current->data());
            versions.push_back(std::make_pair(recVersion, recData));
            recordsSize += record.getSize(recData);
            current = LoggedOperation::getPrevious(current->data());
        }
        size_t recOffset = getDataOffset(versions.size());
        res.first = recOffset + recordsSize;
        assert(res.second == nullptr);
        auto ptr = data + 8;
        if (res.first > oldSize) {
            // we need to allocate a block, since we ran out of space here
            ptr = alloc.allocate(res.first);
            res.second = ptr;
            memcpy(ptr, data, 8);
            memcpy(res.second + 8, data + 8, multiVersionRecord.getSize(data + 8));
        }
        *reinterpret_cast<uint32_t*>(ptr) = uint32_t(res.first);
        *reinterpret_cast<uint32_t*>(ptr + 4) = uint32_t(versions.size());
        uint64_t* versionsPtr = reinterpret_cast<uint64_t*>(ptr + 8);
        uint32_t* offsets = reinterpret_cast<uint32_t*>(ptr + 8 + 8*versions.size());
        for (uint32_t i = 0u; i < versions.size(); ++i) {
            //auto p = versions[i];
            versionsPtr[i] = versions[i].first;
            offsets[i] = uint32_t(recOffset);
            auto rs = record.getSize(versions[i].second);
            memcpy(data + recOffset, versions[i].second, rs);
            recOffset += rs;
        }
    } else {
        std::vector<std::pair<uint64_t, const char*>,
            typename Allocator::template rebind<std::pair<uint64_t, const char*>>> newVersions(allocator);
        const DMLogEntry* current = logEntry;
        uint32_t recordsSize = 0u;
        while (current) {
            auto recVersion = LoggedOperation::getVersion(current->data());
            auto recData = LoggedOperation::getRecord(current->data());
            newVersions.push_back(std::make_pair(recVersion, recData));
            recordsSize += record.getSize(recData);
            current = LoggedOperation::getPrevious(current->data());
        }
        auto oldNumVersions = multiVersionRecord.getNumberOfVersions(data + 8);
        auto numVersions = oldNumVersions + newVersions.size();
        uint32_t oldSize = multiVersionRecord.getSize(data + 8);
        uint32_t newSize = oldSize;
        newSize += recordsSize;
        newSize += newVersions.size() * 8 + newVersions.size() * 4;
        // make sure that we have the correct alignment here
        if (numVersions % 2 == 0 && oldNumVersions % 2 == 1) {
            newSize -= 4;
        } else if (numVersions % 2 == 1 && oldNumVersions % 2 == 0) {
            newSize += 4;
        }
        res.first = newSize;
        auto ptr = data + 8;
        if (res.first > oldSize) {
            // the tuple got too big and will not fit into the space anymore
            res.second = alloc.allocate(res.first + 8);
            ptr = res.second + 8;
            memcpy(res.second, data, 8);
            memcpy(res.second + 8, data + 8, multiVersionRecord.getSize(data + 8));
        }
        // We can make use of the fact, that all versions in the log will be
        // newer than the newest version in the record
        auto oldDataOffset = getDataOffset(oldNumVersions);
        auto dataOffset = getDataOffset(numVersions);
        // First we need to move the records at the end of the buffer
        memmove(ptr + oldDataOffset, ptr + dataOffset, oldSize - oldDataOffset);
        // then, we need to move the offset array
        memmove(ptr + 8+8*oldNumVersions, ptr + 8+8*numVersions, 4*oldNumVersions);
        // Write back the new size and number of versions
        *reinterpret_cast<uint32_t*>(ptr) = newSize;
        *reinterpret_cast<uint32_t*>(ptr + 4) = numVersions;
        uint64_t* versions = reinterpret_cast<uint64_t*>(ptr + 8);
        uint32_t* offsets = reinterpret_cast<uint32_t*>(ptr + 8 + 8*numVersions);
        char* recordPtr = ptr + dataOffset + (oldSize - oldDataOffset);
        // correct the old offsets
        for (size_t i = 0u; i < oldNumVersions; ++i) {
            offsets[i] += dataOffset - oldDataOffset;
        }
        // write the new versions
        for (size_t i = oldNumVersions; i < numVersions; ++i) {
            versions[i] = newVersions[i].first;
            memcpy(recordPtr, newVersions[i].second, newVersions[i].first);
            recordPtr += newVersions[i].first;
        }
    }
    return res;
}

using default_alloc = crossbow::copy_allocator<std::pair<uint64_t, char*>>;
extern template std::pair<size_t, char*> DMRecord::compactAndMerge(char* data,
                                                                   uint64_t minVersion,
                                                                   default_alloc& allocator) const;

}
}
}
