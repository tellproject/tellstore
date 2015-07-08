#pragma once

#include "Record.hpp"

#include <crossbow/enum_underlying.hpp>

#include <vector>
#include <cstdint>

namespace tell {
namespace store {

class ScanQueryImpl {
public:
    virtual ~ScanQueryImpl() = default;

    virtual void process(uint64_t validFrom, uint64_t validTo, const char* data, size_t size, const Record& record) = 0;
};

/**
 * A query has the following form
 * - 8 bytes for the number of columns it has to check
 * - For each column:
 *   - 2 bytes: The column id (called field id in the Record class)
 *   - 2 bytes: The number of predicates it has on the column
 *   - 4 bytes: Padding
 *   - For each predicate:
 *     - 1 byte: The type of the predicate
 *     - 1 byte: Position in the AND-list of queries
 *     - 2 bytes: The data if its size is between 1 and 2 bytes long padding otherwise
 *     - 4 bytes: The data if its size is between 2 and 4 bytes long padding otherwise
 *     - The data if it is larger than 4 bytes or if it is variable size length (padded to 8 bytes)
 */
struct ScanQuery {
    const char* query = nullptr;

    constexpr static off_t offsetToFirstColumn() { return 8; }
    constexpr static off_t offsetToFirstPredicate() { return 8; }

    off_t offsetToNextPredicate(off_t offset, const FieldBase& f) const {
        auto type = getTypeOfPredicate(offset);
        switch (type) {
        case PredicateType::IS_NOT_NULL:
        case PredicateType::IS_NULL:
            return 8;
        default: {
            auto res = f.offsetInQuery();
            res += f.sizeOf(query + offset + res);
            res += ((res % 8 != 0) ? (8 - (res % 8)) : 0);
            return res;
        }
        }
    }

    uint64_t numberOfColumns() const {
        return *reinterpret_cast<const uint64_t*>(query);
    }

    uint16_t columnId(off_t offset) const {
        return *reinterpret_cast<const uint16_t*>(query + offset);
    }

    uint16_t predicatesCount(off_t offset) const {
        return *reinterpret_cast<const uint16_t*>(query + offset + 2);
    }

    PredicateType getTypeOfPredicate(off_t offset) const {
        return crossbow::from_underlying<PredicateType>(*reinterpret_cast<const uint8_t*>(query + offset));
    }

    uint8_t posInQuery(off_t offset) const {
        return *reinterpret_cast<const uint8_t*>(query + offset + 1);
    }

    const char* check(const char* data, std::vector<bool>& bitmap, const Record& record) const
    {
        auto numberOfCols = numberOfColumns();
        auto offset = offsetToFirstColumn();
        for (uint64_t i = 0; i < numberOfCols; ++i) {
            auto cId = columnId(offset);
            auto predCnt = predicatesCount(offset);

            bool isNull;
            FieldType fType;
            const char* field = record.data(data, cId, isNull, &fType);
            FieldBase f(fType);

            offset += offsetToFirstPredicate();
            for (decltype(predCnt) i = 0; i < predCnt; ++i) {
                auto bitmapPos = posInQuery(offset);
                if (bitmap.size() <= bitmapPos) bitmap.resize(bitmapPos + 1);
                else if (bitmap[bitmapPos]) {
                    // if one of several ORs is true, we don't need to check the others
                    offset += offsetToNextPredicate(offset, f);
                    continue;
                }
                auto type = getTypeOfPredicate(offset);

                // we need to handle NULL special
                if (isNull) {
                    bitmap[bitmapPos] = type == PredicateType::IS_NULL;
                    offset += offsetToNextPredicate(offset, f);
                    continue;
                }
                // Note: at this point we know that bitmap[bitmapPos] is false
                // we use this fact quite often: instead of using ||, we just assign
                // to the bitmap.
                // Furthermore we do know, that both values are not null
                switch (type) {
                case PredicateType::IS_NOT_NULL:
                    bitmap[bitmapPos] = true;
                case PredicateType::IS_NULL:
                    offset += offsetToNextPredicate(offset, f);
                    break;
                default:
                    offset += f.offsetInQuery();
                    bitmap[bitmapPos] = f.cmp(type, field, query + offset);
                    offset += f.sizeOf(query + offset);
                    offset += ((offset % 8 != 0) ? (8 - (offset % 8)) : 0);
                }
            }
        }
        return query + offset;
    }
};

} //namespace store
} //namespace tell
