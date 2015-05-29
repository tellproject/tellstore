#pragma once

#include "Record.hpp"
#include "helper.hpp"

#include <vector>
#include <cstdint>

namespace tell {
namespace store {

class ScanQueryImpl {
};

/**
 * A query has the following form
 * - 8 bytes for the number of columns it has to check
 * - For each column:
 *   - 2 bytes: The column id (called field id in the Record class)
 *   - 2 bytes: The number of predicates it has on the coulmn
 *   - 4 bytes: Padding
 *   - For each column:
 *     - 2 bytes: The column id
 *     - 2 bytes: the number of predicates it needs to check
 *     - 4 bytes: padding
 *     - For each predicate:
 *       - 1 byte: The type of the predicate
 *       - 1 byte: Position in the AND-list of queries
 *       - 2 bytes: The data if its size is between 1 and 2 bytes long
 *                  padding otherwise
 *       - 4 bytes: The data if its size is between 2 and 4 bytes long
 *                  padding otherwise
 *       - The data if it is larger than 8 bytes or if it is variable
 *         size length
 */
struct ScanQuery {
    char* query = nullptr;

    constexpr static off_t offsetToFirstColumn() { return 8; }
    constexpr static off_t offsetToFirstPredicate() { return 8; }

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
        return from_underlying<PredicateType>(*reinterpret_cast<const uint8_t*>(query + offset));
    }

    uint8_t posInQuery(off_t offset) const {
        return *reinterpret_cast<const uint8_t*>(query + offset + 1);
    }

    char* check(const char* data, std::vector<bool>& bitmap, const Record& record) const
    {
        auto numberOfCols = numberOfColumns();
        auto offset = offsetToFirstColumn();
        for (uint64_t i = 0; i < numberOfCols; ++i) {
            auto cId = columnId(offset);
            auto predCnt = predicatesCount(offset);
            offset += offsetToFirstPredicate();
            for (decltype(predCnt) i = 0; i < predCnt; ++i) {
                auto type = getTypeOfPredicate(offset);
                auto bitmapPos = posInQuery(offset);
                if (bitmap.size() <= bitmapPos) bitmap.resize(bitmapPos + 1);
                else if (bitmap[bitmapPos]) continue; // if one of several ORs is true, we don't need to check the others
                bool isNull;
                FieldType fType;
                const char* field = record.data(data, cId, isNull, &fType);
                if (isNull) {
                    // we need to handle NULL special
                    bitmap[bitmapPos] = type == PredicateType::IS_NULL;
                    offset += 6;
                    continue;
                }
                // Note: at this point we know that bitmap[bitmapPos] is false
                // we use this fact quite often: instead of using ||, we just assign
                // to the bitmap.
                // Furthermore we do know, that both values are not null
                FieldBase f(fType);
                switch (type) {
                case PredicateType::IS_NOT_NULL:
                    bitmap[bitmapPos] = true;
                case PredicateType::IS_NULL:
                    offset += 6;
                    break;
                default:
                    offset += f.offsetInQuery();
                    bitmap[bitmapPos] = f.cmp(type, field, query + offset);
                    offset += f.sizeOf(query + offset);
                }
            }
        }
        return query + offset;
    }
};

} //namespace store
} //namespace tell
