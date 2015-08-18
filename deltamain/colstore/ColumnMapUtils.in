/**
 *************************************************************************
 * The following convenience functions and macros are used to get pointers
 * to items of interest within the colum-oriented page.
 */

/**
 * Given a reference to table, computes the beginning of the page (basePtr),
 * the total number of records in this page (recordCount) and the index of the
 * current record within the page (return value).
 */
inline uint32_t getBaseKnowledge(const char* data, const Table *table, const char *&basePtr, uint32_t &recordCount) {
    LOG_ASSERT(table != nullptr, "table ptr must be set to a non-NULL value!");
    basePtr = table->pageManager()->getPageStart(data);
    recordCount = *(reinterpret_cast<const uint32_t*>(basePtr));
    return (reinterpret_cast<uint64_t>(data-2-8-reinterpret_cast<uint64_t>(basePtr)) / 16);
}

/**
 * As we always use computeBaseKnowledge in the same way, we have a macro for that.
 */
#define COMPUTE_BASE_KNOWLEDGE(data, table) const char *basePtr; \
    uint32_t recordCount; \
    uint32_t index; \
    index = getBaseKnowledge(data, table, basePtr, recordCount);

inline const uint32_t getRecordCount(const char *basePtr) {
    return *(reinterpret_cast<uint32_t*>(const_cast<char *>(basePtr)));
}

inline const uint32_t getCountHat(uint32_t recordCount) {
    return 2 * ((recordCount + 1) / 2);
}

inline const char *getKeyVersionPtrAt(const uint32_t index, const char * basePtr) {
    return basePtr + 8 + (index*16);
}

inline const uint64_t *getKeyAt(const uint32_t index, const char * basePtr) {
    return reinterpret_cast<const uint64_t*>(getKeyVersionPtrAt(index, basePtr));
}

inline const uint64_t *getVersionAt(const uint32_t index, const char * basePtr) {
    return reinterpret_cast<const uint64_t*>(getKeyVersionPtrAt(index, basePtr) + 8);
}

inline const char *getNewestPtrAt(const uint32_t index, const char * basePtr, const uint32_t recordCount) {
    return basePtr + 8 + (recordCount*16) + (index*8);

}

inline size_t getNullBitMapSize(const Table *table) {
    if (table->schema().allNotNull())
        return 0;
    else
        return (table->getNumberOfFixedSizedFields() + table->getNumberOfVarSizedFields() + 7) / 8;
}

inline const char *getNullBitMapAt(const uint32_t index, const char * basePtr, const uint32_t recordCount, const size_t nullBitMapSize) {
    return basePtr + 8 + (recordCount*24) + (index*nullBitMapSize);
}

inline const int32_t *getVarsizedLenghtAt(const uint32_t index, const char * basePtr, const uint32_t recordCount, const size_t nullBitMapSize) {
    return reinterpret_cast<const int32_t*>(basePtr + 8 + (recordCount*(24 + nullBitMapSize)) + (index*4));
}

inline char *getColumnNAt(const Table *table, const uint32_t N, const uint32_t index, const char * basePtr, const uint32_t recordCount, const size_t nullBitMapSize) {
    auto countHat = getCountHat(recordCount);
    // end of var-sized = beginning of fixed-sized columns
    char *res = const_cast<char *>(reinterpret_cast<const char *>(getVarsizedLenghtAt(countHat, basePtr, recordCount, nullBitMapSize)));
    const uint32_t fixedSizedFields= table->getNumberOfFixedSizedFields();
    uint32_t offset = 0;
    if (N > fixedSizedFields)   // we deal with a var-sized field, but not the first of them
        offset = table->getFieldOffset(fixedSizedFields);
    else                        // we deal with a fixed-sized or the first var-sized field
        offset = table->getFieldOffset(N);
    offset -= table->getFieldOffset(0); // subtract header part (which does not exist in column format)
    res += countHat * offset;
    if (N > fixedSizedFields)
        res += recordCount * (N - fixedSizedFields) * 8;
    res += index * table->getFieldSize(N);
    return res;
}

/**
 * End of convenience functions
 *******************************/
