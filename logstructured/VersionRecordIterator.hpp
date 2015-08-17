#pragma once

#include "ChainedVersionRecord.hpp"

#include <cstdint>

namespace tell {
namespace store {
namespace logstructured {

class Table;

/**
 * @brief Class to iterate over and manipulate the version list of a record
 *
 * Helps fixing the version list when encountering an invalid element.
 */
class VersionRecordIterator {
public:
    VersionRecordIterator(Table& table, uint64_t minVersion, uint64_t key);

    ChainedVersionRecord& operator*() const {
        return *operator->();
    }

    ChainedVersionRecord* operator->() const {
        return mCurrent;
    }

    uint64_t minVersion() const {
        return mMinVersion;
    }

    /**
     * @brief Whether the iterator points to the end of the list
     */
    bool done() const {
        return (mCurrent == nullptr);
    }

    /**
     * @brief Advance the iterator to the next element
     *
     * If the iterator encounters an invalid element it tries to fix the version list by removing the invalid element
     * from the list. The iterator will be transparently reset to the beginning in the case this fails and the iterator
     * can not determine the next element in the list reliably.
     *
     * Must not be called when already at the end.
     */
    void next();

    /**
     * @brief Return the next element in the version history without advancing the iterator
     *
     * Returns a null pointer in case the current element is the last one in the version history.
     *
     * Must not be called when already at the end.
     */
    ChainedVersionRecord* peekNext() {
        return mCurrentData.next();
    }

    /**
     * @brief Whether the iterator points to the newest element in the version list
     */
    bool isNewest() const {
        return (mPrev == nullptr);
    }

    /**
     * @brief The valid to version of the current element
     */
    uint64_t validTo() const {
        return mCurrentData.validTo();
    }

    /**
     * @brief The current element
     */
    ChainedVersionRecord* value() {
        return mCurrent;
    }

    /**
     * @brief Advances the iterator until it points to the given element
     *
     * In case the element is not found in the list the iterator will point to the end of the list or some arbitrary
     * element.
     *
     * @param element The element to search for in the version list
     * @return Whether the element was found in the version list
     */
    bool find(ChainedVersionRecord* element);

    /**
     * @brief Advances the iterator until it points to the element with the given version
     *
     * In case the element is not found in the list the iterator will point to the end of the list or some arbitrary
     * element.
     *
     * @param version The version of the element to search for in the version list
     * @return Whether the element was found in the version list
     */
    bool find(uint64_t version);

    /**
     * @brief Removes the current element from the version list
     *
     * Even in the case the operation succeeds the old element might still be contained in the list (though marked as
     * invalid) and as such it is not safe to release or reuse the memory associated with the element immediately after
     * calling this function. Only when the iterator points to the following element, the memory of the old element may
     * be released.
     *
     * Currently supports only removal from the head of the version list.
     *
     * @return Whether the element was successfully removed
     */
    bool remove();

    /**
     * @brief Replaces the current element from the version list with the given element
     *
     * Even in the case the operation succeeds the old element might still be contained in the list (though marked as
     * invalid) and as such it is not safe to release or reuse the memory associated with the element immediately after
     * calling this function. Only when the iterator points to the given replacement element, the memory of the old
     * element may be released.
     *
     * @param next Element to replace the current element with
     * @return Whether the element was successfully replaced
     */
    bool replace(ChainedVersionRecord* next);

    /**
     * @brief Inserts the given element into the version list before the current element
     *
     * @param element Element to insert into the version list
     * @return Whether the element was successfully inserted
     */
    bool insert(ChainedVersionRecord* element);

private:
    /**
     * @brief Retrieves the head element of the version list from the hash table
     */
    ChainedVersionRecord* retrieveHead(uint64_t key);

    /**
     * @brief Expire the next element in the version list in the version of the current element
     *
     * The current element must have a valid next element.
     */
    void expireNextElement();

    /**
     * @brief Reactivate the next element in the version list
     *
     * The current element must have a valid next element.
     */
    void reactivateNextElement();

    /**
     * @brief Replaces the current invalid element with the given element
     *
     * The element must have been already invalidated. This operation only replaces the current element, the element's
     * mutable data has to be reloaded separately.
     *
     * @param next The next element in the version list to replace the current element with
     * @return Whether the replacement succeeded
     */
    bool replaceCurrentInvalidElement(ChainedVersionRecord* next) {
        return (mPrev ? removeCurrentInvalidFromPrevious(next) : removeCurrentInvalidFromHead(next));
    }

    /**
     * @brief Insert the element as the new head into the version list
     *
     * Iterator has to point to the head element. This operation only sets the current element, the element's mutable
     * data has to be reloaded separately.
     *
     * @param element The element to insert
     * @return Whether the insert was successful
     */
    bool insertHead(ChainedVersionRecord* element);

    /**
     * @brief Removes the current invalid head element from the version list and replaces it with next
     *
     * Iterator has to point to the head element and the element must have been already invalidated. This operation only
     * removes the current element, the element's mutable data has to be reloaded separately.
     *
     * @param next The next element in the version list to replace the current element with
     * @return Whether the removal was successful
     */
    bool removeCurrentInvalidFromHead(ChainedVersionRecord* next);

    /**
     * @brief Removes the current invalid non-head element from the version list and replaces it with next
     *
     * Iterator must not point to the head element and the element must have been already invalidated. This operation
     * only removes the current element, the element's mutable data has to be reloaded separately.
     *
     * @param next The next element in the version list to replace the current element with
     * @return Whether the removal was successful
     */
    bool removeCurrentInvalidFromPrevious(ChainedVersionRecord* next);

    /**
     * @brief Initializes the iterator with the current element or the next valid element.
     *
     * If the iterator encounters an invalid element it tries to fix the version list by removing the invalid element
     * from the list.
     */
    void setCurrentEntry();

    Table& mTable;
    const uint64_t mMinVersion;
    ChainedVersionRecord* mPrev;
    ChainedVersionRecord* mCurrent;
    MutableRecordData mPrevData;
    MutableRecordData mCurrentData;
};

} // namespace logstructured
} // namespace store
} // namespace tell
