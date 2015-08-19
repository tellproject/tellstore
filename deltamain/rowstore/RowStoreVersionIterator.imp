namespace tell {
namespace store {
namespace deltamain {

RowStoreVersionIterator::RowStoreVersionIterator(const Record* record, const char* current)
    :
//    record(record),
    current(current)
{
    currEntry.mRecord = record;
    // go to the first valid entry
    while (current) {
        CDMRecord rec(current);
        if (rec.type() == RecordType::MULTI_VERSION_RECORD) {
            impl::RowStoreMVRecord<const char*> mvRec(current);
            auto numV = mvRec.getNumberOfVersions();
            auto offs = mvRec.offsets();
            for (decltype(numV) i = 0; i < numV; ++i) {
                if (offs[i] > 0) {
                    idx = i;
                    goto END;
                }
            }
            // this MVRecord only contains invalid data, but this should
            // never happen
            LOG_ERROR("You must never get an iterator on an invalid (reverted) record");
            std::terminate();
        } else if (rec.type() == RecordType::LOG_INSERT) {
            return;
        } else {
            LOG_ERROR("A Version iterator must always start either at in insert or on a MVRecord");
            std::terminate();
        }
    }
END:
    initRes();
}

void RowStoreVersionIterator::initRes() {
    if (current == nullptr) return;
    CDMRecord rec(current);
    if (rec.type() == RecordType::MULTI_VERSION_RECORD) {
        impl::RowStoreMVRecord<const char*> mvRec(current);
        auto nV = mvRec.getNumberOfVersions();
        auto versions = mvRec.versions();
        auto offs = mvRec.offsets();
        currEntry.mData = current + offs[idx];
        currEntry.mSize = offs[idx + 1] - offs[idx];
        currEntry.mValidFrom = versions[idx];
        if (idx == nV - 1) {
            // we need to check the next pointer in order to be able
            // to get the validTo property
            auto n = mvRec.getNewest();
            while (n != nullptr) {
                impl::LogOp<const char*> rc(n);
                if (rc.isValidDataRecord()) break;
                n = rc.getPrevious();
            }
            if (n == nullptr) {
                currEntry.mValidTo = std::numeric_limits<uint64_t>::max();
            } else {
                impl::LogOp<const char*> r(n);
                currEntry.mValidTo = r.version();
            }
        } else {
            currEntry.mValidTo = versions[idx + 1];
        }
    } else if (rec.type() == RecordType::LOG_INSERT) {
        impl::LogInsert<const char*> insRec(current);
        currEntry.mData = insRec.dataPtr();
        currEntry.mSize = insRec.recordSize();
        currEntry.mValidFrom = insRec.version();
        auto n = insRec.getNewest();
        while (n != nullptr) {
            impl::LogOp<const char*> rc(n);
            if (rc.isValidDataRecord()) break;
            n = rc.getPrevious();
        }
        if (n == nullptr) {
            currEntry.mValidTo = std::numeric_limits<uint64_t>::max();
        } else {
            impl::LogOp<const char*> r(n);
            currEntry.mValidTo = r.version();
        }
    } else if (rec.type() == RecordType::LOG_UPDATE) {
        impl::LogUpdate<const char*> up(current);
        currEntry.mData = up.dataPtr();
        currEntry.mSize = up.recordSize();
        currEntry.mValidTo = currEntry.mValidFrom;
        currEntry.mValidFrom = up.version();
    }
}

RowStoreVersionIterator& RowStoreVersionIterator::operator++() {
    while (current != nullptr) {
        CDMRecord rec(current);
        if (rec.type() == RecordType::MULTI_VERSION_RECORD) {
            impl::RowStoreMVRecord<const char*> mvRec(current);
            auto numV = mvRec.getNumberOfVersions();
            auto offs = mvRec.offsets();
            ++idx;
            while (numV > idx && offs[idx] < 0);
            if (numV == idx) {
                // we reachted the end of the MVRecord
                current = mvRec.getNewest();
            } else {
                // we are done
                goto END;
            }
        } else if (rec.type() == RecordType::LOG_INSERT) {
            impl::LogInsert<const char*> ins(current);
            if (!ins.isValidDataRecord()) {
                current = nullptr;
            } else {
                goto END;
            }
        } else if (rec.type() == RecordType::LOG_DELETE) {
            current = nullptr;
        } else if (rec.type() == RecordType::LOG_UPDATE) {
            impl::LogUpdate<const char*> upd(current);
            if (upd.isValidDataRecord()) {
                goto END;
            } else {
                current = upd.getPrevious();
            }
        }
    }
    END:
    initRes();
    return *this;
}

} // namespace deltamain
} // namespace store
} // namespace tell
