#include <util/ScanQuery.hpp>

#include <util/Record.hpp>

#include <gtest/gtest.h>

#include <cstdint>
#include <cstring>
#include <memory>
#include <vector>

using namespace tell::store;

namespace {

int64_t gTupleLargenumber = 0x7FFFFFFF00000001;

int32_t gTuple1Number = 12;
crossbow::string gTuple1Text = crossbow::string("Bacon ipsum dolor amet");

int32_t gTuple2Number = 13;
crossbow::string gTuple2Text = crossbow::string("t-bone chicken prosciutto");

/**
 * @brief Test fixture for testing the ScanQuery class
 *
 * Creates a schema with 3 fields and creates two sample tuples.
 */
class ScanQueryTest : public ::testing::Test {
protected:
    ScanQueryTest()
            : mSchema(TableType::TRANSACTIONAL) {
        mSchema.addField(FieldType::BIGINT, "largenumber", true);
        mSchema.addField(FieldType::INT, "number", true);
        mSchema.addField(FieldType::TEXT, "text", true);
        Record record(mSchema);

        GenericTuple insertTuple1({
                std::make_pair<crossbow::string, boost::any>("largenumber", gTupleLargenumber),
                std::make_pair<crossbow::string, boost::any>("number", gTuple1Number),
                std::make_pair<crossbow::string, boost::any>("text", gTuple1Text),
        });
        mTuple1.reset(record.create(insertTuple1, mTuple1Size));

        GenericTuple insertTuple2({
                std::make_pair<crossbow::string, boost::any>("largenumber", gTupleLargenumber),
                std::make_pair<crossbow::string, boost::any>("number", gTuple2Number),
                std::make_pair<crossbow::string, boost::any>("text", gTuple2Text),
        });
        mTuple2.reset(record.create(insertTuple2, mTuple2Size));
    }

    bool checkQuery(size_t qsize, const char* qbuffer, const char* tuple, size_t bitmapSize, const Record& record);

    ScanQuery mQuery;
    std::vector<bool> mQueryBitMap;

    Schema mSchema;

    uint64_t mTuple1Size;
    std::unique_ptr<char[]> mTuple1;

    uint64_t mTuple2Size;
    std::unique_ptr<char[]> mTuple2;
};

bool ScanQueryTest::checkQuery(size_t qsize, const char* qbuffer, const char* tuple, size_t bitmapSize,
        const Record& record) {
    mQueryBitMap.clear();
    mQuery.query = qbuffer;
    auto q = mQuery.check(tuple, mQueryBitMap, record);
    EXPECT_EQ(qbuffer + qsize, q) << "Returned pointer does not point at end of qbuffer";
    EXPECT_EQ(bitmapSize, mQueryBitMap.size()) << "Query bitmap does not contain the expected number of elements";

    for (auto res : mQueryBitMap) {
        if (!res) {
            return false;
        }
    }
    return true;
}

/**
 * @class ScanQuery
 * @test Check if the check succeeds for a query with no predicates
 *
 * The query is equal to "SELECT *".
 */
TEST_F(ScanQueryTest, noPredicates) {
    Record record(mSchema);

    size_t qsize = 8;
    std::unique_ptr<char[]> qbuffer(new char[qsize]);
    memset(qbuffer.get(), 0, qsize);

    EXPECT_TRUE(checkQuery(qsize, qbuffer.get(), mTuple1.get(), 0, record));
    EXPECT_TRUE(checkQuery(qsize, qbuffer.get(), mTuple2.get(), 0, record));
}

/**
 * @class ScanQuery
 * @test Check if the check succeeds for a simple EQUAL predicate
 *
 * The query is equal to "SELECT * WHERE number == 12".
 */
TEST_F(ScanQueryTest, singlePredicate) {
    Record record(mSchema);
    Record::id_t numberField;
    ASSERT_TRUE(record.idOf("number", numberField)) << "Field not found";

    size_t qsize = 24;
    std::unique_ptr<char[]> qbuffer(new char[qsize]);
    memset(qbuffer.get(), 0, qsize);
    *reinterpret_cast<uint64_t*>(qbuffer.get()) = 0x1u;
    *reinterpret_cast<uint16_t*>(qbuffer.get() + 8) = numberField;
    *reinterpret_cast<uint16_t*>(qbuffer.get() + 10) = 0x1u;
    *reinterpret_cast<uint8_t*>(qbuffer.get() + 16) = crossbow::to_underlying(PredicateType::EQUAL);
    *reinterpret_cast<uint8_t*>(qbuffer.get() + 17) = 0x0u;
    *reinterpret_cast<int32_t*>(qbuffer.get() + 20) = gTuple1Number;

    EXPECT_TRUE(checkQuery(qsize, qbuffer.get(), mTuple1.get(), 1, record));
    EXPECT_FALSE(checkQuery(qsize, qbuffer.get(), mTuple2.get(), 1, record));
}

/**
 * @class ScanQuery
 * @test Check if the check succeeds for two simple EQUAL predicates linked by an AND.
 *
 * The query is equal to "SELECT * WHERE number = 12 AND largenumber = 0x7FFFFFFF00000001".
 */
TEST_F(ScanQueryTest, andPredicate) {
    Record record(mSchema);
    Record::id_t numberField;
    ASSERT_TRUE(record.idOf("number", numberField)) << "Field not found";
    Record::id_t largenumberField;
    ASSERT_TRUE(record.idOf("largenumber", largenumberField)) << "Field not found";

    size_t qsize = 48;
    std::unique_ptr<char[]> qbuffer(new char[qsize]);
    memset(qbuffer.get(), 0, qsize);
    *reinterpret_cast<uint64_t*>(qbuffer.get()) = 0x2u;
    *reinterpret_cast<uint16_t*>(qbuffer.get() + 8) = numberField;
    *reinterpret_cast<uint16_t*>(qbuffer.get() + 10) = 0x1u;
    *reinterpret_cast<uint8_t*>(qbuffer.get() + 16) = crossbow::to_underlying(PredicateType::EQUAL);
    *reinterpret_cast<uint8_t*>(qbuffer.get() + 17) = 0x0u;
    *reinterpret_cast<int32_t*>(qbuffer.get() + 20) = gTuple1Number;
    *reinterpret_cast<uint16_t*>(qbuffer.get() + 24) = largenumberField;
    *reinterpret_cast<uint16_t*>(qbuffer.get() + 26) = 0x1u;
    *reinterpret_cast<uint8_t*>(qbuffer.get() + 32) = crossbow::to_underlying(PredicateType::EQUAL);
    *reinterpret_cast<uint8_t*>(qbuffer.get() + 33) = 0x1u;
    *reinterpret_cast<int64_t*>(qbuffer.get() + 40) = gTupleLargenumber;

    EXPECT_TRUE(checkQuery(qsize, qbuffer.get(), mTuple1.get(), 2, record));
    EXPECT_FALSE(checkQuery(qsize, qbuffer.get(), mTuple2.get(), 2, record));
}

/**
 * @class ScanQuery
 * @test Check if the check succeeds for two simple EQUAL predicates linked by an OR.
 *
 * The query is equal to "SELECT * WHERE number = 12 OR text = 't-bone chicken prosciutto'".
 */
TEST_F(ScanQueryTest, orPredicate) {
    Record record(mSchema);
    Record::id_t numberField;
    ASSERT_TRUE(record.idOf("number", numberField)) << "Field not found";
    Record::id_t textField;
    ASSERT_TRUE(record.idOf("text", textField)) << "Field not found";

    size_t qsize = 44 + gTuple2Text.length();
    qsize += ((qsize % 8 != 0) ? (8 - (qsize % 8)) : 0);
    std::unique_ptr<char[]> qbuffer(new char[qsize]);
    memset(qbuffer.get(), 0, qsize);
    *reinterpret_cast<uint64_t*>(qbuffer.get()) = 0x2u;
    *reinterpret_cast<uint16_t*>(qbuffer.get() + 8) = numberField;
    *reinterpret_cast<uint16_t*>(qbuffer.get() + 10) = 0x1u;
    *reinterpret_cast<uint8_t*>(qbuffer.get() + 16) = crossbow::to_underlying(PredicateType::EQUAL);
    *reinterpret_cast<uint8_t*>(qbuffer.get() + 17) = 0x0u;
    *reinterpret_cast<int32_t*>(qbuffer.get() + 20) = gTuple1Number;
    *reinterpret_cast<uint16_t*>(qbuffer.get() + 24) = textField;
    *reinterpret_cast<uint16_t*>(qbuffer.get() + 26) = 0x1u;
    *reinterpret_cast<uint8_t*>(qbuffer.get() + 32) = crossbow::to_underlying(PredicateType::EQUAL);
    *reinterpret_cast<uint8_t*>(qbuffer.get() + 33) = 0x0u;
    *reinterpret_cast<uint32_t*>(qbuffer.get() + 40) = gTuple2Text.length();
    memcpy(qbuffer.get() + 44, gTuple2Text.data(), gTuple2Text.length());

    EXPECT_TRUE(checkQuery(qsize, qbuffer.get(), mTuple1.get(), 1, record));
    EXPECT_TRUE(checkQuery(qsize, qbuffer.get(), mTuple2.get(), 1, record));
}

/**
 * @class ScanQuery
 * @test Check if the check succeeds for two simple EQUAL predicates linked by an OR on the same column.
 *
 * The query is equal to "SELECT * WHERE number = 12 OR number = 13".
 */
TEST_F(ScanQueryTest, sameColumnPredicate) {
    Record record(mSchema);
    Record::id_t numberField;
    ASSERT_TRUE(record.idOf("number", numberField)) << "Field not found";

    size_t qsize = 32;
    std::unique_ptr<char[]> qbuffer(new char[qsize]);
    memset(qbuffer.get(), 0, qsize);
    *reinterpret_cast<uint64_t*>(qbuffer.get()) = 0x1u;
    *reinterpret_cast<uint16_t*>(qbuffer.get() + 8) = numberField;
    *reinterpret_cast<uint16_t*>(qbuffer.get() + 10) = 0x2u;
    *reinterpret_cast<uint8_t*>(qbuffer.get() + 16) = crossbow::to_underlying(PredicateType::EQUAL);
    *reinterpret_cast<uint8_t*>(qbuffer.get() + 17) = 0x0u;
    *reinterpret_cast<int32_t*>(qbuffer.get() + 20) = gTuple1Number;
    *reinterpret_cast<uint8_t*>(qbuffer.get() + 24) = crossbow::to_underlying(PredicateType::EQUAL);
    *reinterpret_cast<uint8_t*>(qbuffer.get() + 25) = 0x0u;
    *reinterpret_cast<uint8_t*>(qbuffer.get() + 28) = gTuple2Number;

    EXPECT_TRUE(checkQuery(qsize, qbuffer.get(), mTuple1.get(), 1, record));
    EXPECT_TRUE(checkQuery(qsize, qbuffer.get(), mTuple2.get(), 1, record));
}

} // anonymous namespace
