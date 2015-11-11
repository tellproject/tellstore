/*
 * (C) Copyright 2015 ETH Zurich Systems Group (http://www.systems.ethz.ch/) and others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Contributors:
 *     Markus Pilman <mpilman@inf.ethz.ch>
 *     Simon Loesing <sloesing@inf.ethz.ch>
 *     Thomas Etter <etterth@gmail.com>
 *     Kevin Bocksrocker <kevin.bocksrocker@gmail.com>
 *     Lucas Braun <braunl@inf.ethz.ch>
 */
#include <util/ScanQuery.hpp>

#include <tellstore/Record.hpp>

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

    bool checkQuery(size_t qsize, const char* qbuffer, uint64_t key, const char* tuple, const Record& record);

    Schema mSchema;

    uint64_t mTuple1Size;
    std::unique_ptr<char[]> mTuple1;

    uint64_t mTuple2Size;
    std::unique_ptr<char[]> mTuple2;
};

bool ScanQueryTest::checkQuery(size_t qsize, const char* qbuffer, uint64_t key, const char* tuple,
        const Record& record) {
    auto q = qbuffer;
    auto res = ScanQueryBatchProcessor::check(q, key, tuple, record);
    EXPECT_EQ(qbuffer + qsize, q) << "Returned pointer does not point at end of qbuffer";
    return res;
}

/**
 * @class ScanQuery
 * @test Check if the check succeeds for a query with no predicates
 *
 * The query is equal to "SELECT *".
 */
TEST_F(ScanQueryTest, noPredicates) {
    Record record(mSchema);

    size_t qsize = 16;
    std::unique_ptr<char[]> qbuffer(new char[qsize]);
    memset(qbuffer.get(), 0, qsize);

    EXPECT_TRUE(checkQuery(qsize, qbuffer.get(), 1, mTuple1.get(), record));
    EXPECT_TRUE(checkQuery(qsize, qbuffer.get(), 2, mTuple2.get(), record));
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

    size_t qsize = 32;
    std::unique_ptr<char[]> qbuffer(new char[qsize]);
    memset(qbuffer.get(), 0, qsize);
    *reinterpret_cast<uint64_t*>(qbuffer.get()) = 0x1u;
    *reinterpret_cast<uint16_t*>(qbuffer.get() + 16) = numberField;
    *reinterpret_cast<uint16_t*>(qbuffer.get() + 18) = 0x1u;
    *reinterpret_cast<uint8_t*>(qbuffer.get() + 24) = crossbow::to_underlying(PredicateType::EQUAL);
    *reinterpret_cast<uint8_t*>(qbuffer.get() + 25) = 0x0u;
    *reinterpret_cast<int32_t*>(qbuffer.get() + 28) = gTuple1Number;

    EXPECT_TRUE(checkQuery(qsize, qbuffer.get(), 1, mTuple1.get(), record));
    EXPECT_FALSE(checkQuery(qsize, qbuffer.get(), 2, mTuple2.get(), record));
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

    size_t qsize = 56;
    std::unique_ptr<char[]> qbuffer(new char[qsize]);
    memset(qbuffer.get(), 0, qsize);
    *reinterpret_cast<uint64_t*>(qbuffer.get()) = 0x2u;
    *reinterpret_cast<uint16_t*>(qbuffer.get() + 16) = numberField;
    *reinterpret_cast<uint16_t*>(qbuffer.get() + 18) = 0x1u;
    *reinterpret_cast<uint8_t*>(qbuffer.get() + 24) = crossbow::to_underlying(PredicateType::EQUAL);
    *reinterpret_cast<uint8_t*>(qbuffer.get() + 25) = 0x0u;
    *reinterpret_cast<int32_t*>(qbuffer.get() + 28) = gTuple1Number;
    *reinterpret_cast<uint16_t*>(qbuffer.get() + 32) = largenumberField;
    *reinterpret_cast<uint16_t*>(qbuffer.get() + 34) = 0x1u;
    *reinterpret_cast<uint8_t*>(qbuffer.get() + 40) = crossbow::to_underlying(PredicateType::EQUAL);
    *reinterpret_cast<uint8_t*>(qbuffer.get() + 41) = 0x1u;
    *reinterpret_cast<int64_t*>(qbuffer.get() + 48) = gTupleLargenumber;

    EXPECT_TRUE(checkQuery(qsize, qbuffer.get(), 1, mTuple1.get(), record));
    EXPECT_FALSE(checkQuery(qsize, qbuffer.get(), 2, mTuple2.get(), record));
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

    size_t qsize = 48 + gTuple2Text.length();
    qsize += ((qsize % 8 != 0) ? (8 - (qsize % 8)) : 0);
    std::unique_ptr<char[]> qbuffer(new char[qsize]);
    memset(qbuffer.get(), 0, qsize);
    *reinterpret_cast<uint64_t*>(qbuffer.get()) = 0x2u;
    *reinterpret_cast<uint16_t*>(qbuffer.get() + 16) = numberField;
    *reinterpret_cast<uint16_t*>(qbuffer.get() + 18) = 0x1u;
    *reinterpret_cast<uint8_t*>(qbuffer.get() + 24) = crossbow::to_underlying(PredicateType::EQUAL);
    *reinterpret_cast<uint8_t*>(qbuffer.get() + 25) = 0x0u;
    *reinterpret_cast<int32_t*>(qbuffer.get() + 28) = gTuple1Number;
    *reinterpret_cast<uint16_t*>(qbuffer.get() + 32) = textField;
    *reinterpret_cast<uint16_t*>(qbuffer.get() + 34) = 0x1u;
    *reinterpret_cast<uint8_t*>(qbuffer.get() + 40) = crossbow::to_underlying(PredicateType::EQUAL);
    *reinterpret_cast<uint8_t*>(qbuffer.get() + 41) = 0x0u;
    *reinterpret_cast<uint32_t*>(qbuffer.get() + 44) = gTuple2Text.length();
    memcpy(qbuffer.get() + 48, gTuple2Text.data(), gTuple2Text.length());

    EXPECT_TRUE(checkQuery(qsize, qbuffer.get(), 1, mTuple1.get(), record));
    EXPECT_TRUE(checkQuery(qsize, qbuffer.get(), 2, mTuple2.get(), record));
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

    size_t qsize = 40;
    std::unique_ptr<char[]> qbuffer(new char[qsize]);
    memset(qbuffer.get(), 0, qsize);
    *reinterpret_cast<uint64_t*>(qbuffer.get()) = 0x1u;
    *reinterpret_cast<uint16_t*>(qbuffer.get() + 16) = numberField;
    *reinterpret_cast<uint16_t*>(qbuffer.get() + 18) = 0x2u;
    *reinterpret_cast<uint8_t*>(qbuffer.get() + 24) = crossbow::to_underlying(PredicateType::EQUAL);
    *reinterpret_cast<uint8_t*>(qbuffer.get() + 25) = 0x0u;
    *reinterpret_cast<int32_t*>(qbuffer.get() + 28) = gTuple1Number;
    *reinterpret_cast<uint8_t*>(qbuffer.get() + 32) = crossbow::to_underlying(PredicateType::EQUAL);
    *reinterpret_cast<uint8_t*>(qbuffer.get() + 33) = 0x0u;
    *reinterpret_cast<uint8_t*>(qbuffer.get() + 36) = gTuple2Number;

    EXPECT_TRUE(checkQuery(qsize, qbuffer.get(), 1, mTuple1.get(), record));
    EXPECT_TRUE(checkQuery(qsize, qbuffer.get(), 2, mTuple2.get(), record));
}

/**
 * @class ScanQuery
 * @test Check if the check succeeds for the key that is in the partition
 *
 * The query is equal to "SELECT * WHERE key % 2 = 1".
 */
TEST_F(ScanQueryTest, partitioning) {
    Record record(mSchema);

    size_t qsize = 16;
    std::unique_ptr<char[]> qbuffer(new char[qsize]);
    memset(qbuffer.get(), 0, qsize);

    *reinterpret_cast<uint32_t*>(qbuffer.get() + 8) = 0x2u;
    *reinterpret_cast<uint32_t*>(qbuffer.get() + 12) = 0x1u;

    EXPECT_TRUE(checkQuery(qsize, qbuffer.get(), 1, mTuple1.get(), record));
    EXPECT_FALSE(checkQuery(qsize, qbuffer.get(), 2, mTuple2.get(), record));
}

} // anonymous namespace
