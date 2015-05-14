#include "Client.hpp"

#include <util/CommitManager.hpp>
#include <util/Record.hpp>
#include <util/Logging.hpp>

#include <boost/system/error_code.hpp>

#include <chrono>
#include <functional>

namespace tell {
namespace store {

void Client::init() {
    LOG_INFO("Initializing TellStore client");

    boost::system::error_code ec;
    mManager.init(mConfig, ec, [this]() {
        LOG_DEBUG("Start transaction");
        auto trans = mManager.startTransaction();
        auto res = trans->execute(std::bind(&Client::executeTransaction, this, std::placeholders::_1));
        if (!res) {
            LOG_ERROR("Unable to execute transaction function");
        }
    });
    if (ec) {
        LOG_ERROR("Failure init %1% %2%", ec, ec.message());
    }
}

void Client::shutdown() {
    LOG_INFO("Shutting down the TellStore client");

    // TODO
}

void Client::executeTransaction(Transaction& transaction) {
    boost::system::error_code ec;

    LOG_INFO("Adding table");
    Schema schema;
    schema.addField(FieldType::INT, "foo", true);
    Record record(schema);

    uint64_t tableId;
    auto startTime = std::chrono::high_resolution_clock::now();
    auto res = transaction.createTable("testTable", schema, tableId, ec);
    auto endTime = std::chrono::high_resolution_clock::now();
    if (ec) {
        LOG_ERROR("Error adding table [errcode = %1% %2%]", ec, ec.message());
        return;
    }
    if (!res) {
        LOG_ERROR("Table already exists");
        return;
    }
    auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(endTime - startTime);
    LOG_INFO("Adding table took %1%ns", duration.count());

    CommitManager commitManager;
    auto snapshot = commitManager.startTx();
    bool succeeded = false;

    LOG_INFO("Insert tuple");
    GenericTuple insertTuple({std::make_pair<crossbow::string, boost::any>("foo", 12)});
    size_t insertSize;
    std::unique_ptr<char[]> insertData(record.create(insertTuple, insertSize));
    startTime = std::chrono::high_resolution_clock::now();
    transaction.insert(tableId, 1, insertSize, insertData.get(), snapshot, ec, &succeeded);
    endTime = std::chrono::high_resolution_clock::now();
    if (ec) {
        LOG_ERROR("Error inserting tuple [errcode = %1% %2%]", ec, ec.message());
        return;
    }
    if (!succeeded) {
        LOG_ERROR("Insert did not succeed");
        return;
    }
    duration = std::chrono::duration_cast<std::chrono::nanoseconds>(endTime - startTime);
    LOG_INFO("Inserting tuple took %1%ns", duration.count());

    LOG_INFO("Get tuple");
    size_t getSize;
    const char* getData;
    bool isNewest = false;
    startTime = std::chrono::high_resolution_clock::now();
    succeeded = transaction.get(tableId, 1, getSize, getData, snapshot, isNewest, ec);
    endTime = std::chrono::high_resolution_clock::now();
    if (ec) {
        LOG_ERROR("Error getting tuple [errcode = %1% %2%]", ec, ec.message());
        return;
    }
    if (!succeeded) {
        LOG_ERROR("Tuple not found");
        return;
    }
    if (!isNewest) {
        LOG_ERROR("Tuple not the newest");
        return;
    }
    duration = std::chrono::duration_cast<std::chrono::nanoseconds>(endTime - startTime);
    LOG_INFO("Getting tuple took %1%ns", duration.count());

    LOG_INFO("Check tuple");
    Record::id_t fooField;
    if (!record.idOf("foo", fooField)) {
        LOG_ERROR("foo field not found");
    }
    bool fooIsNull;
    auto fooData = record.data(getData, fooField, fooIsNull);
    if (*reinterpret_cast<const int32_t*>(fooData) != 12) {
        LOG_ERROR("Tuple value not 12");
    }
    LOG_INFO("Tuple check successful");
}

} // namespace store
} // namespace tell
