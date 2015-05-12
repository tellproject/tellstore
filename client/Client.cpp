#include "Client.hpp"

#include <util/CommitManager.hpp>
#include <util/Record.hpp>
#include <util/Logging.hpp>

#include <crossbow/thread/thread.hpp>

#include <boost/system/error_code.hpp>

namespace tell {
namespace store {

void Client::init() {
    LOG_INFO("Initializing TellStore client");

    crossbow::thread t([this]() {
        boost::system::error_code ec;

        LOG_INFO("Initializing TellStore server connection [host = %1%, port = %2%]", mConfig.server, mConfig.port);
        mConnection.init(mConfig, ec);
        if (ec) {
            LOG_ERROR("Error initializing TellStore server connection [errcode = %1% %2%]", ec, ec.message());
            return;
        }

        LOG_INFO("Adding table", mConfig.server, mConfig.port);
        Schema schema;
        schema.addField(FieldType::INT, "foo", true);
        Record record(schema);

        uint64_t tableId;
        auto res = mConnection.createTable("testTable", schema, tableId, ec);
        if (ec) {
            LOG_ERROR("Error adding table [errcode = %1% %2%]", ec, ec.message());
            return;
        }
        if (!res) {
            LOG_ERROR("Table already exists");
            return;
        }

        CommitManager commitManager;
        auto snapshot = commitManager.startTx();
        bool succeeded = false;


        GenericTuple insertTuple({std::make_pair<crossbow::string, boost::any>("foo", 12)});
        size_t insertSize;
        std::unique_ptr<char[]> insertData(record.create(insertTuple, insertSize));
        mConnection.insert(tableId, 1, insertSize, insertData.get(), snapshot, ec, &succeeded);
        if (ec) {
            LOG_ERROR("Error inserting tuple [errcode = %1% %2%]", ec, ec.message());
            return;
        }
        if (!succeeded) {
            LOG_ERROR("Insert did not succeed");
            return;
        }

        size_t getSize;
        const char* getData;
        bool isNewest = false;
        succeeded = mConnection.get(tableId, 1, getSize, getData, snapshot, isNewest, ec);
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

        Record::id_t fooField;
        if (!record.idOf("foo", fooField)) {
            LOG_ERROR("foo field not found");
        }
        bool fooIsNull;
        auto fooData = record.data(getData, fooField, fooIsNull);
        if (*reinterpret_cast<const int32_t*>(fooData) != 12) {
            LOG_ERROR("Tuple value not 12");
        }
    });
}

void Client::shutdown() {
    LOG_INFO("Shutting down the TellStore client");

    // TODO
}

} // namespace store
} // namespace tell
