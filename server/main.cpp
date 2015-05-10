#include "ServerConfig.hpp"
#include "ConnectionManager.hpp"

#include <tellstore.hpp>
#include <util/Epoch.hpp>
#include <util/Logging.hpp>
#include <util/StorageConfig.hpp>

#include <crossbow/infinio/EventDispatcher.hpp>
#include <crossbow/program_options.hpp>

#include <iostream>
#include <thread>
#include <vector>

int main(int argc, const char** argv) {
    tell::store::StorageConfig storageConfig;
    tell::store::ServerConfig serverConfig;
    bool help = false;

    auto opts = crossbow::program_options::create_options(argv[0],
            crossbow::program_options::toggle<'h'>("help", help),
            crossbow::program_options::value<'p'>("port", serverConfig.port),
            crossbow::program_options::value<'m'>("memory", storageConfig.totalMemory),
            crossbow::program_options::value<'c'>("capacity", storageConfig.hashMapCapacity));

    try {
        crossbow::program_options::parse(opts, argc, argv);
    } catch (crossbow::program_options::argument_not_found e) {
        std::cerr << e.what() << std::endl << std::endl;
        crossbow::program_options::print_help(std::cout, opts);
        return 1;
    }

    if (help) {
        crossbow::program_options::print_help(std::cout, opts);
        return 0;
    }

    LOG_INFO("Starting TellStore server");
    tell::store::init();

    // Initialize storage
    tell::store::Storage storage(storageConfig);

    // Initialize network server
    crossbow::infinio::EventDispatcher dispatcher;
    tell::store::ConnectionManager connectionManager(storage, dispatcher, serverConfig);
    boost::system::error_code ec;
    connectionManager.init(ec);
    if (ec) {
        LOG_FATAL("Failure initializing the connection manager [error = %1% %2%]", ec, ec.message());
        return 1;
    }

    // Start event dispatcher threads
    auto execDispatcher = [&dispatcher] () {
        dispatcher.run();
    };

    std::vector<std::thread> threads;
    for (size_t i = 1; i < serverConfig.serverThreads; ++i) {
        threads.emplace_back(execDispatcher);
    }
    execDispatcher();

    // Join event dispatcher threads
    for (auto& t : threads) {
        t.join();
    }

    return 0;
}
