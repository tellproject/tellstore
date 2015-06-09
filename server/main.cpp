#include "ServerConfig.hpp"
#include "ConnectionManager.hpp"

#include <tellstore.hpp>
#include <util/Epoch.hpp>
#include <util/Logging.hpp>
#include <util/StorageConfig.hpp>

#include <crossbow/program_options.hpp>

#include <iostream>
#include <system_error>

int main(int argc, const char** argv) {
    tell::store::StorageConfig storageConfig;
    tell::store::ServerConfig serverConfig;
    bool help = false;
    crossbow::string logLevel("DEBUG");

    auto opts = crossbow::program_options::create_options(argv[0],
            crossbow::program_options::value<'h'>("help", &help),
            crossbow::program_options::value<'l'>("log-level", &logLevel),
            crossbow::program_options::value<'p'>("port", &serverConfig.port),
            crossbow::program_options::value<'m'>("memory", &storageConfig.totalMemory),
            crossbow::program_options::value<'c'>("capacity", &storageConfig.hashMapCapacity),
            crossbow::program_options::value<'s'>("scan-threads", &storageConfig.numScanThreads));

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

    serverConfig.infinibandLimits.receiveBufferCount = 128;
    serverConfig.infinibandLimits.sendBufferCount = 128;
    serverConfig.infinibandLimits.bufferLength = 32 * 1024;
    serverConfig.infinibandLimits.sendQueueLength = 128;
    serverConfig.infinibandLimits.maxScatterGather = 32;

    tell::store::logger->config.level = tell::store::logLevelFromString(logLevel);

    LOG_INFO("Starting TellStore server");
    tell::store::init();

    // Initialize storage
    tell::store::Storage storage(storageConfig);

    // Initialize network server
    tell::store::ConnectionManager connectionManager(storage, serverConfig);

    LOG_INFO("Exiting TellStore server");
    return 0;
}
