#include "Client.hpp"
#include "ClientConfig.hpp"

#include <crossbow/allocator.hpp>
#include <crossbow/infinio/InfinibandService.hpp>
#include <crossbow/logger.hpp>
#include <crossbow/program_options.hpp>

#include <iostream>

int main(int argc, const char** argv) {
    crossbow::string commitManagerHost;
    crossbow::string tellStoreHost;
    size_t numTuple = 1000000ull;
    size_t numTransactions = 10;
    tell::store::ClientConfig clientConfig;
    bool help = false;
    crossbow::string logLevel("DEBUG");

    auto opts = crossbow::program_options::create_options(argv[0],
            crossbow::program_options::value<'h'>("help", &help),
            crossbow::program_options::value<'l'>("log-level", &logLevel),
            crossbow::program_options::value<'c'>("commit-manager", &commitManagerHost),
            crossbow::program_options::value<'s'>("server", &tellStoreHost),
            crossbow::program_options::value<'m'>("memory", &clientConfig.scanMemory),
            crossbow::program_options::value<'n'>("tuple", &numTuple),
            crossbow::program_options::value<'t'>("transactions", &numTransactions));

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

    clientConfig.commitManager = crossbow::infinio::Endpoint(crossbow::infinio::Endpoint::ipv4(), commitManagerHost);
    clientConfig.tellStore = crossbow::infinio::Endpoint(crossbow::infinio::Endpoint::ipv4(), tellStoreHost);

    crossbow::infinio::InfinibandLimits infinibandLimits;
    infinibandLimits.receiveBufferCount = 128;
    infinibandLimits.sendBufferCount = 128;
    infinibandLimits.bufferLength = 32 * 1024;
    infinibandLimits.sendQueueLength = 128;

    crossbow::logger::logger->config.level = crossbow::logger::logLevelFromString(logLevel);

    LOG_INFO("Starting TellStore client [commitmanager = %1%, tellStore = %2%, memory = %3%GB, tuple = %4%, "
            "transactions = %5%]", clientConfig.commitManager, clientConfig.tellStore,
            double(clientConfig.scanMemory) / double(1024 * 1024 * 1024), numTuple, numTransactions);

    // Initialize allocator
    crossbow::allocator::init();

    // Initialize network stack
    crossbow::infinio::InfinibandService service(infinibandLimits);
    tell::store::Client client(service, clientConfig, numTuple, numTransactions);
    service.run();

    LOG_INFO("Exiting TellStore client");
    return 0;
}
