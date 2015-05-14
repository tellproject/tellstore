#include "Client.hpp"
#include "ClientConfig.hpp"

#include <util/Epoch.hpp>
#include <util/Logging.hpp>

#include <crossbow/infinio/EventDispatcher.hpp>
#include <crossbow/program_options.hpp>

#include <iostream>
#include <thread>
#include <vector>

int main(int argc, const char** argv) {
    tell::store::ClientConfig clientConfig;
    bool help = false;

    auto opts = crossbow::program_options::create_options(argv[0],
            crossbow::program_options::value<'h'>("help", &help),
            crossbow::program_options::value<'s'>("server", &clientConfig.server),
            crossbow::program_options::value<'p'>("port", &clientConfig.port));

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

    LOG_INFO("Starting TellStore client");
    tell::store::init();

    // Initialize network stack
    crossbow::infinio::EventDispatcher dispatcher;
    tell::store::Client client(dispatcher, clientConfig);
    client.init();

    // Start event dispatcher threads
    auto execDispatcher = [&dispatcher] () {
        dispatcher.run();
    };

    LOG_INFO("Start dispatcher threads");
    std::vector<std::thread> threads;
    for (size_t i = 1; i < clientConfig.networkThreads; ++i) {
        threads.emplace_back(execDispatcher);
    }
    execDispatcher();

    // Join event dispatcher threads
    for (auto& t : threads) {
        t.join();
    }

    return 0;
}
