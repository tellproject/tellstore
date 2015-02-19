#include <iostream>
#include <crossbow/program_options.hpp>
#include <util/TableManager.hpp>
#include "tellstore.hpp"

using namespace std;
using namespace crossbow;
using namespace crossbow::program_options;

int main() {
    tell::store::StorageConfig config;
    bool print_help;
    config.gcIntervall = 60;
    config.totalMemory = 200 * 0x100000; // 200 MB
    tell::store::Storage storage(config);
    auto po = create_options("TellStore",
            toggle<'h'>(print_help, "help"));
    return 0;
}
