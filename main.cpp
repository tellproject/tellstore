#include <iostream>
#include <util/TableManager.hpp>
#include "tellstore.hpp"

using namespace std;

int main() {
    tell::store::StorageConfig config;
    config.gcIntervall = 60;
    config.totalMemory = 200*0x100000; // 200 MB
    tell::store::Storage storage(config);
    return 0;
}
