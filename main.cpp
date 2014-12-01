#include <iostream>
#include <util/TableManager.hpp>
#include "tellstore.hpp"

using namespace std;

int main() {
    tell::store::StorageConfig config;
    config.gcIntervall = 60;
    tell::store::Storage storage(config);
    return 0;
}
