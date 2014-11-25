#include <iostream>
#include "tellstore.hpp"

using namespace std;

int main() {
    std::hash<int> h1;
    std::hash<int> h2;

    cout << h1(12) << endl;
    cout << h2(12) << endl;
    return 0;
}
