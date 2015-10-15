#include <iostream>
#include <cstdint>
#include <random>
#include <chrono>
#include <vector>
#include <boost/dynamic_bitset.hpp>

/**
 * Lessons learned so far:
 * - SIMD registers are used as soon as we compile with -03
 * - If we want to get the real speed up (and use all SIMD registers by unfolding both loops,
 *   we have to know both, input-size and number of comparisons, at compile time.
 * - Otherwise, only some of the SIMD registers are used, and speedup is 0, compared to
 *   code optimized with 02.
 * - Example compile command: g++ -std=c++11 -march=native -O3 simd.cpp -o simd
 * - Command for getting assembly code: g++ -std=c++11 -march=native -O3 -S simd.cpp
 */

typedef std::mt19937 Engine;
typedef std::uniform_int_distribution<unsigned> Intdistr;


template <typename T, typename U>
void smaller (const T *input, U outputs, const T *comparison_values, const unsigned array_size, const unsigned comparisons) {

    for (unsigned i = 0; i < array_size; ++i)
    {
        for (unsigned m = 0; m < comparisons; ++m) {
            outputs[m*array_size+i] = input[i] < comparison_values[m];
        }
    }
}

//template <typename T>
//void pretty_print (T *arr, unsigned size, std::string s = "Pretty Print") {
//    std::cout << s << ":" << std::endl;
//    for (auto r = arr; r < arr+size; ++r ) {
//        std::cout << *r << std::endl;
//    }
//}

template <typename T>
void fill (T *arr, unsigned size) {
    Engine engine (0);
    Intdistr distr (0, 100000);
    for (auto r = arr; r < arr+size; ++r ) {
        *r = distr(engine);
    }
}

int main (int argc, char *argv[]) {
    typedef unsigned TestType;
    static constexpr unsigned repetitions = 100;

    constexpr unsigned input_size = 1000000;
    constexpr unsigned comparisons = 3;

//    if (argc != 3)
//    {
//        std::cout << "Usage: ./simd <input-size> <comparisons>" << std::endl;
//        return -1;
//    }
//    unsigned long input_size = std::stoi(argv[1]);
//    unsigned comparisons = std::stoi(argv[2]);

    std::cout << "input size: " << input_size << std::endl;
    std::cout << "comparisons :" << comparisons<< std::endl;

    TestType test_input [input_size];
    fill(test_input, input_size);
//    pretty_print(test_input, input_size, "Input");

    TestType comparison_values [comparisons];
    for (unsigned c = 0; c < comparisons; ++c) {
        comparison_values[c] = test_input[c];
    }
//    pretty_print(comparison_values, comparisons, "Comparison values");

    bool results [comparisons * input_size];
//    std::vector<bool> results (comparisons * input_size);
//    boost::dynamic_bitset<> results(comparisons * input_size);

    auto start = std::chrono::high_resolution_clock::now();
    for (unsigned i = 0; i < repetitions; ++i)
        smaller(test_input, results, comparison_values, input_size, comparisons);
    auto end = std::chrono::high_resolution_clock::now();
    std::cout << "Avg Time [microsecs]: "
              << (std::chrono::duration_cast<std::chrono::microseconds>(end-start).count())
                 /((double)repetitions)
              << std::endl;

//    for (unsigned c = 0; c < comparisons; ++c) {
//        pretty_print(&results[c*input_size], input_size, "Result");
//    }

    return 0;
}
