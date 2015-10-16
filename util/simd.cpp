#include <iostream>
#include <cstdint>
#include <random>
#include <chrono>
#include <vector>
#include <boost/dynamic_bitset.hpp>
#include <numeric>

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
            outputs[m*array_size+i] = (float) (input[i] < comparison_values[m]);
        }
    }
}

template <typename T>
void pretty_print (T *arr, unsigned size, std::string s = "Pretty Print") {
    std::cout << s << ":" << std::endl;
    for (auto r = arr; r < arr+size; ++r ) {
        std::cout << *r << std::endl;
    }
}

template <typename T>
void compute_stats(const std::vector<T> stats, double &mean, double &stdev) {
    double sum = std::accumulate(stats.begin(), stats.end(), 0.0);
    mean = sum / stats.size();

    std::vector<double> diff(stats.size());
    std::transform(stats.begin(), stats.end(), diff.begin(),
                   std::bind2nd(std::minus<double>(), mean));
    double sq_sum = std::inner_product(diff.begin(), diff.end(), diff.begin(), 0.0);
    stdev = std::sqrt(sq_sum / stats.size());
}

template <typename T>
void fill (T *arr, unsigned size) {
    Engine engine (0);
    Intdistr distr (0, 100000);
    for (auto r = arr; r < arr+size; ++r ) {
        *r = distr(engine);
    }
}

int main (int argc, char *argv[]) {
    //**** PARAMS ****/
    typedef unsigned TestType;
    static constexpr unsigned repetitions = 10000;

    constexpr unsigned input_size = 500000;
    constexpr unsigned comparisons = 1;

//    if (argc != 3)
//    {
//        std::cout << "Usage: ./simd <input-size> <comparisons>" << std::endl;
//        return -1;
//    }
//    unsigned long input_size = std::stoi(argv[1]);
//    unsigned comparisons = std::stoi(argv[2]);

    std::cout << "input size: " << input_size << std::endl;
    std::cout << "comparisons: " << comparisons<< std::endl;


    //**** INPUT ****/
    TestType test_input [input_size];
    fill(test_input, input_size);
//    pretty_print(test_input, input_size, "Input");

    TestType comparison_values [comparisons];
    for (unsigned c = 0; c < comparisons; ++c) {
        comparison_values[c] = test_input[c];
    }
//    pretty_print(comparison_values, comparisons, "Comparison values");


    //**** COMPUTE ****/
    std::vector<unsigned long> stats (repetitions);
    bool results [comparisons * input_size];
//    std::vector<bool> results (comparisons * input_size);
//    boost::dynamic_bitset<> results(comparisons * input_size);

    for (unsigned i = 0; i < repetitions; ++i) {
        auto start = std::chrono::high_resolution_clock::now();
        smaller(test_input, results, comparison_values, input_size, comparisons);
        auto end = std::chrono::high_resolution_clock::now();
        stats [i] =
            std::chrono::duration_cast<std::chrono::microseconds>(end-start).count();
    }

    //**** REPORT ****/
    double mean, stdev;
    compute_stats(stats, mean, stdev);
    std::cout
//              << "Avg Time [microsecs]: "
              << mean
              << "\t"
//              << "(+/- "
              << stdev
//              << ")"
              << std::endl;
//    pretty_print(stats.data(), repetitions, "Stats");

//    for (unsigned c = 0; c < comparisons; ++c) {
//        pretty_print(&results[c*input_size], input_size, "Result");
//    }

    return 0;
}
