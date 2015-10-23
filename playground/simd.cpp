#include <iostream>
#include <cstdint>
#include <random>
#include <chrono>
#include <vector>
#include <boost/dynamic_bitset.hpp>
#include <bitset>
#include <numeric>
#include <functional>

/**
 * Lessons learned so far:
 * - SIMD registers are used as soon as we compile with -03
 * - If we want to get the real speed up (and use all SIMD registers by unfolding both loops,
 *   we have to know both, input-size and number of comparisons, at compile time.
 * - Otherwise, only some of the SIMD registers are used, and speedup is 0, compared to
 *   code optimized with 02.
 * - Example compile command: g++ -std=c++11 -march=native -O3 simd.cpp -o simd
 * - Command for getting assembly code: g++ -std=c++11 -march=native -O3 -S simd.cpp
 * - Function pointers (array of std::function) are very slow. On the other hand, templated functions seem promissing :-)
 * - In either case, byte arrays are blazingly fast
 */

typedef std::mt19937 Engine;
typedef std::uniform_int_distribution<unsigned> Intdistr;

/**
 * takes a bunch of comparison values and an input array and compares each array value
 * with each comparison value (with smaller), outputting an array of booleans
 */
template <typename T, typename U>
void smaller (const T *input, U &outputs, const T *comparison_values, const unsigned array_size, const unsigned comparisons) {
    for (unsigned i = 0; i < array_size; ++i)
    {
        for (unsigned m = 0; m < comparisons; ++m) {
            outputs[m*array_size+i] = (input[i] < comparison_values[m]);
        }
    }
}

/**
 * takes a bunch of comparison values and an input array and compares each array value
 * with each comparison value (alternating smaller and larger), outputting an array of booleans
 */
template <typename T, typename U>
void smaller_greater (const T *input, U &outputs, const T *comparison_values, const unsigned array_size, const unsigned comparisons) {
    for (unsigned i = 0; i < array_size; ++i)
    {
        for (unsigned m = 0; m < comparisons; m+=2) {
            outputs[m*array_size+i] = (input[i] < comparison_values[m]);
            outputs[(m+1)*array_size+i] = (input[i] > comparison_values[m+1]);
        }
    }
}

template <typename T, typename U>
void general_compare (const T *input, U &outputs, const T *comparison_values,
                      const unsigned array_size, const unsigned comparisons,
                      std::function<bool(T,T)>* comparators) {
    for (unsigned i = 0; i < array_size; ++i)
    {
        for (unsigned m = 0; m < comparisons; ++m) {
            outputs[m*array_size+i] = comparators[m](input[i],comparison_values[m]);
        }
    }
}

template <typename T, typename U, typename Compare>
void templated_compare (const T *input, U &outputs, const T *comparison_values, const unsigned array_size, const unsigned comparisons) {
    Compare comp;
    for (unsigned i = 0; i < array_size; ++i)
    {
        for (unsigned m = 0; m < comparisons; ++m) {
            outputs[m*array_size+i] = comp (input[i],comparison_values[m]);
        }
    }
}

// hand-unroll, only one comparison at the time
template <typename T, typename U, typename Compare, int UNROLL>
void templated_compare (const T *input, U &outputs, const T comparison_value, const unsigned array_size) {
    Compare comp;
    for (unsigned i = 0; i < array_size; i+= UNROLL)
    {
        for(unsigned j = 0; j < UNROLL; ++j) {
            outputs[i+j] = comp (input[i+j],comparison_value);
        }
    }
}


template <typename T>
void pretty_print (T arr, unsigned size, std::string s = "Pretty Print", unsigned index = 0) {
//    std::cout << s << ":" << std::endl;
//    for (unsigned i = 0; i < size; ++i) {
//        std::cout << arr[index*size + i] << std::endl;
//    }
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

template <typename T>
void fillRandomComparators(std::function<bool(T,T)> *comparators, unsigned size)
{
    Engine engine (0);
    Intdistr distr (0, 5);
    unsigned comp;
    for (unsigned i = 0; i < size; ++i) {
        comp = distr(engine);
        switch (comp) {
            case 0:
                comparators[i] = std::greater<T>();
                break;
            case 1:
                comparators[i] = std::greater_equal<T>();
                break;
            case 2:
                comparators[i] = std::equal_to<T>();
                break;
            case 3:
                comparators[i] = std::not_equal_to<T>();
                break;
            case 4:
                comparators[i] = std::less_equal<T>();
                break;
            case 5:
                comparators[i] = std::less<T>();
                break;
        }
//        std::cout << "Comparator: " << comp << std::endl;
    }
}

int main (int argc, char *argv[]) {
    //**** PARAMS ****/
    typedef unsigned TestType;
    static constexpr unsigned repetitions = 10000;

    constexpr unsigned input_size = 500000;
    constexpr unsigned comparisons = 17;    // 17 seems to be a magic number if you use std::bitset

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
    pretty_print(test_input, input_size, "Input");

    TestType comparison_values [comparisons];
    for (unsigned c = 0; c < comparisons; ++c) {
        comparison_values[c] = test_input[c];
    }
    pretty_print(comparison_values, comparisons, "Comparison values");

    std::function<bool(TestType,TestType)> comparators [comparisons];
    fillRandomComparators(comparators, comparisons);
//    std::function<bool(TestType,TestType)> comparators [] = {std::less_equal<TestType>()};


    //**** COMPUTE ****/
    std::vector<unsigned long> stats (repetitions);
    bool results [comparisons * input_size];
//    std::bitset<comparisons*input_size> results;
//    std::vector<bool> results (comparisons * input_size);
//    boost::dynamic_bitset<> results(comparisons * input_size);

    for (unsigned i = 0; i < repetitions; ++i) {
        auto start = std::chrono::high_resolution_clock::now();
//        smaller(test_input, results, comparison_values, input_size, comparisons);
//        smaller_greater(test_input, results, comparison_values, input_size, comparisons);
//        general_compare(test_input, results, comparison_values, input_size, comparisons, comparators);
        templated_compare<TestType, decltype(results), std::greater<TestType>>(test_input, results, comparison_values, input_size, comparisons);
        auto end = std::chrono::high_resolution_clock::now();
        stats [i] =
            std::chrono::duration_cast<std::chrono::nanoseconds>(end-start).count();
    }

    //**** REPORT ****/
    double mean, stdev;
    compute_stats(stats, mean, stdev);
    std::cout
//              << "Avg Time [nanosecs]: "
              << mean
              << "\t"
//              << "(+/- "
              << stdev
//              << ")"
              << std::endl;

//    for (unsigned c = 0; c < comparisons; ++c) {
//        pretty_print(results, input_size, std::string("Result"), c);
//    }

//    std::cout << "Result: " << results << std::endl;

    return 0;
}
