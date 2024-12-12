#include "lsm_tree.h"
#include "test_helpers.h"

#include <iostream>
#include <vector>
#include <random>
#include <chrono>
#include <fstream>
#include <utility>

// 10 MB is the same as 2560 4 KB pages
int BUFFER_POOL_NUM_PAGES = 2560;
// 1 MB = 1024 KB = 1048576 bytes which can hold 65536 key-value long pairs
int CURR_MEMTABLE_SIZE = 1048576 / ENTRY_SIZE;
// 1 GB = 1073741824 bytes which is 67108864 key-value long pairs
int TOTAL_PAIRS = 1073741824 / ENTRY_SIZE;

int BYTES_IN_MB = 1048576;

int GET_QUERIES_SIZE = 1048576 / ENTRY_SIZE;

// Still 1 MB of scans, but split up into 64 scans that scan 1024 * ENTRY_SIZE  bytes
int SCAN_QUERIES_SIZE = 1048576 / (ENTRY_SIZE * 1024);

// Keys to use for querying later
std::vector<long> all_keys;

std::vector<std::pair<long, long>> generate_random_pairs(size_t count)
{
    std::vector<std::pair<long, long>> pairs;
    pairs.reserve(count); // Reserve memory for efficiency

    // Random number generation
    std::random_device rd;                                 // Seed
    std::mt19937_64 gen(rd());                             // 64-bit Mersenne Twister engine
    std::uniform_int_distribution<long> dist(0, LONG_MAX); // Generate positive longs

    for (size_t i = 0; i < count; ++i)
    {
        long key = dist(gen);
        long value = dist(gen);
        pairs.emplace_back(key, value); // Add the pair to the vector
        all_keys.emplace_back(key);
    }

    return pairs;
}

std::vector<long> generate_random_keys(size_t count)
{
    std::vector<long> keys;
    // keys.reserve(count); // Reserve memory for efficiency

    // Random number generation
    std::random_device rd; // Seed
    // std::cout << "Seed: " << rd() << std::endl;
    std::mt19937_64 gen(rd());                             // 64-bit Mersenne Twister engine
    std::uniform_int_distribution<long> dist(0, LONG_MAX); // Generate positive longs

    for (size_t i = 0; i < count; ++i)
    {
        long key = dist(gen);
        keys.emplace_back(key); // Add the pair to the vector
    }

    return keys;
}

void write_to_csv(const std::string &filename, const std::vector<std::pair<int, double>> &data)
{
    try
    {
        // Open the file stream using RAII
        std::ofstream file(filename, std::ios::out);

        // Check if the file stream is valid
        if (!file)
        {
            throw std::runtime_error("Error: Could not open file " + filename);
        }

        // Write header row
        file << "Key,Value\n";

        // Write data rows
        for (const auto &pair : data)
        {
            file << pair.first << "," << pair.second << "\n";
        }

        // File will be closed automatically when the ofstream goes out of scope
        std::cout << "Data successfully written to " << filename << '\n';
    }
    catch (const std::exception &e)
    {
        // Handle any errors
        std::cerr << e.what() << '\n';
    }
}

std::vector<std::pair<int, double>> combine_coordinates(
    const std::vector<int> &x_values,
    const std::vector<double> &y_values)
{

    // Ensure the vectors are the same size
    if (x_values.size() != y_values.size())
    {
        throw std::invalid_argument("Vectors must have the same size");
    }

    // Create a vector to store the coordinates
    std::vector<std::pair<int, double>> coordinates;

    // Reserve space for efficiency
    coordinates.reserve(x_values.size());

    // Combine x and y into pairs
    for (size_t i = 0; i < x_values.size(); ++i)
    {
        coordinates.emplace_back(x_values[i], y_values[i]);
    }

    return coordinates;
}

int main()
{
    // Generate the vector of random key-value pairs
    std::cerr << "Generating data: \n";
    std::vector<std::pair<long, long>> random_pairs = generate_random_pairs(TOTAL_PAIRS);

    std::string current_database = "exp_" + getCurrentTimestamp();
    Memtable *memtable = dbOpen(current_database, CURR_MEMTABLE_SIZE);

    BufferPool *buffer_pool = new BufferPool(BUFFER_POOL_NUM_PAGES);

    std::cerr << "Starting put experiment: \n";

    bool with_btree = true;

    // Create LSM tree
    LSMTree *lsm_tree = new LSMTree(CURR_MEMTABLE_SIZE, current_database, memtable);
    std::vector<int> x_values_mb = {1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024};
    std::vector<double> put_latency = {};
    std::vector<double> get_latency = {};
    std::vector<double> scan_latency = {};

    int i = 0;
    for (auto &x_value : x_values_mb)
    {
        int checkpoint = x_value * BYTES_IN_MB / ENTRY_SIZE;

        auto put_start_time = std::chrono::high_resolution_clock::now();

        while (i < checkpoint)
        {
            lsm_tree->put(random_pairs[i].first, random_pairs[i].second);
            i++;
        }

        auto put_end_time = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double> put_elapsed_time = put_end_time - put_start_time;

        put_latency.push_back(put_elapsed_time.count());

        std::cout << x_value << "MB of puts took " << put_elapsed_time.count() << " seconds." << std::endl;

        // Uncomment for Random Get Keys
        std::vector<long> random_get_queries = generate_random_keys(GET_QUERIES_SIZE);

        // // Uncomment for Random but Existing Get Keys
        // std::random_device rd;
        // std::mt19937 gen(rd());
        // std::shuffle(all_keys.begin(), all_keys.end(), gen);

        // // Take the first query_count keys
        // std::vector<long> random_get_queries(all_keys.begin(), all_keys.begin() + GET_QUERIES_SIZE);

        auto get_start_time = std::chrono::high_resolution_clock::now();
        for (auto &key : random_get_queries)
        {
            lsm_tree->get(key, buffer_pool, with_btree);
        }
        auto get_end_time = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double> get_elapsed_time = get_end_time - get_start_time;

        get_latency.push_back(get_elapsed_time.count());
        std::cout << "1MB of random gets took " << get_elapsed_time.count() << " seconds." << std::endl;

        std::vector<long> random_scan_queries = generate_random_keys(GET_QUERIES_SIZE);

        auto scan_start_time = std::chrono::high_resolution_clock::now();
        for (auto &key : random_scan_queries)
        {
            if (LONG_MAX - key < 1024)
            {
                lsm_tree->scan(key - 1, key, buffer_pool, with_btree);
            }
            else
            {
                lsm_tree->scan(key, key + 1, buffer_pool, with_btree);
            }
        }
        auto scan_end_time = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double> scan_elapsed_time = scan_end_time - scan_start_time;

        scan_latency.push_back(scan_elapsed_time.count());
        std::cout << "1MB of random scans took " << scan_elapsed_time.count() << " seconds." << std::endl;
    }

    std::cerr << "Done!\n";

    // Write to CSV
    write_to_csv("./../experiments/step3put.csv", combine_coordinates(x_values_mb, put_latency));
    write_to_csv("./../experiments/step3get.csv", combine_coordinates(x_values_mb, get_latency));
    write_to_csv("./../experiments/step3scan.csv", combine_coordinates(x_values_mb, scan_latency));

    return 0;
}