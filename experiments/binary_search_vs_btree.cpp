#include <iostream>
#include <fstream>
#include <vector>
#include <random>
#include <chrono>
#include <map>
#include <filesystem>
#include "global.h"
#include "memtable.h"
#include "sst.h"
#include "static_b_tree.h"
#include "test_helpers.h"

// Constants
const std::string DATABASE_NAME = "experiment_binary_search_vs_btree";

// Map to store generated key-value pairs
std::map<long, long> generated_pairs;

// Keys to use for querying later
std::vector<long> all_keys;

// Progress Bar Function
void displayProgressBar(size_t current, size_t total, size_t bar_width = 50)
{
    float progress = static_cast<float>(current) / static_cast<float>(total);
    size_t pos = static_cast<size_t>(bar_width * progress);

    std::cout << "[";
    for (size_t i = 0; i < bar_width; ++i)
    {
        if (i < pos)
            std::cout << "=";
        else if (i == pos)
            std::cout << ">";
        else
            std::cout << " ";
    }
    std::cout << "] " << int(progress * 100.0) << "%\r";
    std::cout.flush();
}

// Function wrapper to measure execution time
template <typename Func, typename... Args,
          typename ReturnType = decltype(std::declval<Func>()(std::forward<Args>(std::declval<Args>())...)),
          typename std::enable_if<!std::is_same<ReturnType, void>::value, int>::type = 0>
auto measureExecutionTime(const std::string &function_name, Func func, Args &&...args)
{
    // Start time
    auto start = std::chrono::high_resolution_clock::now();

    // Execute the function and capture the return value
    auto result = func(std::forward<Args>(args)...);

    // End time
    auto end = std::chrono::high_resolution_clock::now();

    // Calculate duration
    double duration = std::chrono::duration<double>(end - start).count();

    // Return the result and the duration
    return std::make_pair(result, duration);
}

template <typename Func, typename... Args,
          typename ReturnType = decltype(std::declval<Func>()(std::forward<Args>(std::declval<Args>())...)),
          typename std::enable_if<std::is_same<ReturnType, void>::value, int>::type = 0>
double measureExecutionTime(const std::string &function_name, Func func, Args &&...args)
{
    // Start time
    auto start = std::chrono::high_resolution_clock::now();

    // Execute the function
    func(std::forward<Args>(args)...);

    // End time
    auto end = std::chrono::high_resolution_clock::now();

    // Calculate duration
    double duration = std::chrono::duration<double>(end - start).count();

    // Return the duration
    return duration;
}

// Cleanup helper
void cleanupTestFiles()
{
    const std::string database_path = DATA_FILE_PATH + DATABASE_NAME;

    try
    {
        // Check if the database directory exists
        if (std::filesystem::exists(database_path))
        {
            // Iterate over all files in the directory and remove them
            for (const auto &entry : std::filesystem::directory_iterator(database_path))
            {
                std::filesystem::remove(entry.path());
            }
        }
        else
        {
            std::cout << "Database directory does not exist. No files to remove.\n";
        }
    }
    catch (const std::filesystem::filesystem_error &e)
    {
        std::cerr << "Error while cleaning up database files: " << e.what() << '\n';
    }
}

// Generate random key-value pairs
std::vector<std::pair<long, long>> generateRandomData(size_t count)
{
    std::vector<std::pair<long, long>> data;
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<long> dist(1, 1e9);

    for (size_t i = 0; i < count; ++i)
    {
        long key = dist(gen);
        long value = dist(gen);
        data.emplace_back(key, value);
        all_keys.emplace_back(key);
    }
    return data;
}

// Create SST and B-Tree from random data
void createTestSSTWithBTree(const std::vector<std::pair<long, long>>& data, LSMTree*& lsm_tree, Memtable*& memtable) {
    // Memtable *memtable = new Memtable(data.size());
    memtable = new Memtable(MEMTABLE_SIZE);
    lsm_tree = new LSMTree(MEMTABLE_SIZE, DATABASE_NAME, memtable);

    generated_pairs.clear();

    size_t total_entries = data.size();
    for (size_t i = 0; i < total_entries; ++i) {
        const auto& [key, value] = data[i];
        // memtable->put(key, value);
        lsm_tree->put(key, value);
        generated_pairs[key] = value;

        // Update progress bar
        if (i % (total_entries / 100) == 0 || i == total_entries - 1)
        { // Update progress every 1% or on the last entry
            displayProgressBar(i + 1, total_entries);
        }
    }
    displayProgressBar(total_entries, total_entries); // Ensure the progress bar reaches 100%
    std::cout << std::endl;                         // Move to a new line after the progress bar

    // return std::make_pair(sst_filename, btree_filename);
}

// Measure throughput for a given query method
double measureThroughput(std::function<void()> query_func, size_t query_count)
{
    auto start = std::chrono::high_resolution_clock::now();
    for (size_t i = 0; i < query_count; ++i)
    {
        query_func();
        // Update progress bar
        if (i % (query_count / 100) == 0 || i == query_count - 1) { // Update progress every 1% or on the last entry
            displayProgressBar(i + 1, query_count);
        }
    }
    displayProgressBar(query_count, query_count); // Ensure the progress bar reaches 100%
    std::cout << std::endl; // Move to a new line after the progress bar
    auto end = std::chrono::high_resolution_clock::now();
    double duration = std::chrono::duration<double>(end - start).count();
    return query_count / duration; // Queries per second
}

int main() {
    const std::string output_file = "./../experiments/binary_vs_btree_results.csv";
    std::ofstream ofs(output_file);
    ofs << "Data Size (MB),Binary Search Throughput (queries/sec),B-Tree Throughput (queries/sec)\n";
    ofs.close();

    std::string filepath = DATA_FILE_PATH + "experiment_binary_search_vs_btree";
    if (!std::filesystem::create_directory(filepath))
    {
        std::cerr << "Error: creating directory - " << filepath << std::endl;
    }

    BufferPool buffer_pool(BUFFER_POOL_SIZE);

    size_t query_count = 1000;
    size_t data_size = MEGABYTE;

    while (data_size <= GIGABYTE) {
        std::cout << "\nTesting with data size: " << data_size / MEGABYTE << " MBs." << std::endl;

        // Clean up test files
        std::cout << "Removing all files from the " << DATABASE_NAME << " directory..." << std::endl;
        measureExecutionTime("cleanupTestFiles", cleanupTestFiles);
        std::cout << "All files in the " << DATABASE_NAME << " directory have been removed.\n"
                  << std::endl;

        // Generate random data
        std::cout << "Generating random data to add to the Memtable..." << std::endl;
        auto [data, data_generation_time] = measureExecutionTime("generateRandomData", generateRandomData, data_size);

        size_t update_interval = std::max<size_t>(1, data.size() / 100);
        for (size_t i = 0; i < data.size(); ++i)
        {
            if (i % update_interval == 0)
            { // Update every 1% or more frequently for small datasets
                displayProgressBar(i, data.size());
            }
        }
        displayProgressBar(data.size(), data.size());
        std::cout << std::endl;
        std::cout << "Finished generating random data to add to the Memtable." << std::endl;
        std::cout << "Function \"generateRandomData\" took " << data_generation_time << " seconds to complete.\n"
                  << std::endl;

        // Initialize LSM Tree
        LSMTree *lsm_tree = nullptr;

        // Create SST and B-Tree
        Memtable *memtable = nullptr;
        std::cout << "Adding values to the Memtable and potentially creating SSTs and B-Trees..." << std::endl;
        auto sst_creation_time = measureExecutionTime("createTestSSTWithBTree", [&]() {
            return createTestSSTWithBTree(data, lsm_tree, memtable);
        });
        std::cout << "Finished adding values to the Memtable and potentially creating SSTs and B-Trees." << std::endl;
        std::cout << "Function \"createTestSSTWithBTree\" took " << sst_creation_time << " seconds to complete.\n"
                  << std::endl;

        // Generate random query keys
        std::cout << "Generating random query keys..." << std::endl;
        auto [query_keys, query_key_generation_time] = measureExecutionTime("generateRandomQueryKeys", [&]()
                                                                            {
            // Shuffle the keys
            std::random_device rd;
            std::mt19937 gen(rd());
            std::shuffle(all_keys.begin(), all_keys.end(), gen);

            // Take the first query_count keys
            std::vector<long> keys(all_keys.begin(), all_keys.begin() + query_count);
            return keys;
        });
        displayProgressBar(query_count, query_count);
        std::cout << std::endl;
        std::cout << "Finished generating random query keys." << std::endl;
        std::cout << "Function \"generateRandomQueryKeys\" took " << query_key_generation_time << " seconds to complete.\n"
                  << std::endl;

        // Measure Binary Search Throughput
        std::cout << "Measuring Binary Search Throughput..." << std::endl;
        auto [binary_throughput, binary_throughput_time] = measureExecutionTime("measureBinarySearchThroughput", [&]()
                                                                                { return measureThroughput([&]()
                                                                                                           {
                for (long key : query_keys) {
                    lsm_tree->get(key, &buffer_pool, false);
                }
            }, query_count);
        });
        std::cout << "Finished Measuring Binary Search Throughput." << std::endl;
        std::cout << "Function \"measureBinarySearchThroughput\" took " << binary_throughput_time << " seconds to complete.\n"
                  << std::endl;

        // Measure B-Tree Throughput
        std::cout << "Measuring B-Tree Throughput..." << std::endl;
        auto [btree_throughput, btree_throughput_time] = measureExecutionTime("measureBTreeThroughput", [&]()
                                                                              { return measureThroughput([&]()
                                                                                                         {
                for (long key : query_keys) {
                    lsm_tree->get(key, &buffer_pool, true);
                }
            }, query_count);
        });
        std::cout << "Finished Measuring B-Tree Throughput..." << std::endl;
        std::cout << "Function \"measureBTreeThroughput\" took " << btree_throughput_time << " seconds to complete.\n"
                  << std::endl;

        // Record results
        ofs.open(output_file, std::ios::app);
        ofs << data_size / 1024 << "," << binary_throughput << "," << btree_throughput << "\n";
        ofs.close();

        data_size *= 2;

        delete memtable;
        delete lsm_tree;
        all_keys.clear();
        generated_pairs.clear();
    }

    std::cout << "Experiment completed. Results saved to " << output_file << std::endl;
    return 0;
}
