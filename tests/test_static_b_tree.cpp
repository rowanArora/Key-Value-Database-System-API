#include "test_static_b_tree.h"

extern void check(bool condition, const std::string &test_name);

// Filenames for the SST and B-Tree files
std::string sst_filename = DATA_FILE_PATH + "test_db/sst_test_file.bin";
std::string btree_filename = DATA_FILE_PATH + "test_db/btree_test_file.bin";
std::string bloom_filename = DATA_FILE_PATH + "test_db/bloom_test_file.bin";

// Global map to store generated key-value pairs for testing
std::map<int, int> generated_pairs;

// Helper function to clean up test files
void cleanup_test_files()
{
    std::filesystem::remove(sst_filename);
    std::filesystem::remove(btree_filename);
    std::filesystem::remove(btree_filename + ".txt");
}

// Function to create and write a Memtable to SST (generates B-Tree file)
void createTestSSTWithBTree(int size)
{
    Memtable memtable(size);
    generated_pairs.clear(); // Clear previous data if any

    for (int i = 1; i <= size; ++i)
    {
        int rand_key = i;
        int rand_val = i * 10;
        // std::cout << "Key : " << rand_key << ", Value: " << rand_val << std::endl;
        memtable.put(rand_key, rand_val);
        generated_pairs[rand_key] = rand_val; // Store in map for retrieval
    }

    writeMemtableToDisk(&memtable, sst_filename, btree_filename, bloom_filename, "test_db");
    std::cout << "Test SST and B-Tree files created." << std::endl;
}

// Test function for retrieving a single key from the B-Tree file using direct I/O
void testBTreeGet(StaticBTree btree, std::uniform_int_distribution<> distrib, std::mt19937 &gen, BufferPool *buffer_pool)
{
    std::cout << "Running testBTreeGet..." << std::endl;

    // Test known keys
    for (const auto &pair : generated_pairs)
    {
        long key = pair.first;
        long expected_value = pair.second;
        long value = btree.get(key, buffer_pool);
        check(value == expected_value, "BTree Get Test: Expected " + std::to_string(expected_value) +
                                           " and got " + std::to_string(value) + " for key " + std::to_string(key));
        if (value != expected_value)
        {
            exit(1);
        }
    }

    // Test multiple non-existent keys
    for (int i = 0; i < 3; ++i)
    {
        long non_existent_key = distrib(gen) * -1;
        if (generated_pairs.find(non_existent_key) == generated_pairs.end())
        {
            long value = btree.get(non_existent_key, buffer_pool);
            check(value == -1, "BTree Get Test: Expected -1 and got " + std::to_string(value) +
                                   " for non-existent key " + std::to_string(non_existent_key));
            if (value != -1)
            {
                exit(2);
            }
        }
    }

    std::cout << "Passed: testBTreeGet" << std::endl;
}

// Test function for scanning a range of keys in the B-Tree file using direct I/O
void testBTreeRangeScan(StaticBTree btree, std::uniform_int_distribution<> distrib, std::mt19937 &gen, BufferPool *buffer_pool)
{
    std::cout << "Running testBTreeRangeScan..." << std::endl;

    // Define specific ranges to test: small, medium, and large ranges, including the full range
    std::vector<std::pair<int, int>> test_ranges = {
        {1, 10},                                                                 // Small range
        {1, generated_pairs.rbegin()->first},                                    // Full range
        {generated_pairs.rbegin()->first - 10, generated_pairs.rbegin()->first}, // Edge case at the end
        {10, 20},                                                                // Medium range
        {250, 510},
        {10, 1}};

    for (const auto &range : test_ranges)
    {
        int start = range.first;
        int end = range.second;

        std::cout << "Testing range [" << start << ", " << end << "]" << std::endl;

        // Perform the scan
        std::vector<std::pair<long, long>> results = btree.scan(start, end, buffer_pool);

        // Build expected results based on the generated pairs
        std::vector<std::pair<long, long>> expected_results;
        for (const auto &pair : generated_pairs)
        {
            if (pair.first >= start && pair.first <= end)
            {
                expected_results.emplace_back(pair.first, pair.second);
            }
        }

        // Verify the results
        bool correct_size = results.size() == expected_results.size();
        check(correct_size, "BTree Range Scan Test: Expected size " + std::to_string(expected_results.size()) +
                                " and got " + std::to_string(results.size()) + " for range [" +
                                std::to_string(start) + ", " + std::to_string(end) + "]");
        if (!correct_size)
        {
            exit(3);
        }

        if (correct_size)
        {
            for (size_t j = 0; j < results.size(); ++j)
            {
                bool correct_pair = (results[j] == expected_results[j]);
                check(correct_pair, "BTree Range Scan Test: Expected pair (" + std::to_string(expected_results[j].first) +
                                        ", " + std::to_string(expected_results[j].second) + ") and got (" +
                                        std::to_string(results[j].first) + ", " + std::to_string(results[j].second) + ")");
                if (!correct_pair)
                {
                    exit(4);
                }
            }
        }
    }

    std::cout << "Passed: testBTreeRangeScan" << std::endl;
}

// Main function to run all B-Tree tests
int testBTreeMain(int memtable_size)
{
    std::cout << "Running all unit tests for static B-Tree..." << std::endl;

    std::string filepath = DATA_FILE_PATH + "test_db";
    if (!std::filesystem::create_directory(filepath))
    {
        std::cerr << "Error: creating directory - " << filepath << std::endl;
    }

    cleanup_test_files();                  // Clean up before each test run
    createTestSSTWithBTree(memtable_size); // Creates the SST and B-Tree binary files

    int min = 1;
    int max = memtable_size;
    BufferPool *buffer_pool = new BufferPool(BUFFER_POOL_MAX_PAGES);

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> distrib(min, max);
    StaticBTree btree(sst_filename, btree_filename);

    testBTreeGet(btree, distrib, gen, buffer_pool);
    testBTreeRangeScan(btree, distrib, gen, buffer_pool);

    std::cout << "Finished running static B-Tree tests." << std::endl;
    return 0;
}