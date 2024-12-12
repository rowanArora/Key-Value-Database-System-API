#include "test_lsm_tree.h"
#include <stdlib.h>
#include <map>

extern void check(bool condition, const std::string &test_name);

void testLSMScanTwoPage()
{
    // Open DB
    int db_size = 257;
    std::string current_database = "test_db";
    Memtable *memtable = dbOpen(current_database, db_size);

    BufferPool *buffer_pool = new BufferPool(BUFFER_POOL_MAX_PAGES);

    // Create LSM tree
    LSMTree *lsm_tree = new LSMTree(db_size, current_database, memtable);

    // Put values for SST1
    std::map<long, long> answer_map;
    for (int i = 1; i <= 257; ++i)
    {
        // memtable->put(i, i * 10);
        // put(memtable, current_database, lsm_tree, db_size, i, i * 10);
        lsm_tree->put(i, i * 10);
        answer_map[i] = i * 10;
    }

    for (int i = 2; i <= 258; ++i)
    {
        lsm_tree->put(i, i * 100);
        answer_map[i] = i * 100;
    }

    // Setup Expected values to compare
    std::vector<std::pair<long, long>> answer_vector;
    for (const auto &entry : answer_map)
    {
        answer_vector.push_back(entry);
    }

    std::pair<std::pair<long, long> *, int> scanned_pairs = lsm_tree->scan(0, 258, buffer_pool, false);
    std::pair<long, long> *key_value_pairs = scanned_pairs.first;
    int scanned_size = scanned_pairs.second;

    bool is_success = true;
    if (answer_vector.size() == scanned_size)
    {
        for (int i = 0; i < answer_vector.size(); i++)
        {
            // std::cerr << "(" << answer_vector[i].first << ", " << answer_vector[i].second << ")\n";
            // std::cerr << "(" << key_value_pairs[i].first << ", " << key_value_pairs[i].second << ")\n";
            if (answer_vector[i] != key_value_pairs[i])
            {
                is_success = false;
            }
        }
    }
    else
    {
        std::cerr << "Sizes: (" << answer_vector.size() << ", " << scanned_size << ")\n";
        is_success = false;
    }

    check(is_success, "testLSMScanTwoPage: Scan SSTs with two page each.");
    dbClear(current_database);
    free(buffer_pool);
    free(memtable);
    free(lsm_tree);
}

void testLSMScanTwoPagesDiskOnePageInMemoryOneLevel()
{
    // Open DB
    int db_size = 256;
    std::string current_database = "test_db";
    Memtable *memtable = dbOpen(current_database, db_size);

    BufferPool *buffer_pool = new BufferPool(BUFFER_POOL_MAX_PAGES);

    // Create LSM tree
    LSMTree *lsm_tree = new LSMTree(db_size, current_database, memtable);

    // Put values for SST1
    std::map<long, long> answer_map;
    for (int i = 1; i <= 513; ++i)
    {
        // std::cerr<< "Put index: " << i << "\n";
        lsm_tree->put(i, i * 10);
        answer_map[i] = i * 10;
    }

    // Setup Expected values to compare
    std::vector<std::pair<long, long>> answer_vector;
    for (const auto &entry : answer_map)
    {
        answer_vector.push_back(entry);
    }

    std::pair<std::pair<long, long> *, int> scanned_pairs = lsm_tree->scan(0, 513, buffer_pool, false);
    std::pair<long, long> *key_value_pairs = scanned_pairs.first;
    int scanned_size = scanned_pairs.second;

    bool is_success = true;
    if (answer_vector.size() == scanned_size)
    {
        for (int i = 0; i < answer_vector.size(); i++)
        {
            // std::cerr << "(" << answer_vector[i].first << ", " << answer_vector[i].second << ")\n";
            // std::cerr << "(" << key_value_pairs[i].first << ", " << key_value_pairs[i].second << ")\n";
            if (answer_vector[i] != key_value_pairs[i])
            {
                is_success = false;
            }
        }
    }
    else
    {
        std::cerr << "Sizes: (" << answer_vector.size() << ", " << scanned_size << ")\n";
        is_success = false;
    }

    check(is_success, "testLSMScanTwoPagesDiskOnePageInMemoryOneLevel: Scan LSM tree with 3 pages total.");
    free(buffer_pool);
    free(memtable);
    free(lsm_tree);
    dbClear(current_database);
}

void testLSMScanThreePagesOnDiskTwoLevel()
{
    int db_size = 256;
    std::string current_database = "test_db";
    Memtable *memtable = dbOpen(current_database, db_size);

    BufferPool *buffer_pool = new BufferPool(BUFFER_POOL_MAX_PAGES);

    // Create LSM tree
    LSMTree *lsm_tree = new LSMTree(db_size, current_database, memtable);

    // Put values for SST1
    std::map<long, long> answer_map;
    for (int i = 1; i <= 513; ++i)
    {
        // std::cerr<< "Put index: " << i << "\n";
        lsm_tree->put(i, i * 10);
        answer_map[i] = i * 10;
    }

    std::pair<std::string, std::string> filenames = dbClose(memtable, current_database);
    lsm_tree->insertSST(filenames.first, filenames.second);

    // Setup Expected values to compare
    std::vector<std::pair<long, long>> answer_vector;
    for (const auto &entry : answer_map)
    {
        answer_vector.push_back(entry);
    }

    std::pair<std::pair<long, long> *, int> scanned_pairs = lsm_tree->scan(0, 513, buffer_pool, false);
    std::pair<long, long> *key_value_pairs = scanned_pairs.first;
    int scanned_size = scanned_pairs.second;

    bool is_success = true;
    if (answer_vector.size() == scanned_size)
    {
        for (int i = 0; i < answer_vector.size(); i++)
        {
            // std::cerr << "(" << answer_vector[i].first << ", " << answer_vector[i].second << ")\n";
            // std::cerr << "(" << key_value_pairs[i].first << ", " << key_value_pairs[i].second << ")\n";
            if (answer_vector[i] != key_value_pairs[i])
            {
                is_success = false;
            }
        }
    }
    else
    {
        std::cerr << "Sizes: (" << answer_vector.size() << ", " << scanned_size << ")\n";
        is_success = false;
    }

    check(is_success, "testLSMScanThreePagesOnDiskTwoLevel: Scan LSM tree with 3 pages total");
    free(buffer_pool);
    free(memtable);
    free(lsm_tree);
    dbClear(current_database);
}