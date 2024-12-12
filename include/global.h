#ifndef GLOBALS_H
#define GLOBALS_H

#include <string>

// Paths
extern std::string last_known_database;
const std::string DATA_FILE_PATH = "./../data/";

// Constants
const long INTERNAL = -1;
const long LEAF = -2;

// Page and Entry Sizes
const size_t PAGE_SIZE = 4096;                         // Page size in bytes for SST reads
const size_t ENTRY_SIZE = sizeof(long) + sizeof(long); // Each entry has a key and value (8 bytes each)
const size_t MAX_PAIRS = PAGE_SIZE / ENTRY_SIZE;       // Maximum key-value pairs per page

// LSM Tree Configuration
const size_t LEVEL_SIZE_RATIO = 2; // Level size ratio for LSM tree
const size_t MAX_LSM_LEVEL = 5;    // Maximum levels in LSM tree

// Buffer Pool and Memory Configuration
const size_t MEGABYTE = 1024 * 1024;
const size_t GIGABYTE = MEGABYTE * 1024;
const size_t BUFFER_POOL_SIZE = (10 * MEGABYTE) / PAGE_SIZE; // 10 MB
const size_t MEMTABLE_SIZE = MEGABYTE;                       // 1 MB memtable size

// Experiment Parameters
const size_t DATA_SIZE = 1 * MEGABYTE * 1024;      // 1 GB total data size for experiment
const size_t MEASUREMENT_INTERVAL = 10 * MEGABYTE; // Measure every 10 MB of data inserted

// B-Tree Configuration
const size_t BTREE_PAGE_SIZE = sizeof(bool)                // is_leaf flag
                               + sizeof(int)               // num_keys count
                               + sizeof(long) * MAX_PAIRS  // array of keys
                               + sizeof(long) * MAX_PAIRS; // array of pages or values

#endif // GLOBALS_H
