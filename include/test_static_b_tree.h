#ifndef TEST_STATIC_B_TREE_H
#define TEST_STATIC_B_TREE_H

#include <iostream>
#include <cassert>
#include <filesystem>
#include <ctime>
#include <random>
#include <map>
#include "static_b_tree.h"
#include "sst.h"

// Helper functions for B-Tree tests
void cleanup_test_files();
void createTestSSTWithBTree(int size);

// Individual test functions for B-Tree operations
void testBTreeGet(StaticBTree btree, std::uniform_int_distribution<> distrib, std::mt19937 &gen, BufferPool *buffer_pool);       // Tests the get function for specific keys in the B-Tree file
void testBTreeRangeScan(StaticBTree btree, std::uniform_int_distribution<> distrib, std::mt19937 &gen, BufferPool *buffer_pool); // Tests the range scan function for a range of keys

// Main function to run all B-Tree tests
int testBTreeMain(int memtable_size);

#endif // TEST_STATIC_B_TREE_H
