#include <iostream>

#include "test_memtable.h"
#include "test_sst.h"
#include "test_buffer_pool.h"
#include "test_static_b_tree.h"
#include "test_lsm_tree.h"

// Global counters for test results
int total_tests = 0;
int passed_tests = 0;
int failed_tests = 0;

// Helper function to check test results
void check(bool condition, const std::string &test_name)
{
    total_tests++;
    if (condition)
    {
        // std::cout << "[PASSED] " << test_name << std::endl;
        passed_tests++;
    }
    else
    {
        std::cout << "[FAILED] " << test_name << std::endl;
        failed_tests++;
    }
}

// Change any of the following constants to true to run different tests.
// Step 1.1
const bool test_memtable = false;    // Tests to create Memtables of different sizes.
const bool test_memtablePut = false; // Tests to put Nodes into Memtables of different sizes.
const bool test_memtableGet = false; // Tests to get Nodes from Memtables of different sizes.

// Step 1.2
const bool test_memtableToSST = false; // Tests to convert Memtables of different sizes into SSTs.
const bool test_memtableScan = false;  // Tests to scan through SSTs of different sizes and return a list of Nodes.

// Step 1.3
const bool test_get = false;  // Tests to get Nodes from both Memtables and SSTs of different sizes.
const bool test_scan = false; // Tests to scan through both Memtables and SSTs of different sizes and return a list of Nodes.

// Step 2.3
const bool test_BTree_min_node = true;          // Tests for a B-Tree with a very tiny Leaf Node
const bool test_BTree_leaf_node = true;         // Tests for a B-Tree with a single Leaf Node
const bool test_BTree_internal_node = true;     // Tests for a B-Tree with a single Internal Node and Two Leaf Nodes
const bool test_BTree_internal_node_max = true; // Tests for a B-Tree with a single Internal Node and 256 Leaf Nodes
const bool test_BTree_multiple_nodes = false;   // Tests for a B-Tree with one layer of Internal Nodes

// Step 3.1
const bool test_lsm_tree_scan = true;

int main(int argc, char *argv[])
{
    std::cout << "Running all unit tests..." << std::endl;

    if (test_memtable)
    {
        std::cout << "\nTesting Memtable creation..." << std::endl;
        testMemtableCreation();
    }

    if (test_memtablePut)
    {
        std::cout << "\nTesting putting Nodes to the Memtable..." << std::endl;
        testMemtablePutNodes();
    }

    if (test_memtableGet)
    {
        std::cout << "\nTesting getting Nodes from the Memtable..." << std::endl;
        testMemtableGetNodes();
    }

    if (test_memtableToSST)
    {
        std::cout << "\nTesting conversion from Memtable to SST..." << std::endl;
        // Add your testing function here.
        testWriteMemtableToSST();
    }

    // if (test_memtableScan)
    // {
    //     std::cout << "\nTesting scanning Nodes from the Memtable..." << std::endl;
    //     // Add your testing function here.
    //     test_scanMemtableEmpty();
    //     test_scanMemtableAllInRange();
    // }

    if (test_get)
    {
        std::cout << "\nTesting getting Nodes from the Memtable and all SSTs..." << std::endl;
        // Add your testing function here.
        testGetFromSST();
    }

    if (test_scan)
    {
        std::cout << "\nTesting scanning Nodes from the Memtable and all SSTs..." << std::endl;
        // Add your testing function here.
    }

    if (test_BTree_min_node)
    {
        std::cout << "\nTesting B-Tree with a Tiny Leaf Node..." << std::endl;
        int n = testBTreeMain(16);
    }

    if (test_BTree_leaf_node)
    {
        std::cout << "\nTesting B-Tree with a Single Leaf Node..." << std::endl;
        int n = testBTreeMain(256);
    }

    if (test_BTree_internal_node)
    {
        std::cout << "\nTesting B-Tree with a Single Internal Node and Two Leaf Nodes..." << std::endl;
        int n = testBTreeMain(512);
    }

    if (test_BTree_internal_node_max)
    {
        std::cout << "\nTesting B-Tree with a Single Internal Node and 256 Leaf Nodes..." << std::endl;
        int n = testBTreeMain(65536);
    }

    if (test_BTree_multiple_nodes)
    {
        std::cout << "\nTesting B-Tree a Layer of Internal Nodes..." << std::endl;
        int n = testBTreeMain(131072);
    }

    if (test_lsm_tree_scan)
    {
        std::cout << "\nTesting LSM Scan SSTs with two pages..." << std::endl;
        testLSMScanTwoPage();
        std::cout << "\nTesting LSM Scan SSTs with two pages on disk and one page on memory..." << std::endl;
        testLSMScanTwoPagesDiskOnePageInMemoryOneLevel();
        std::cout << "\nTesting LSM Scan SSTs with three pages on disk..." << std::endl;
        testLSMScanThreePagesOnDiskTwoLevel();
    }

    std::cout << "\nFinished running all unit tests..." << std::endl;
    std::cout << "\nTotal Number of Tests: " << total_tests << std::endl;
    std::cout << "\nNumber of Tests Passed: " << passed_tests << ", meaning a " << (passed_tests / total_tests) * 100 << "% success rate!" << std::endl;
    std::cout << "\nNumber of Tests Failed: " << failed_tests << ", meaning a " << (failed_tests / total_tests) * 100 << "% failure rate!" << std::endl;
    return 0;
}