#include "memtable.h"
#include "test_memtable.h"
#include <iostream>

// Declare the check function from tests_main.cpp
extern void check(bool condition, const std::string &test_name);

void testMemtableCreation()
{
    Memtable memtable(5);
    // Check if memtable is created correctly
    check(memtable.get(1) == nullptr, "Memtable Creation Test: Initial state should be empty");
}

void testMemtablePutNodes()
{
    Memtable memtable(5);
    memtable.put(1, 100);
    memtable.put(2, 200);

    // Verify insertion
    Node *node = memtable.get(1);
    check(node != nullptr && node->value == 100, "Memtable Put Test: Insert and Get Key 1");

    node = memtable.get(2);
    check(node != nullptr && node->value == 200, "Memtable Put Test: Insert and Get Key 2");
}

void testMemtableGetNodes()
{
    Memtable memtable(5);
    memtable.put(1, 100);
    memtable.put(2, 200);

    // Test getting an existing key
    Node *node = memtable.get(1);
    check(node != nullptr && node->value == 100, "Memtable Get Test: Get Existing Key 1");

    // Test getting a non-existent key
    node = memtable.get(3);
    check(node == nullptr, "Memtable Get Test: Get Non-existent Key 3");
}
