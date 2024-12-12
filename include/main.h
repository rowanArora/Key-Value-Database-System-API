#ifndef MAIN_H
#define MAIN_H
#include <iostream>
#include <string>
#include <regex>
#include <filesystem>
#include <vector>
#include <tuple>
#include <utility>
#include <set>
#include <climits> // For LONG_MIN and LONG_MAX
#include "global.h"
#include "memtable.h"
#include "static_b_tree.h"
#include "lsm_tree.h"
#define BUFFER_POOL_MAX_PAGES 10

std::pair<std::string, std::string> put(Memtable *memtable, std::string database_name, LSMTree *lsm_tree, int memtable_size, long key, long value);
Memtable *dbOpen(std::string database_name, int memtable_size);

std::pair<std::string, std::string> close(Memtable *current_memtable, std::string current_database, LSMTree *lsm_tree);

#endif