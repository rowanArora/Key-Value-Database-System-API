#ifndef TEST_HELPERS_H
#define TEST_HELPERS_H

#include "memtable.h"
#include "sst.h"

#include <utility>

Memtable *dbOpen(std::string database_name, int memtable_size);
std::pair<std::string, std::string> dbClose(Memtable *current_memtable, std::string current_database);
void dbClear(std::string target_directory);

#endif