#ifndef SST_H
#define SST_H

#include "global.h"
#include "memtable.h"
#include "static_b_tree.h"
#include "lsm_tree.h"
#include <filesystem>
#include <algorithm>
#include <cmath>
#include <iostream>
#include <chrono>
#include <ctime>
#include <iomanip>
#include <string.h>
#include <string>
#include <fstream>
#include <climits>  // For LONG_MIN and LONG_MAX
#include <fcntl.h>  // for open, O_RDONLY
#include <unistd.h> // for pread, close

std::string getCurrentTimestamp();
std::pair<std::string, std::string> writeMemtableToDisk(Memtable *memtable, std::string database_name);
std::pair<std::string, std::string> writeMemtableToDisk(Memtable *memtable, std::string sst_filename, std::string btree_filename, std::string bloom_filename, std::string database_name);

Memtable *retrieveMemtableFromSST(std::string filename);
std::vector<std::string> getDataFiles(const std::string current_database, std::string prefix);
NodeFileOffset *binarySearch(std::string sstFileName, long key, BufferPool *buffer_pool);
std::vector<std::pair<long, long>> binarySearchScan(const std::string sstFileName, long key1, long key2, BufferPool *buffer_pool);


#endif