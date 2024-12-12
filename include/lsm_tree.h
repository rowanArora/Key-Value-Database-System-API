#ifndef LSM_TREE_H
#define LSM_TREE_H

#include "global.h"
#include "memtable.h"
#include "sst.h"
#include <map>
#include <utility>
#include <vector>
#include <string>
#include <ctime>
#include <fcntl.h>  // for open, O_RDONLY
#include <unistd.h> // for pread, close
#include <optional>
#include <cstring>
#include <stdio.h>
#include <climits>

struct SST
{
    int level;
    int level_index;
    std::string sst_filename;
    std::string btree_filename;

    SST(int level, int level_index, std::string &sst_filename, std::string &btree_filename);
    virtual ~SST() = default;
};

class LSMTree
{
private:
    Memtable *memtable;
    std::vector<std::vector<SST>> levels;
    int max_level = MAX_LSM_LEVEL;
    std::string database_name;
    size_t memtable_size;
    size_t level_size_ratio = LEVEL_SIZE_RATIO;

    std::pair<SST &, SST &> fileCompare(SST &sst1, SST &sst2);
    std::pair<std::string, std::string> mergeSSTs(SST &sst1, SST &sst2, bool last_level);

public:
    LSMTree(size_t memtable_size, std::string database, Memtable *memtable);
    ~LSMTree() = default;

    void put(long key, long value);
    void insertSST(std::string sst_filename, std::string btree_filename);
    NodeFileOffset *get(long key, BufferPool *buffer_pool, bool with_btree);
    std::pair<std::pair<long, long> *, int> scan(long key1, long key2, BufferPool *buffer_pool, bool with_btree);
    Memtable *changeMemtable(Memtable *new_memtable);
    void freeMemtable();
    void compactLevels();
    void printLSMTree();
};

#endif