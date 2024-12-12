#include "global.h"
#include "xxhash64.h"
#include <string>
#include <unordered_map>
#include <cmath>
#include <vector>
#include <list>
#include <cstring>

#define BUFFER_POOL_MAX_PAGES 10

struct Page
{
    std::string id;
    char data[PAGE_SIZE];
    Page *next;

    Page(std::string id, char data[PAGE_SIZE]) : id(id), next(nullptr)
    {
        std::memcpy(this->data, data, PAGE_SIZE);
    }
    ~Page() {}
};
typedef struct Page Page;

class BufferPool
{
private:
    // Hash table for pages, mapping hash value to pages
    std::unordered_map<int, Page *> page_table;
    // Maximum number of pages allowed in the pool
    long max_pages;
    long curr_num_pages;
    int hashKey(std::string id);

    std::list<Page *> lru;                                            // Tracks the LRU list
    std::unordered_map<Page *, std::list<Page *>::iterator> page_map; // Maps pages to their positions in the lru

public:
    BufferPool(long max_pages);
    ~BufferPool();
    void insertPage(Page *page);
    Page *searchForPage(std::string id);
};