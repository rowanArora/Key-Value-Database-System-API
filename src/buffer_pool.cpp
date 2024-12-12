#include "buffer_pool.h"
#include "xxhash64.h"
#include <iostream>

////////////////////////////////////////////////////////////////////////////
// Define the BufferPool's private methods struct's.
/*
    Returns an index to the hashmap using the XXHash64 hash function.
*/
int BufferPool::hashKey(std::string id)
{
    return XXHash64::hash(id.c_str(), id.size(), 0) % max_pages;
}
////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////
// Define the BufferPool's public methods struct's.
BufferPool::BufferPool(long num_pages)
{
    max_pages = num_pages;
    curr_num_pages = 0;
}

BufferPool::~BufferPool()
{
    // Free all dynamically allocated pages
    for (auto &entry : page_table)
    {
        Page *current = entry.second;
        while (current != nullptr)
        {
            Page *temp = current;
            current = current->next;
            delete temp;
        }
    }
    page_table.clear();
    lru.clear();
    page_map.clear();
}

/*
    Return the page given a page id from the buffer pool. If id is not in
    bufferpool, then we return nullptr.
*/
Page *BufferPool::searchForPage(std::string id)
{
    int hash_index = hashKey(id);

    // Check if hash_index is in the page_table.
    if (page_table.find(hash_index) != page_table.end())
    {
        // Traverse the chain to find the page
        Page *current_page = page_table[hash_index];
        while (current_page != nullptr)
        {
            if (current_page->id == id)
            {
                // Move the page to the front of the LRU list
                lru.splice(lru.begin(), lru, page_map[current_page]);
                return current_page;
            }
            current_page = current_page->next;
        }
    }
    return nullptr; // Page not found
}

/*
    Insert the given page to the buffer pool, and evict if necessary.
*/
void BufferPool::insertPage(Page *page)
{
    if (!page)
        return; // Handle null pointer input

    int hash_index = hashKey(page->id);

    // Check if the page already exists
    if (searchForPage(page->id) != nullptr)
        return; // Page is already in the buffer pool

    // If buffer pool is full, evict the least recently used page
    if (curr_num_pages >= max_pages)
    {
        // LRU Eviction: Remove the least recently used page
        Page *least_recently_used = lru.back();
        lru.pop_back(); // Remove from LRU list
        page_map.erase(least_recently_used); // Remove from map

        int evict_index = hashKey(least_recently_used->id);

        // Remove the evicted page from the hash table
        if (page_table[evict_index] == least_recently_used)
        {
            page_table[evict_index] = least_recently_used->next;
        }
        else
        {
            // Handle chaining removal if not the head
            Page *current_page = page_table[evict_index];
            while (current_page->next != least_recently_used)
            {
                current_page = current_page->next;
            }
            current_page->next = least_recently_used->next;
        }
        delete least_recently_used; // Free memory
        curr_num_pages--;
    }

    // Insert the new page
    page->next = page_table[hash_index]; // Chain the page
    page_table[hash_index] = page;
    curr_num_pages++;

    // Add the page to the front of the LRU list
    lru.push_front(page);
    page_map[page] = lru.begin();
}
////////////////////////////////////////////////////////////////////////////