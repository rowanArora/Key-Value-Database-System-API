#include "test_sst.h"

extern void check(bool condition, const std::string &test_name);

void testWriteMemtableToSST()
{
    Memtable *memtable = dbOpen("test_db", 257);

    for (int i = 1; i < 257; ++i)
    {
        memtable->put(i, i * 10);
    }

    std::string sst_filename = DATA_FILE_PATH + "test_db/sst_test_file.bin";
    std::string btree_filename = DATA_FILE_PATH + "test_db/btree_test_file.bin";
    std::string bloom_filename = DATA_FILE_PATH + "test_db/bloom_" + ".bin";

    // Write the Memtable to the SST file and build the B-Tree
    writeMemtableToDisk(memtable, sst_filename, btree_filename, bloom_filename, "test_db");

    // Verify by reading the file back using Direct I/O
    int fd = open(sst_filename.c_str(), O_RDONLY | O_DIRECT);
    if (fd < 0)
    {
        std::cerr << "Error opening SST file for read verification." << std::endl;
        return;
    }

    // Allocate aligned memory for Direct I/O
    void *read_buffer;
    if (posix_memalign(&read_buffer, PAGE_SIZE, PAGE_SIZE) != 0)
    {
        std::cerr << "Error: Memory alignment allocation failed for the SST Buffer." << std::endl;
        close(fd);
        return;
    }
    std::memset(read_buffer, INTERNAL, PAGE_SIZE);

    // Read the first page (assuming data fits within one page)
    ssize_t bytes_read = pread(fd, read_buffer, PAGE_SIZE, 0);
    if (bytes_read < 0)
    {
        std::cerr << "Error reading SST file." << std::endl;
        free(read_buffer);
        close(fd);
        return;
    }

    // Cast buffer data to long pointer for easy access to key-value pairs
    long *buffer_data = static_cast<long *>(read_buffer);

    // Assertions to verify the data
    assert(buffer_data[0] == 1 && buffer_data[1] == 10);         // First pair: {1, 10}
    assert(buffer_data[2] == 2 && buffer_data[3] == 20);         // Second pair: {2, 20}
    assert(buffer_data[4] == 3 && buffer_data[5] == 30);         // Third pair: {3, 30}
    assert(buffer_data[6] == 4 && buffer_data[7] == 40);         // Fourth pair: {4, 40}
    assert(buffer_data[8] == 5 && buffer_data[9] == 50);         // Fifth pair: {5, 50}
    assert(buffer_data[510] == 256 && buffer_data[511] == 2560); // Last pair: {256, 2560}

    // Clean up
    free(read_buffer);
    close(fd);

    std::cout << "Direct I/O SST file verification passed!" << std::endl;

    // char *filename = writeMemtableToDisk(&memtable);
    // std::cerr << filename << "\n";
    // free(filename);
    // Test scan
    // std::pair<std::pair<long, long> *, int> pairArraySize = memtable.scan(1, 5);
    // bool is_success = pairArraySize.second == 5;
    // for (int i = 0; i < pairArraySize.second; i++)
    // {
    //     // std::cout << "First Pair:<" + std::to_string(pairArraySize.first[i].first) + ", " + std::to_string(pairArraySize.first[i].second) + ">\n";
    //     is_success = is_success && (pairArraySize.first[i].first * 10 == pairArraySize.first[i].second);
    // }
    // check(is_success, "Memtable Scan Test: Get all elements in memtable");
}
void testGetFromSST()
{
    std::string current_database = "test_db";
    Memtable *memtable = dbOpen(current_database, 500);

    for (int i = 1; i <= 500; ++i)
    {
        memtable->put(i, i * 10);
    }

    std::pair<std::string, std::string> filenames = dbClose(memtable, current_database);

    BufferPool *buffer_pool = new BufferPool(BUFFER_POOL_MAX_PAGES);

    memtable = dbOpen(current_database, 500);

    bool is_success = true;
    for (int i = 1; i <= 500; i++)
    {
        Node *retrieved_node = memtable->get(i, current_database, buffer_pool)->node;
        if (retrieved_node != nullptr)
        {
            is_success = is_success && (retrieved_node->key * 10 == retrieved_node->value);
        }
        else
        {
            is_success = false;
            break;
        }
    }

    check(is_success, "testGetFromSST: Get Items From SST");

    dbClear(current_database);
}