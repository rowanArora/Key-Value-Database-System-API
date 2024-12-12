#include "sst.h"
#include "bloom_filter.h"
////////////////////////////////////////////////////////////////////////////
/*
    Writes a given memtable to a sorted string table (SST), into a file with the
    following name format "SST_<timestamp>", where timestamp is the time the SST
    was created and the format is YYYYMMDD_HHMMSS. Return the relative filepath
    of the file that was created in this function, the filename char[] must be
    freed by the user.

    Example: "SST_20240926_110323.bin"

    This function assumes that the memtable is ready to be written to a sorted
    file (i.e. The memtable has reached its max capacity OR database closing.)
*/

std::string getCurrentTimestamp()
{
    // Get current time as a time_point
    auto now = std::chrono::system_clock::now();

    // Convert to time_t to extract calendar components
    std::time_t now_time_t = std::chrono::system_clock::to_time_t(now);

    // Convert to milliseconds for finer granularity
    auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()) % 1000;

    // Convert to local time structure
    std::tm local_time;
    localtime_r(&now_time_t, &local_time);

    // Format the time into a string
    std::ostringstream oss;
    oss << std::put_time(&local_time, "%Y%m%d_%H%M%S") << "_" << std::setfill('0') << std::setw(3) << now_ms.count();
    return oss.str();
}

std::pair<std::string, std::string> writeMemtableToDisk(Memtable *memtable, std::string database_name)
{
    std::string string_time_now = getCurrentTimestamp();

    std::string sst_filename = DATA_FILE_PATH + database_name + "/sst_" + string_time_now + ".bin";
    std::string btree_filename = DATA_FILE_PATH + database_name + "/btree_" + string_time_now + ".bin";
    std::string bloom_filename = DATA_FILE_PATH + database_name + "/bloom_" + string_time_now + ".bin";

    last_known_database = database_name;

    return writeMemtableToDisk(memtable, sst_filename, btree_filename, bloom_filename, database_name);
}

/*
    Writes a given memtable to a sorted string table (SST), into a file with the
    given filename which should be of format "<path>SST_<timestamp>", where path
    is path to the file and timestamp is the time the SST was created and the
    format is YYYYMMDD_HHMMSS. Return the relative filepath of the file that was
    created in this function, the filename char[] must be freed by the user.

    This function assumes that the memtable is ready to be written to a sorted
    file (i.e. The memtable has reached its max capacity OR database closing.)
*/
std::pair<std::string, std::string> writeMemtableToDisk(Memtable *memtable, std::string sst_filename, std::string btree_filename, std::string bloom_filename, std::string database_name)
{
    // Get all key-value pairs in the memtable.
    // Initialize Bloom filter
    size_t bloom_size = 2400; // Example size (can be tuned based on expected key count)
    int num_hashes = 3;       // Number of hash functions
    BloomFilter bloom_filter(bloom_size, num_hashes);

    // Get all key value pairs in memtable.
    std::pair<std::pair<long, long> *, int> pair_array_size = memtable->scan(LONG_MIN, LONG_MAX);
    std::pair<long, long> *key_value_pairs = pair_array_size.first;
    int size = pair_array_size.second;

    // Open the SST file for writing with Direct I/O
    int sst_fd = open(sst_filename.c_str(), O_WRONLY | O_CREAT | O_DIRECT, 0666);
    if (sst_fd < 0)
    {
        std::cerr << "Write memtable Error: Failed to open SST file for writing." << std::endl;
        return {sst_filename, btree_filename};
    }

    // Open B-Tree file for writing with Direct I/O
    int btree_fd = open(btree_filename.c_str(), O_WRONLY | O_CREAT | O_DIRECT, 0666);
    if (btree_fd < 0)
    {
        std::cerr << "Error: Failed to open B-Tree file for writing." << std::endl;
        return {sst_filename, btree_filename};
    }

    // Alignment and buffer size requirements for Direct I/O
    const size_t buffer_size = PAGE_SIZE;

    // Create the buffer to write to for the SST
    void *sst_buffer;
    if (posix_memalign(&sst_buffer, PAGE_SIZE, buffer_size) != 0)
    {
        std::cerr << "Error: Memory alignment allocation failed for the SST Buffer." << std::endl;
        close(sst_fd);
        return {sst_filename, btree_filename};
    }
    std::memset(sst_buffer, INTERNAL, buffer_size);

    // Create the buffer to write to for the B-Tree
    void *btree_buffer;
    if (posix_memalign(&btree_buffer, PAGE_SIZE, buffer_size) != 0)
    {
        std::cerr << "Error: Memory alignment allocation failed for the B-Tree Buffer." << std::endl;
        close(btree_fd);
        return {sst_filename, btree_filename};
    }

    // Offsets for writing the SST
    size_t sst_write_offset = 0;
    size_t sst_buffer_offset = 0;

    // Variables to create the B-Tree and write it to disk
    StaticBTree btree(sst_filename, btree_filename);
    size_t btree_write_offset = 0;

    // Variables to create the B-Tree
    int leaf_node_pairs_written = 0;
    long curr_page = 0;
    long final_key_added;
    int leaf_nodes_written = 0;

    // Write key-value pairs to the SST file
    for (int i = 0; i < size; ++i)
    {
        long key = key_value_pairs[i].first;
        long value = key_value_pairs[i].second;

        bloom_filter.put(std::to_string(key));

        // Copy the key and value into the aligned buffer at the current buffer offset
        std::memcpy(static_cast<char *>(sst_buffer) + sst_buffer_offset, &key, sizeof(key));
        sst_buffer_offset += sizeof(key);
        std::memcpy(static_cast<char *>(sst_buffer) + sst_buffer_offset, &value, sizeof(value));
        sst_buffer_offset += sizeof(value);

        // If the buffer is full, write it to the SST file
        if (sst_buffer_offset + ENTRY_SIZE > buffer_size)
        {
            ssize_t bytes_written = pwrite(sst_fd, sst_buffer, buffer_size, sst_write_offset);
            if (bytes_written != buffer_size)
            {
                std::cerr << "Error: Incomplete write for key-value pairs batch." << std::endl;
                break;
            }
            sst_write_offset += bytes_written;
            sst_buffer_offset = 0;                             // Reset the buffer for the next batch
            std::memset(sst_buffer, INTERNAL, buffer_size); // Clear the buffer
        }

        // Incremenet the number of key-value pairs written to the Leaf Node and assign key to final_key_added in case this Leaf Node will not be entirely filled
        leaf_node_pairs_written++;
        final_key_added = key;

        // When a Leaf Node is full, i.e., when leaf_node_pairs_written is equal to MAX_PAIRS, add the most recently added key (the max key in the Leaf Node) to max_keys_in_leaves and reset both leaf_node_pairs_written and write_leaf_node
        if (leaf_node_pairs_written == MAX_PAIRS)
        {
            curr_page++;
            btree.insertInternalNode(key, curr_page);
            leaf_node_pairs_written = 0;
            leaf_nodes_written++;
        }
    }

    // After processing all key-value pairs, check if there is remaining data in the buffer
    if (sst_buffer_offset > 0)
    {
        // Pad the remaining buffer space with -1
        std::memset(static_cast<char *>(sst_buffer) + sst_buffer_offset, INTERNAL, buffer_size - sst_buffer_offset);

        if (sst_buffer_offset % PAGE_SIZE != 0) {
            // Set the last long in the buffer to LEAF
            long *buffer_as_longs = static_cast<long *>(sst_buffer);
            int total_longs_in_buffer = buffer_size / sizeof(long);
            buffer_as_longs[total_longs_in_buffer - 2] = LEAF;
        }

        // Write the padded buffer to the SST file
        ssize_t bytes_written = pwrite(sst_fd, sst_buffer, buffer_size, sst_write_offset);
        if (bytes_written != buffer_size)
        {
            std::cerr << "Error: Incomplete write for final buffer." << std::endl;
        }
        sst_write_offset += bytes_written;

        curr_page++;
        btree.insertInternalNode(final_key_added, curr_page);
    }

    // Clean up SST resources
    free(sst_buffer);
    close(sst_fd);

    // Finalize the B-Tree and write Internal Nodes to the B-Tree file
    if (btree.getNodes().size() > 0 && leaf_nodes_written > 1)
    {
        btree.finalizeTree();
        btree.writeNodes(btree_fd, btree_buffer, btree_write_offset);
    }

    // Clean up B-Tree resources
    free(btree_buffer);
    close(btree_fd);

    // Return the SST and B-Tree filenames
    // Serialize the Bloom filter to a separate file
    bloom_filter.serialize(bloom_filename);

    return {sst_filename, btree_filename};
}

////////////////////////////////////////////////////////////////////////////
NodeFileOffset *binarySearch(const std::string sst_filename, long key, BufferPool *buffer_pool)
{
    int fd = open(sst_filename.c_str(), O_RDONLY | O_DIRECT, 0666);
    if (fd < 0)
    {
        std::cerr << "Error: Unable to open SST file " << sst_filename << std::endl;
        return nullptr;
    }
    // Get the file size
    off_t file_size = lseek(fd, 0, SEEK_END);
    long entries = file_size / ENTRY_SIZE;

    long start = 0;
    long end = entries - 1;

    // Page buffer to store key-value pairs during each read
    char page_buffer[PAGE_SIZE];
    // Initialize buffer to INTERNAL values
    std::memset(page_buffer, INTERNAL, PAGE_SIZE);

    while (start <= end)
    {
        long mid = start + (end - start) / 2;

        // off_t page_offset = (mid * ENTRY_SIZE / PAGE_SIZE) * PAGE_SIZE;
        off_t page_offset = (mid / (PAGE_SIZE / ENTRY_SIZE)) * PAGE_SIZE;

        size_t entries_page;
        // First check if the  page is already in buffer pool
        std::string filename_offset = sst_filename + std::to_string(page_offset);

        Page *possible_page = buffer_pool->searchForPage(filename_offset);
        if (possible_page != nullptr)
        {
            // If the page is in the buffer pool then we get the page from the buffer pool.
            std::memcpy(page_buffer, possible_page->data, PAGE_SIZE);
            entries_page = PAGE_SIZE / ENTRY_SIZE;
        }
        else
        {
            // Use pread to read one page starting from page_offset
            ssize_t read_page = pread(fd, page_buffer, PAGE_SIZE, page_offset);
            if (read_page < 0)
            {
                std::cerr << "Error: Failed to read page in SST file " << sst_filename << std::endl;
                close(fd);
                return nullptr;
            }
            // Determine the number of entries in the page
            entries_page = read_page / ENTRY_SIZE;
            // Store page in buffer
            Page *page = new Page(sst_filename + std::to_string(page_offset), page_buffer);
            buffer_pool->insertPage(page);
        }

        // Determine if the input key exists in this page by comparing it to the smallest and largest keys in it
        long smallest_key = 0, largest_key = 0;
        std::memcpy(&smallest_key, page_buffer, sizeof(long));                                   // First key
        std::memcpy(&largest_key, page_buffer + ((entries_page - 1) * ENTRY_SIZE), sizeof(long)); // Last key


        if (largest_key < 0) {
            char *curr_offset = page_buffer;
            // Go through the key-value pairs in the page.
            for (size_t i = 0; i < entries_page; i++)
            {
                long curr_key = 0;
                memcpy(&curr_key, curr_offset, sizeof(long));
                curr_offset += sizeof(long);
                long curr_val = 0;
                memcpy(&curr_val, curr_offset, sizeof(long));
                curr_offset += sizeof(long);

                if (curr_key == key)
                {
                    close(fd);
                    NodeFileOffset *ret = new NodeFileOffset(new Node(curr_key, curr_val), sst_filename, page_offset);
                    return ret; // Key found, return node
                }

                // If query key is smaller than smallest key in this page, then go to another page.
                if (curr_key > key || curr_key < 0)
                {
                    end = mid - 1;
                    break;
                }
                // If query key is not in this page, then go to another page.
                else if (i == entries_page - 1)
                {
                    start = mid + 1;
                }
            }
        }
        else if (key < smallest_key)
        {
            end = mid - 1;
            continue;
        }
        else if (key > largest_key && largest_key >= 0)
        {
            start = mid + 1;
            continue;
        }
        else
        {
            int left = 0;
            int right = entries_page - 1;
            while (left <= right) 
            {
                int mid = left + (right - left) / 2;
                char *curr_offset = page_buffer + mid * ENTRY_SIZE;
                long key_at_mid;
                memcpy(&key_at_mid, curr_offset, sizeof(long));

                if (key_at_mid >= smallest_key && key_at_mid <= largest_key) 
                { // Valid key
                    if (key_at_mid == key) 
                    {
                        long val_at_mid;
                        memcpy(&val_at_mid, curr_offset + sizeof(long), sizeof(long));
                        close(fd);
                        NodeFileOffset *ret = new NodeFileOffset(new Node(key_at_mid, val_at_mid), sst_filename, page_offset);
                        return ret; // Key found, return node
                    } 
                    else if (key_at_mid < key) 
                    {
                        left = mid + 1; // Search in the right half
                    } 
                    else 
                    {
                        right = mid - 1; // Search in the left half
                    }
                } 
                else if (mid == MAX_PAIRS - 1 && key_at_mid < 0) 
                {
                    // If key is greater than all valid keys, direct to the last child
                    long val_at_mid;
                    memcpy(&val_at_mid, curr_offset + sizeof(long), sizeof(long));
                    close(fd);
                    NodeFileOffset *ret = new NodeFileOffset(new Node(key_at_mid, val_at_mid), sst_filename, page_offset);
                    return ret; // Key found, return node
                }
            }

            std::cerr << "Error: Failed to find key in this SST. " << std::endl;
            close(fd);
            return nullptr;
        }
    }

    close(fd);
    return nullptr; // Key not found
}

std::vector<std::pair<long, long>> binarySearchScan(const std::string sst_filename, long key1, long key2, BufferPool *buffer_pool)
{
    int fd = open(sst_filename.c_str(), O_RDONLY | O_DIRECT, 0666);
    if (fd < 0)
    {
        std::cerr << "Error: Unable to open SST file " << sst_filename << std::endl;
        return {};
    }

    off_t file_size = lseek(fd, 0, SEEK_END);
    long total_entries = file_size / ENTRY_SIZE;

    long start = 0;
    long end = total_entries - 1;

    char page_buffer[PAGE_SIZE];
    std::memset(page_buffer, INTERNAL, PAGE_SIZE);

    long first_in_range = -1; // Index of the first key in range
    std::vector<std::pair<long, long>> results;

    // Step 1: Use binary search to find the first key within the range
    while (start <= end)
    {
        long mid = start + (end - start) / 2;
        off_t page_offset = (mid * ENTRY_SIZE / PAGE_SIZE) * PAGE_SIZE;

        Page *cached_page = buffer_pool->searchForPage(sst_filename + std::to_string(page_offset));
        if (cached_page != nullptr)
        {
            std::memcpy(page_buffer, cached_page->data, PAGE_SIZE);
        }
        else
        {
            ssize_t bytes_read = pread(fd, page_buffer, PAGE_SIZE, page_offset);
            if (bytes_read < 0)
            {
                std::cerr << "Error: Failed to read SST file " << sst_filename << std::endl;
                close(fd);
                return {};
            }
            Page *new_page = new Page(sst_filename + std::to_string(page_offset), page_buffer);
            buffer_pool->insertPage(new_page);
        }

        char *current_offset = page_buffer;
        size_t entries_per_page = PAGE_SIZE / ENTRY_SIZE;

        for (size_t i = 0; i < entries_per_page; i++)
        {
            long current_key;
            memcpy(&current_key, current_offset, sizeof(long));
            current_offset += sizeof(long);

            if (current_key < 0)
                break;

            if (current_key >= key1)
            {
                first_in_range = mid; // First key in range found
                end = mid - 1;      // Narrow search for the lower bound
                break;
            }

            if (i == entries_per_page - 1)
                start = mid + 1;
        }
    }

    // Step 2: Collect all keys and values in the range sequentially
    if (first_in_range != -1)
    {
        long index = first_in_range;
        while (index < total_entries)
        {
            off_t page_offset = (index * ENTRY_SIZE / PAGE_SIZE) * PAGE_SIZE;

            Page *cached_page = buffer_pool->searchForPage(sst_filename + std::to_string(page_offset));
            if (cached_page != nullptr)
            {
                std::memcpy(page_buffer, cached_page->data, PAGE_SIZE);
            }
            else
            {
                ssize_t bytes_read = pread(fd, page_buffer, PAGE_SIZE, page_offset);
                if (bytes_read < 0)
                {
                    std::cerr << "Error: Failed to read SST file " << sst_filename << std::endl;
                    break;
                }
                Page *new_page = new Page(sst_filename + std::to_string(page_offset), page_buffer);
                buffer_pool->insertPage(new_page);
            }

            char *current_offset = page_buffer;
            size_t entries_per_page = PAGE_SIZE / ENTRY_SIZE;

            for (size_t i = 0; i < entries_per_page; i++)
            {
                long current_key;
                memcpy(&current_key, current_offset, sizeof(long));
                current_offset += sizeof(long);
                long current_val;
                memcpy(&current_val, current_offset, sizeof(long));
                current_offset += sizeof(long);

                if (current_key < 0)
                    break;

                if (current_key > key2)
                {
                    close(fd);
                    return results;
                }

                if (current_key >= key1)
                {
                    results.emplace_back(current_key, current_val);
                }
            }

            index += PAGE_SIZE / ENTRY_SIZE;
        }
    }

    close(fd);
    return results;
}

// Function to get SST files
std::vector<std::string> getDataFiles(const std::string current_database, std::string prefix)
{
    std::vector<std::string> sst_files;
    std::string directory = DATA_FILE_PATH + current_database; // Path to the SST directory

    // List all SST files in the directory
    for (const auto &entry : std::filesystem::directory_iterator(directory))
    {

        if (entry.path().extension() == ".bin")
        {
            // Check if the prefix is correct (either sst or btree)
            std::string filename = entry.path().filename().string();

            if (filename.rfind(prefix, 0) == 0)
            {
                // std::cout << filename << std::endl;
                sst_files.push_back(entry.path().string());
            }
        }
    }

    // Sort the files in reverse order so that we can access youngest to oldest file
    std::sort(sst_files.begin(), sst_files.end(), std::greater<std::string>());
    return sst_files;
}

////////////////////////////////////////////////////////////////////////////