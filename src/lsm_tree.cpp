#include "lsm_tree.h"
#include "bloom_filter.h"
////////////////////////////////////////////////////////////////////////////
// Define the SST struct's constructor and destructor.
SST::SST(int level, int level_index, std::string &sst_filename, std::string &btree_filename)
    : level(level), level_index(level_index), sst_filename(sst_filename), btree_filename(btree_filename) {}

// SST::~SST() {}
////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////
// Define the LSMTree class's constructor and destructor.
LSMTree::LSMTree(size_t m_s, std::string database, Memtable *memtable)
    : memtable_size(m_s), database_name(database), memtable(memtable)
{
    for (int i = 0; i < max_level; i++)
    {
        levels.emplace_back();
    }
}

// LSMTree::~LSMTree() {}
////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////
// Implement all of the LSMTree class's private functions.
// Implementation of the fileCompare function.
std::pair<SST &, SST &> LSMTree::fileCompare(SST &sst1, SST &sst2)
{
    return sst1.sst_filename < sst2.sst_filename ? std::pair<SST &, SST &>{sst1, sst2} : std::pair<SST &, SST &>{sst2, sst1};
}

void freeMergeSSTsResources(int fd1, int fd2, int fd3, int fd4, void *buffer1, void *buffer2, void *buffer3, void *buffer4)
{
    if (fd1 >= 0)
    {
        close(fd1);
    }
    if (fd2 >= 0)
    {
        close(fd2);
    }
    if (fd3 >= 0)
    {
        close(fd3);
    }
    if (fd4 >= 0)
    {
        close(fd4);
    }

    if (buffer1 != nullptr)
    {
        free(buffer1);
        buffer1 = nullptr;
    }
    if (buffer2 != nullptr)
    {
        free(buffer2);
        buffer2 = nullptr;
    }
    if (buffer3 != nullptr)
    {
        free(buffer3);
        buffer3 = nullptr;
    }
    if (buffer4 != nullptr)
    {
        free(buffer4);
        buffer4 = nullptr;
    }

    return;
}

/*
    Merges the two given SSTs together. If successful then return new merged SST
    filename. If unsuccessful then return empty string.
*/
std::pair<std::string, std::string> LSMTree::mergeSSTs(SST &sst1, SST &sst2, bool last_level)
{
    // Prepare all needed Resources
    int fd1 = -1;
    int fd2 = -1;
    int fd3 = -1;
    int fd4 = -1;
    void *read_buffer_1 = nullptr;
    void *read_buffer_2 = nullptr;
    void *write_buffer_SST = nullptr;
    void *write_buffer_BTree = nullptr;
    // Open two sst files
    fd1 = open(sst1.sst_filename.c_str(), O_RDONLY | O_DIRECT);
    if (fd1 < 0)
    {
        std::cerr << "Read SST Error: Failed to open SST file - " << sst1.sst_filename << " for read." << std::endl;
        freeMergeSSTsResources(fd1, fd2, fd3, fd4, read_buffer_1, read_buffer_2, write_buffer_SST, write_buffer_BTree);
        return {"", ""};
    }

    fd2 = open(sst2.sst_filename.c_str(), O_RDONLY | O_DIRECT);
    if (fd2 < 0)
    {
        std::cerr << "Read SST Error: Failed to open SST file - " << sst2.sst_filename << " for read." << std::endl;
        freeMergeSSTsResources(fd1, fd2, fd3, fd4, read_buffer_1, read_buffer_2, write_buffer_SST, write_buffer_BTree);
        return {"", ""};
    }
    // Prepare new filenames
    std::string string_time_now = getCurrentTimestamp();
    std::string new_sst_filename = DATA_FILE_PATH + database_name + "/sst_" + string_time_now + ".bin";
    std::string new_btree_filename = DATA_FILE_PATH + database_name + "/btree_" + string_time_now + ".bin";
    std::string bloom_filter_filename = DATA_FILE_PATH + database_name + "/bloom_" + string_time_now + ".bin";

    // Prepare new files to write to
    fd3 = open(new_sst_filename.c_str(), O_WRONLY | O_CREAT | O_DIRECT, 0666);
    fd4 = open(new_btree_filename.c_str(), O_WRONLY | O_CREAT | O_DIRECT, 0666);

    if (fd3 < 0)
    {
        perror("open failed");
        std::cerr << "Read SST Error: Failed to open SST file for read." << std::endl;
        freeMergeSSTsResources(fd1, fd2, fd3, fd4, read_buffer_1, read_buffer_2, write_buffer_SST, write_buffer_BTree);
        return {"", ""};
    }

    if (fd4 < 0)
    {
        perror("open failed");
        std::cerr << "Read B-Tree Error: Failed to open B-Tree file for read." << std::endl;
        freeMergeSSTsResources(fd1, fd2, fd3, fd4, read_buffer_1, read_buffer_2, write_buffer_SST, write_buffer_BTree);
        return {"", ""};
    }

    // Alignment and buffer size requirements for Direct I/O
    const size_t buffer_size = PAGE_SIZE;

    if (posix_memalign(&read_buffer_1, PAGE_SIZE, buffer_size) != 0)
    {
        std::cerr << "Error: Memory alignment allocation failed." << std::endl;
        freeMergeSSTsResources(fd1, fd2, fd3, fd4, read_buffer_1, read_buffer_2, write_buffer_SST, write_buffer_BTree);
        return {"", ""};
    }

    if (posix_memalign(&read_buffer_2, PAGE_SIZE, buffer_size) != 0)
    {
        std::cerr << "Error: Memory alignment allocation failed." << std::endl;
        freeMergeSSTsResources(fd1, fd2, fd3, fd4, read_buffer_1, read_buffer_2, write_buffer_SST, write_buffer_BTree);
        return {"", ""};
    }

    if (posix_memalign(&write_buffer_SST, PAGE_SIZE, buffer_size) != 0)
    {
        std::cerr << "Error: Memory alignment allocation failed." << std::endl;
        freeMergeSSTsResources(fd1, fd2, fd3, fd4, read_buffer_1, read_buffer_2, write_buffer_SST, write_buffer_BTree);
        return {"", ""};
    }

    if (posix_memalign(&write_buffer_BTree, PAGE_SIZE, buffer_size) != 0)
    {
        std::cerr << "Error: Memory alignment allocation failed." << std::endl;
        freeMergeSSTsResources(fd1, fd2, fd3, fd4, read_buffer_1, read_buffer_2, write_buffer_SST, write_buffer_BTree);
        return {"", ""};
    }

    off_t read_page_offset_1 = 0;
    off_t read_page_offset_2 = 0;
    off_t sst_write_page_offset = 0;

    off_t sst_write_offset = 0;

    off_t read_file_size1 = lseek(fd1, 0, SEEK_END);
    off_t read_file_size2 = lseek(fd2, 0, SEEK_END);

    // std::cerr << read_file_size1 << ", " << read_file_size2 << "\n";

    // Initially read in a buffer size worth of data from first SSTs
    ssize_t read_page1 = pread(fd1, read_buffer_1, PAGE_SIZE, read_page_offset_1);
    if (read_page1 < 0)
    {
        std::cerr << "Error: Failed to read page in SST file " << sst1.sst_filename << std::endl;
        freeMergeSSTsResources(fd1, fd2, fd3, fd4, read_buffer_1, read_buffer_2, write_buffer_SST, write_buffer_BTree);
        return {"", ""};
    }
    read_page_offset_1 += read_page1;

    // Intially read in a buffer size worth of data from second SSTs
    ssize_t read_page2 = pread(fd2, read_buffer_2, PAGE_SIZE, read_page_offset_2);
    if (read_page2 < 0)
    {
        std::cerr << "Error: Failed to read page in SST file " << sst2.sst_filename << std::endl;
        freeMergeSSTsResources(fd1, fd2, fd3, fd4, read_buffer_1, read_buffer_2, write_buffer_SST, write_buffer_BTree);
        return {"", ""};
    }
    read_page_offset_2 += read_page2;

    long *long_buffer_1 = static_cast<long *>(read_buffer_1);
    long *long_buffer_2 = static_cast<long *>(read_buffer_2);

    // Calculate the number of long values
    size_t num_longs = buffer_size / sizeof(long);

    int buffer_1_Index = 0;
    int buffer_2_Index = 0;

    // Variables to create the B-Tree and write it to disk
    StaticBTree btree(new_sst_filename, new_btree_filename);
    size_t btree_write_offset = 0;

    // Variables to create the B-Tree
    int leaf_node_pairs_written = 0;
    long curr_page = 0;
    long final_key_added;
    int leaf_nodes_written = 0;

    // Initialize Bloom filter
    size_t bloom_size = 2400; // Example size
    int num_hashes = 3;       // Number of hash functions
    BloomFilter bloom_filter(bloom_size, num_hashes);

    // Run main while loop which will continually read data from both SSTS
    while (true)
    {
        long key1 = long_buffer_1[buffer_1_Index];
        long key2 = long_buffer_2[buffer_2_Index];

        // If both of the next data is valid then check which key to put in first
        if (key1 >= 0 && key2 >= 0)
        {
            // If key2 is greater than add key1 and value1 first
            if (key1 < key2)
            {
                long value1 = long_buffer_1[buffer_1_Index + 1];
                // Make sure to check tombstones if its the last level
                if (!(last_level && (value1 == LONG_MIN)))
                {

                    std::memcpy(static_cast<char *>(write_buffer_SST) + sst_write_page_offset, &key1, sizeof(key1));
                    sst_write_page_offset += sizeof(key1);
                    std::memcpy(static_cast<char *>(write_buffer_SST) + sst_write_page_offset, &value1, sizeof(value1));
                    sst_write_page_offset += sizeof(value1);
                    bloom_filter.put(std::to_string(key1));

                    // Incremenet the number of key-value pairs written to the Leaf Node and assign key to final_key_added in case this Leaf Node will not be entirely filled
                    leaf_node_pairs_written++;
                    final_key_added = key1;

                    // When a Leaf Node is full, i.e., when leaf_node_pairs_written is equal to MAX_PAIRS, add the most recently added key (the max key in the Leaf Node) to max_keys_in_leaves and reset both leaf_node_pairs_written and write_leaf_node
                    if (leaf_node_pairs_written == MAX_PAIRS)
                    {
                        curr_page++;
                        btree.insertInternalNode(key1, curr_page);
                        leaf_node_pairs_written = 0;
                        leaf_nodes_written++;
                    }
                }
                buffer_1_Index += 2;
            }
            // If they are equal then we must take the newer one but still advance both
            // In this case the data in buffer2 is newer thus we take from buffer2
            else if (key1 == key2)
            {
                long value2 = long_buffer_2[buffer_2_Index + 1];
                // Make sure to check tombstones if its the last level
                if (!(last_level && (value2 == LONG_MIN)))
                {

                    std::memcpy(static_cast<char *>(write_buffer_SST) + sst_write_page_offset, &key2, sizeof(key2));
                    sst_write_page_offset += sizeof(key2);
                    std::memcpy(static_cast<char *>(write_buffer_SST) + sst_write_page_offset, &value2, sizeof(value2));
                    sst_write_page_offset += sizeof(value2);
                    bloom_filter.put(std::to_string(key2));

                    // Incremenet the number of key-value pairs written to the Leaf Node and assign key to final_key_added in case this Leaf Node will not be entirely filled
                    leaf_node_pairs_written++;
                    final_key_added = key2;

                    // When a Leaf Node is full, i.e., when leaf_node_pairs_written is equal to MAX_PAIRS, add the most recently added key (the max key in the Leaf Node) to max_keys_in_leaves and reset both leaf_node_pairs_written and write_leaf_node
                    if (leaf_node_pairs_written == MAX_PAIRS)
                    {
                        curr_page++;
                        btree.insertInternalNode(key2, curr_page);
                        leaf_node_pairs_written = 0;
                        leaf_nodes_written++;
                    }
                }
                buffer_1_Index += 2;
                buffer_2_Index += 2;
            }
            // If key1 is greater than add key2 and value2 first
            else
            {
                long value2 = long_buffer_2[buffer_2_Index + 1];
                // Make sure to check tombstones if its the last level
                if (!(last_level && (value2 == LONG_MIN)))
                {

                    std::memcpy(static_cast<char *>(write_buffer_SST) + sst_write_page_offset, &key2, sizeof(key2));
                    sst_write_page_offset += sizeof(key2);
                    std::memcpy(static_cast<char *>(write_buffer_SST) + sst_write_page_offset, &value2, sizeof(value2));
                    sst_write_page_offset += sizeof(value2);
                    bloom_filter.put(std::to_string(key2));

                    // Incremenet the number of key-value pairs written to the Leaf Node and assign key to final_key_added in case this Leaf Node will not be entirely filled
                    leaf_node_pairs_written++;
                    final_key_added = key2;

                    // When a Leaf Node is full, i.e., when leaf_node_pairs_written is equal to MAX_PAIRS, add the most recently added key (the max key in the Leaf Node) to max_keys_in_leaves and reset both leaf_node_pairs_written and write_leaf_node
                    if (leaf_node_pairs_written == MAX_PAIRS)
                    {
                        curr_page++;
                        btree.insertInternalNode(key2, curr_page);
                        leaf_node_pairs_written = 0;
                        leaf_nodes_written++;
                    }
                }
                buffer_2_Index += 2;
            }
        }
        // If write buffer is full then write it to file and then clear the buffer
        if (sst_write_page_offset + ENTRY_SIZE > buffer_size)
        {
            ssize_t bytes_written = pwrite(fd3, write_buffer_SST, buffer_size, sst_write_offset);
            if (bytes_written != buffer_size)
            {
                perror("pwrite failed");
                std::cerr << "Error: Incomplete write for key-value pairs batch." << std::endl;
                freeMergeSSTsResources(fd1, fd2, fd3, fd4, read_buffer_1, read_buffer_2, write_buffer_SST, write_buffer_BTree);
                return {"", ""};
            }
            sst_write_offset += bytes_written;
            sst_write_page_offset = 0;                               // Reset the buffer for the next batch
            std::memset(write_buffer_SST, INTERNAL, buffer_size); // Clear the buffer
        }
        // If buffer2 is done reading its page or there is no valid data left, then try to read in the next page
        if (key2 < 0 || buffer_2_Index >= num_longs)
        {
            read_page2 = pread(fd2, read_buffer_2, PAGE_SIZE, read_page_offset_2);
            if (read_page2 < 0)
            {
                std::cerr << "Error: Failed to read page in SST file " << sst2.sst_filename << std::endl;
                freeMergeSSTsResources(fd1, fd2, fd3, fd4, read_buffer_1, read_buffer_2, write_buffer_SST, write_buffer_BTree);
                return {"", ""};
            }
            // If there is no more pages to read from 2nd SST, then we finish reading the rest of 1st SST.
            if (read_page2 == 0)
            {
                // If there is no more data in sst2, write the rest of the data in sst1.
                while (true)
                {
                    key1 = long_buffer_1[buffer_1_Index];
                    if (key1 >= 0)
                    {
                        long value1 = long_buffer_1[buffer_1_Index + 1];
                        if (!(last_level && (value1 == LONG_MIN)))
                        {

                            std::memcpy(static_cast<char *>(write_buffer_SST) + sst_write_page_offset, &key1, sizeof(key1));
                            sst_write_page_offset += sizeof(key1);
                            std::memcpy(static_cast<char *>(write_buffer_SST) + sst_write_page_offset, &value1, sizeof(value1));
                            sst_write_page_offset += sizeof(value1);
                            bloom_filter.put(std::to_string(key1));

                            // Incremenet the number of key-value pairs written to the Leaf Node and assign key to final_key_added in case this Leaf Node will not be entirely filled
                            leaf_node_pairs_written++;
                            final_key_added = key1;

                            // When a Leaf Node is full, i.e., when leaf_node_pairs_written is equal to MAX_PAIRS, add the most recently added key (the max key in the Leaf Node) to max_keys_in_leaves and reset both leaf_node_pairs_written and write_leaf_node
                            if (leaf_node_pairs_written == MAX_PAIRS)
                            {
                                curr_page++;
                                btree.insertInternalNode(key1, curr_page);
                                leaf_node_pairs_written = 0;
                                leaf_nodes_written++;
                            }
                        }
                        // std::cerr << long_buffer_1[buffer_1_Index] << ", " << long_buffer_1[buffer_1_Index + 1] << std::endl;
                        buffer_1_Index += 2;
                    }

                    if (key1 < 0 || buffer_1_Index >= num_longs)
                    {
                        read_page1 = pread(fd1, read_buffer_1, PAGE_SIZE, read_page_offset_1);
                        if (read_page1 < 0)
                        {
                            std::cerr << "Error: Failed to read page in SST file " << sst1.sst_filename << std::endl;
                            freeMergeSSTsResources(fd1, fd2, fd3, fd4, read_buffer_1, read_buffer_2, write_buffer_SST, write_buffer_BTree);
                            return {"", ""};
                        }
                        // If we finished reading from sst2 then break as there is no more data left
                        if (read_page1 == 0)
                        {
                            break;
                        }
                        read_page_offset_1 += read_page1;
                        buffer_1_Index = 0;
                    }
                    if (sst_write_page_offset + ENTRY_SIZE > buffer_size)
                    {
                        ssize_t bytes_written = pwrite(fd3, write_buffer_SST, buffer_size, sst_write_offset);
                        if (bytes_written != buffer_size)
                        {
                            perror("pwrite failed");
                            std::cerr << "Error: Incomplete write for key-value pairs batch." << std::endl;
                            freeMergeSSTsResources(fd1, fd2, fd3, fd4, read_buffer_1, read_buffer_2, write_buffer_SST, write_buffer_BTree);
                            return {"", ""};
                        }
                        sst_write_offset += bytes_written;
                        sst_write_page_offset = 0;                               // Reset the buffer for the next batch
                        std::memset(write_buffer_SST, INTERNAL, buffer_size); // Clear the buffer
                    }
                }
                break;
            }
            read_page_offset_2 += read_page2;
            buffer_2_Index = 0;
        }
        // If buffer1 is done reading its page or there is no valid data left, then try to read in the next page
        if (key1 < 0 || buffer_1_Index >= num_longs)
        {
            read_page1 = pread(fd1, read_buffer_1, PAGE_SIZE, read_page_offset_1);
            if (read_page1 < 0)
            {
                std::cerr << "Error: Failed to read page in SST file " << sst1.sst_filename << std::endl;
                freeMergeSSTsResources(fd1, fd2, fd3, fd4, read_buffer_1, read_buffer_2, write_buffer_SST, write_buffer_BTree);
                return {"", ""};
            }
            if (read_page1 == 0)
            {
                // If there is no more data in sst1, write the rest of the data in sst2.
                while (true)
                {
                    key2 = long_buffer_2[buffer_2_Index];
                    if (key2 >= 0)
                    {
                        long value2 = long_buffer_2[buffer_2_Index + 1];
                        if (!(last_level && (value2 == LONG_MIN)))
                        {

                            std::memcpy(static_cast<char *>(write_buffer_SST) + sst_write_page_offset, &key2, sizeof(key2));
                            sst_write_page_offset += sizeof(key2);
                            std::memcpy(static_cast<char *>(write_buffer_SST) + sst_write_page_offset, &value2, sizeof(value2));
                            sst_write_page_offset += sizeof(value2);
                            bloom_filter.put(std::to_string(key2));

                            // Incremenet the number of key-value pairs written to the Leaf Node and assign key to final_key_added in case this Leaf Node will not be entirely filled
                            leaf_node_pairs_written++;
                            final_key_added = key2;

                            // When a Leaf Node is full, i.e., when leaf_node_pairs_written is equal to MAX_PAIRS, add the most recently added key (the max key in the Leaf Node) to max_keys_in_leaves and reset both leaf_node_pairs_written and write_leaf_node
                            if (leaf_node_pairs_written == MAX_PAIRS)
                            {
                                curr_page++;
                                btree.insertInternalNode(key2, curr_page);
                                leaf_node_pairs_written = 0;
                                leaf_nodes_written++;
                            }
                        }
                        buffer_2_Index += 2;
                    }

                    if (key2 < 0 || buffer_2_Index >= num_longs)
                    {
                        read_page2 = pread(fd2, read_buffer_2, PAGE_SIZE, read_page_offset_2);
                        if (read_page2 < 0)
                        {
                            std::cerr << "Error: Failed to read page in SST file " << sst2.sst_filename << std::endl;
                            freeMergeSSTsResources(fd1, fd2, fd3, fd4, read_buffer_1, read_buffer_2, write_buffer_SST, write_buffer_BTree);
                            return {"", ""};
                        }
                        // If we finished reading from sst2 then break as there is no more data left
                        if (read_page2 == 0)
                        {
                            break;
                        }
                        read_page_offset_2 += read_page2;
                        buffer_1_Index = 0;
                    }

                    if (sst_write_page_offset + ENTRY_SIZE > buffer_size)
                    {
                        ssize_t bytes_written = pwrite(fd3, write_buffer_SST, buffer_size, sst_write_offset);
                        if (bytes_written != buffer_size)
                        {
                            perror("pwrite failed");
                            std::cerr << "Error: Incomplete write for key-value pairs batch." << std::endl;
                            freeMergeSSTsResources(fd1, fd2, fd3, fd4, read_buffer_1, read_buffer_2, write_buffer_SST, write_buffer_BTree);
                            return {"", ""};
                        }
                        sst_write_offset += bytes_written;
                        sst_write_page_offset = 0;                               // Reset the buffer for the next batch
                        std::memset(write_buffer_SST, INTERNAL, buffer_size); // Clear the buffer
                    }
                }
                break;
            }
            read_page_offset_1 += read_page1;
            buffer_1_Index = 0;
        }
    }

    // Write any remaining data in the buffer if it's partially filled
    if (sst_write_page_offset > 0)
    {
        // Pad the remaining buffer with zeroes (or another INTERNAL if needed)
        std::memset(static_cast<char *>(write_buffer_SST) + sst_write_page_offset, INTERNAL, buffer_size - sst_write_page_offset);

        if (sst_write_page_offset % PAGE_SIZE != 0) {
            // Set the last long in the buffer to LEAF
            long *buffer_as_longs = static_cast<long *>(write_buffer_SST);
            int total_longs_in_buffer = buffer_size / sizeof(long);
            buffer_as_longs[total_longs_in_buffer - 2] = LEAF;
        }

        ssize_t bytes_written = pwrite(fd3, write_buffer_SST, buffer_size, sst_write_offset);
        if (bytes_written != buffer_size)
        {
            perror("pwrite failed");
            std::cerr << "Error: Incomplete final write for remaining key-value pairs." << std::endl;
            freeMergeSSTsResources(fd1, fd2, fd3, fd4, read_buffer_1, read_buffer_2, write_buffer_SST, write_buffer_BTree);
            return {"", ""};
        }

        curr_page++;
        btree.insertInternalNode(final_key_added, curr_page);
    }

    // Finalize the B-Tree and write Internal Nodes to the B-Tree file
    if (btree.getNodes().size() > 0 && leaf_nodes_written > 1)
    {
        btree.finalizeTree();
        btree.writeNodes(fd4, write_buffer_BTree, btree_write_offset);
    }

    // Delete old  Bloom filter files
    std::string btreeFile1 = sst1.sst_filename;
    btreeFile1.replace(btreeFile1.find("sst_"), 4, "btree_");
    std::string btreeFile2 = sst2.sst_filename;
    btreeFile2.replace(btreeFile2.find("sst_"), 4, "btree_");

    if (std::remove(btreeFile1.c_str()) != 0)
    {
        perror("Error deleting Bloom file 1");
    }
    if (std::remove(btreeFile2.c_str()) != 0)
    {
        perror("Error deleting Bloom file 2");
    }

    bloom_filter.serialize(bloom_filter_filename);

    // Delete old  Bloom filter files
    std::string bloomFile1 = sst1.sst_filename;
    bloomFile1.replace(bloomFile1.find("sst_"), 4, "bloom_");
    std::string bloomFile2 = sst2.sst_filename;
    bloomFile2.replace(bloomFile2.find("sst_"), 4, "bloom_");

    if (std::remove(bloomFile1.c_str()) != 0)
    {
        perror("Error deleting Bloom file 1");
    }
    if (std::remove(bloomFile2.c_str()) != 0)
    {
        perror("Error deleting Bloom file 2");
    }

    freeMergeSSTsResources(fd1, fd2, fd3, fd4, read_buffer_1, read_buffer_2, write_buffer_SST, write_buffer_BTree);
    return {new_sst_filename, new_btree_filename};
}

/*
    Puts the given sst and btree into the first level of the LSM tree,
    if compaction is needed then perform compactLevels.
*/
void LSMTree::put(long key, long value)
{
    memtable->put(key, value);

    if (!(memtable->getCurrSize() >= memtable->getMemtableSize()))
    {
        return;
    }
    // Write current memtable to SST
    std::pair<std::string, std::string> filenames = writeMemtableToDisk(memtable, database_name);

    // Free the currentMemtable as that information is no longer needed (its in SST now)
    // Newly created database
    this->changeMemtable(nullptr);

    std::string sst_filename = filenames.first;
    std::string btree_filename = filenames.second;

    insertSST(sst_filename, btree_filename);
}

void LSMTree::insertSST(std::string sst_filename, std::string btree_filename)
{
    if (levels[0].size() < level_size_ratio)
    {
        // std::cerr << "LSM add to level 0.\n";
        levels[0].emplace_back(0, levels[0].size(), sst_filename, btree_filename);

        if (levels[0].size() == level_size_ratio)
        {
            // std::cerr << "compactLevels\n";
            compactLevels();
        }
    }
    return;
}

/*
    Returns the result of scanning the lsm tree.
*/
std::pair<std::pair<long, long> *, int> LSMTree::scan(long key1, long key2, BufferPool *buffer_pool, bool with_btree)
{
    // First, check the memtable for the key
    std::map<long, long> key_value_pairs;

    // First check memtable
    std::pair<std::pair<long, long> *, int> array_size_pair = memtable->scan(key1, key2);
    std::pair<long, long> *key_value_pairs_memtable = array_size_pair.first;
    int scanned_size = array_size_pair.second;
    for (int i = 0; i < scanned_size; i++)
    {
        // std::cerr << "From Memtable: " << key_value_pairs_memtable[i].first << ", " << key_value_pairs_memtable[i].second;
        key_value_pairs[key_value_pairs_memtable[i].first] = key_value_pairs_memtable[i].second;
    }
    free(key_value_pairs_memtable);

    // Alignment and buffer size requirements for Direct I/O
    const size_t buffer_size = PAGE_SIZE;

    for (int level_idx = 0; level_idx < levels.size(); level_idx++)
    {
        std::vector<SST> level = levels[level_idx];

        for (int i = 0; i < level.size(); ++i)
        {
            std::string sst_filename = level[i].sst_filename; // Filter file associated with the SST
            std::string btree_filename = level[i].btree_filename;

            std::vector<std::pair<long, long>> scanned_values;
            if (with_btree)
            {
                StaticBTree btree(sst_filename, btree_filename);
                scanned_values = btree.scan(key1, key2, buffer_pool);
            }
            else
            {
                scanned_values = binarySearchScan(sst_filename, key1, key2, buffer_pool);
            }

            for (int j = 0; j < scanned_values.size(); j++)
            {
                if (key_value_pairs.find(scanned_values[j].first) == key_value_pairs.end())
                {
                    // std::cerr << "From SST: " << scanned_values[j].first << ", " << scanned_values[j].second;
                    key_value_pairs[scanned_values[j].first] = scanned_values[j].second;
                }
            }

            // If we find all of the keys in the range then end early (no point in searching more)
            if (key_value_pairs.size() >= (key2 - key1 + 1))
            {
                // Dynamically allocate memory for the result array
                std::pair<long, long> *arr_values = new std::pair<long, long>[key_value_pairs.size()];
                std::copy(key_value_pairs.begin(), key_value_pairs.end(), arr_values);

                return {arr_values, static_cast<int>(key_value_pairs.size())};
            }
        }
    }

    // Dynamically allocate memory for the result array
    std::pair<long, long> *arr_values = new std::pair<long, long>[key_value_pairs.size()];
    std::copy(key_value_pairs.begin(), key_value_pairs.end(), arr_values);

    return {arr_values, static_cast<int>(key_value_pairs.size())};
}

/*
    Returns the result of get on the lsm tree.
*/
NodeFileOffset *LSMTree::get(long key, BufferPool *buffer_pool, bool with_btree)
{
    // First, check the memtable for the key
    Node *result = memtable->get(key);
    if (result != nullptr)
    {
        return new NodeFileOffset(result, "", -1); // Return from memtable if found
    }

    // Iterate through each level in the LSM tree
    for (int level_idx = 0; level_idx < levels.size(); ++level_idx)
    {
        std::vector<SST> level = levels[level_idx];

        for (int i = 0; i < level.size(); ++i)
        {
            std::string sst_filename = level[i].sst_filename; // Filter file associated with the SST
            std::string btree_filename = level[i].btree_filename;

            // Generate the Bloom filter filename
            std::string bloom_filename = sst_filename;
            bloom_filename.replace(bloom_filename.find("sst_"), 4, "bloom_");

            // std::cerr << "Loading Bloom filter from file: " << bloomFilename << std::endl;

            // Check if Bloom filter is in the buffer pool
            Page *bloom_page = buffer_pool->searchForPage(bloom_filename);
            const size_t bloom_size_bytes = 2400 / 8; // Adjust size based on your configuration
            BloomFilter bloom_filter(2400, 3);       // Match the size and hash functions used during creation

            if (bloom_page != nullptr)
            {
                // Load from the buffer pool
                bloom_filter.loadBitArrayFromBuffer(bloom_page->data, bloom_size_bytes);
            }
            else
            {
                // Load from file
                bloom_filter.loadBitArrayFromFile(bloom_filename);

                // Create a temporary buffer for the bit array
                char temp_buffer[PAGE_SIZE] = {0}; // Assuming PAGE_SIZE >= bloom_size_bytes
                bloom_filter.copyBitArrayToBuffer(temp_buffer);

                // Insert into the buffer pool
                Page *new_page = new Page(bloom_filename, temp_buffer);
                buffer_pool->insertPage(new_page);
            }
            // std::cerr << "Checking Bloom filter for key: " << key << std::endl;

            if (!bloom_filter.mightContain(std::to_string(key)))
            {
                std::cerr << "Key not in bloom filter of this SST" << std::endl;
                continue; // Skip this SST if the Bloom filter says the key is absent
            }

            // std::cerr << "Key might be in SST: " << sstFileName << std::endl;
            if (with_btree)
            {
                StaticBTree btree(sst_filename, btree_filename);
                long value = btree.get(key, buffer_pool);
                // If value is found then break out of the look
                if (value != -1)
                {
                    return new NodeFileOffset(new Node(key, value), btree_filename, 0);
                }
            }
            else
            {
                NodeFileOffset *ret = binarySearch(sst_filename, key, buffer_pool);
                if (ret != nullptr)
                {
                    return ret;
                }
            }
        }
    }

    return nullptr; // Key not found
}

/*
    Changes the memtable of the current LSMTree
*/
Memtable *LSMTree::changeMemtable(Memtable *new_memtable)
{
    if (new_memtable == nullptr)
    {
        // Clean up the old memtable if necessary
        delete this->memtable;

        // Assign the new memtable
        this->memtable = new Memtable(memtable_size);

        return this->memtable;
    }
    else
    {
        // Assign the new memtable
        this->memtable = new_memtable;

        return this->memtable;
    }
}

/*
    Compacts the levels in the LSMTree.
*/
void LSMTree::compactLevels()
{
    for (int level_idx = 0; level_idx < levels.size(); ++level_idx)
    {
        std::vector<SST> level = levels[level_idx];
        bool is_last_level = (max_level - 1) == level_idx;
        if (level.size() == level_size_ratio)
        {
            SST merged_sst = level[0];
            for (size_t i = 1; i < level.size(); ++i)
            {
                SST current_sst = level[i];
                std::pair<std::string, std::string> merged_filenames = this->mergeSSTs(merged_sst, current_sst, is_last_level);
                std::string new_merged_sst_filename = merged_filenames.first;
                std::string new_merged_btree_filename = merged_filenames.second;
                // Delete the current_sst
                if (std::remove(current_sst.sst_filename.c_str()) != 0)
                {
                    perror("Error deleting file");
                    return;
                }
                // Delete the old merged_sst
                if (std::remove(merged_sst.sst_filename.c_str()) != 0)
                {
                    perror("Error deleting file");
                    return;
                }
                merged_sst.sst_filename = new_merged_sst_filename;
                merged_sst.btree_filename = new_merged_btree_filename;
            }
            // Clear current level as we have merged all of them.
            levels[level_idx].clear();

            // Check file size to see where it goes
            int fd = open(merged_sst.sst_filename.c_str(), O_RDONLY);
            if (fd < 0)
            {
                std::cerr << "CompactLevels LSM Tree: Failed to open merged SST file - " << merged_sst.sst_filename << std::endl;
                return;
            }
            off_t merged_file_size = lseek(fd, 0, SEEK_END);
            close(fd);

            size_t current_level_max_size = pow(level_size_ratio, level_idx + 1) * memtable_size;
            // If file has less than p^(level + 1) entries, then it stays on the same level
            if (merged_file_size <= current_level_max_size || is_last_level)
            {
                levels[level_idx].push_back(merged_sst);
            }
            // Otherwise add it to the next level
            else
            {
                merged_sst.level = level_idx + 1;
                levels[level_idx + 1].push_back(merged_sst);
            }
        }
    }
}

/*
    Print the current LSMTree.
*/
void LSMTree::printLSMTree()
{
    std::cerr << "LSM Tree: \n";
    for (const std::vector level : levels)
    {
        for (const SST &sst : level)
        {
            int fd = open(sst.sst_filename.c_str(), O_RDONLY);
            if (fd < 0)
            {
                std::cerr << "CompactLevels LSM Tree: Failed to open merged SST file - " << sst.sst_filename << std::endl;
                return;
            }
            off_t filesize = lseek(fd, 0, SEEK_END);
            close(fd);

            int fd1 = open(sst.btree_filename.c_str(), O_RDONLY);
            if (fd1 < 0)
            {
                std::cerr << "CompactLevels LSM Tree: Failed to open merged SST file - " << sst.btree_filename << std::endl;
                return;
            }
            off_t filesize1 = lseek(fd1, 0, SEEK_END);
            close(fd1);

            std::cerr << "Level:" << sst.level << ", Index: " << sst.level_index << ", SSTFilename: " << sst.sst_filename << ", Filesize: " << filesize << ", BtreeFilename : " << sst.btree_filename << ", Filesize : " << filesize1 << "\n";
        }
    }
}