#include "static_b_tree.h"
#include <fstream>
#include <algorithm>
#include <iostream>

////////////////////////////////////////////////////////////////////////////
// Private: Constructors and Destructors

/*
    Constructor for BTreeNode.

    Input:
        leaf                Boolean flag indicating if this node is a leaf node.

    Initializes:
        num_keys            Set to 0, indicating no keys are present initially.
        is_leaf             Set based on the input to determine if this is a leaf node.
        keys                Array initialized to -1 (empty) values.
        pages_or_values     Array initialized to -1 to represent empty pages/values.
*/
BTreeNode::BTreeNode(bool leaf) : num_keys(0), is_leaf(leaf)
{
    keys.fill(-1); // Initialize all elements to -1
    pages_or_values.fill(-1); // Initialize all elements to -1
}

/*
    Constructor for StaticBTree.

    Initializes:
        sst_filename        Empty filename for SST, set later upon initialization.
        btree_filename      Empty filename for B-Tree, set later upon initialization.
        root_page_index     Defaulted to 0, updated when the B-Tree root node is created.
*/
StaticBTree::StaticBTree() : sst_filename(""), btree_filename(""), root_page_index(0) {}

/*
    Overloaded constructor for StaticBTree.

    Input:
        sst_filename        Filename for SST data storage.
        btree_filename      Filename for B-Tree storage.

    Initializes:
        root_page_index     Set to 0, updated when root node is created.
*/
StaticBTree::StaticBTree(std::string sst_filename, std::string btree_filename)
    : sst_filename(sst_filename), btree_filename(btree_filename), root_page_index(0) {}

////////////////////////////////////////////////////////////////////////////
// Private: Primary Functions

// /*
//     Binary search helper function for locating a key.

//     Input:
//         keys                Array of sorted keys.
//         num_keys            Number of keys in the array.
//         key                 The key to search for.

//     Returns:
//         Position of the key if found, or where it would be inserted.
// */
int StaticBTree::binarySearch(const long *keys, int num_keys, long key)
{
    int left = 0, right = num_keys - 1;

    while (left <= right) 
    {
        int mid = left + (right - left) / 2;
        long val_at_mid = keys[mid];

        if (val_at_mid >= 0) 
        { // Valid key
            if (val_at_mid == key) 
            {
                return mid; // Exact match
            } 
            else if (val_at_mid < key) 
            {
                left = mid + 1; // Search in the right half
            } 
            else 
            {
                right = mid - 1; // Search in the left half
            }
        } 
        else if (mid == MAX_PAIRS - 1 && (val_at_mid == -1 || val_at_mid == LEAF)) 
        {
            // If key is greater than all valid keys, direct to the last child
            return num_keys - 1; // Return the last valid index for traversal
        }
    }

    // Return the index of the closest child page for the key
    return left;
}

/*
    Retrieves a value associated with a key from a specific B-Tree page.

    Input:
        page_index          Index of the page to search.
        key                 The key to search for.
        buffer_pool         The BufferPool containing recently read pages.

    Returns:
        Value associated with the key, or -1 if not found.
*/
long StaticBTree::get(long page_index, long key, BufferPool *buffer_pool, Page *prev_page)
{
    char page[PAGE_SIZE];

    // If page is already in the buffer pool, then retrieve it from the buffer pool, otherwise, read the page from the B-Tree file and if the buffer pool exists, then add it to the buffer pool as a new page.
    std::string filename_offset = btree_filename + std::to_string(page_index * PAGE_SIZE);
    if (buffer_pool) {
        Page *possible_page = buffer_pool->searchForPage(filename_offset);
        if (possible_page && possible_page != prev_page)
        {
            // If the page is in the buffer pool then we get the page from the buffer pool.
            // std::cerr << "Saved 1 I/O by reading " << filename_offset << " from BufferPool while finding key: " << key << ".\n";
            std::memcpy(page, possible_page->data, PAGE_SIZE);
        }
        else
        {
            if (loadPage(btree_filename, page_index, page) < 0)
            {
                if (page_index > 0) {
                    page_index--;
                }
                if (loadPage(sst_filename, page_index, page) < 0) {
                    return -1; // Return immediately on failure
                }
            }

            Page *page_struct = new Page(filename_offset, page);
            buffer_pool->insertPage(page_struct);
            prev_page = page_struct;
        }
    }
    else {
        if (loadPage(btree_filename, page_index, page) < 0)
        {
            if (page_index > 0) {
                page_index--;
            }
            if (loadPage(sst_filename, page_index - 1, page) < 0) {
                return -1; // Return immediately on failure
            } 
        }
    }

    // Retrieve all of the values from the page we read
    auto [is_leaf, num_keys, keys, pages_or_values] = readPageContents(page);

    // If the number of keys in the page we read is less than or equal to 0, that means that there was some sort of error, so return -1
    if (num_keys <= 0) {
        std::cerr << "Error: Invalid node with zero keys at page " << page_index << std::endl;
        return -1;
    }

    // If the page we read is a Leaf Node, then retrieve the value at key and return it
    if (is_leaf) {
        int pos = binarySearch(keys.data(), num_keys, key);
        if (pos == LEAF) {
            return pages_or_values[MAX_PAIRS - 1];
        }
        else if (pos < num_keys && keys[pos] == key) {
            return pages_or_values[pos];
        }
    } 
    // If the page we read is an Internal Node, then search the page we have read to determine if key exists in it or if it exist in another Internal Node
    else {
        int pos = binarySearch(keys.data(), num_keys, key);
        if (pos == -1) {
            // If pos is -1 and num_keys is 255 (MAX_PAIRS - 1), that means we need to retrieve the value at 255 from pages_or_values
            if (num_keys == MAX_PAIRS - 1) {
                pos = MAX_PAIRS - 1;   
            }
            // If pos is -1 and num_keys is less than 255, that means the Internal Node we read is not entirely full, so we need to retrieve the value at num_keys - 1 from pages_or_values
            else {
                pos = num_keys - 1;
            }
        }

        // If pages_or_values[pos] is still -1 for some reason, then iteratively decrease pos by 1 to move backwards through pages_or_values to find the first position where pages_or_values[pos] is not -1
        while (pages_or_values[pos] == -1) {
            pos--;
        }
        int child_page = pages_or_values[pos];

        // Recursively call get with the child_page that we found
        return get(child_page, key, buffer_pool, prev_page);
    }

    // Return -1 if we were not able to find the key in the B-Tree
    return -1;
}

/*
    Scans a range of keys in a specific B-Tree page.

    Input:
        page_index          Index of the page to start the scan.
        key1                Start of the key range.
        key2                End of the key range.
        buffer_pool         The BufferPool containing recently read pages.

    Returns:
        Vector of key-value pairs within the specified range.
*/
std::vector<std::pair<long, long>> StaticBTree::scan(long page_index, long key1, long key2, BufferPool *buffer_pool, Page *prev_page)
{
    std::vector<std::pair<long, long>> results;
    char page[PAGE_SIZE];

    // If page is already in the buffer pool, then retrieve it from the buffer pool, otherwise, read the page from the B-Tree file and if the buffer pool exists, then add it to the buffer pool as a new page.
    std::string filename_offset = btree_filename + std::to_string(page_index * PAGE_SIZE);
    if (buffer_pool) {
        Page *possible_page = buffer_pool->searchForPage(filename_offset);
        if (possible_page && possible_page != prev_page)
        {
            // If the page is in the buffer pool then we get the page from the buffer pool.
            // std::cerr << "Saved 1 I/O by reading " << filename_offset << " from BufferPool while finding key: " << key << ".\n";
            std::memcpy(page, possible_page->data, PAGE_SIZE);
        }
        else
        {
            if (loadPage(btree_filename, page_index, page) < 0)
            {
                if (page_index > 0) {
                    page_index--;
                }
                if (loadPage(sst_filename, page_index, page) < 0) {
                    return results; // Return immediately on failure
                }
            }

            Page *page_struct = new Page(filename_offset, page);
            buffer_pool->insertPage(page_struct);
            prev_page = page_struct;
        }
    }
    else {
        if (loadPage(btree_filename, page_index, page) < 0)
        {
            if (page_index > 0) {
                page_index--;
            }
            if (loadPage(sst_filename, page_index - 1, page) < 0) {
                return results; // Return immediately on failure
            } 
        }
    }

    // Retrieve all of the values from the page we read
    auto [is_leaf, num_keys, keys, pages_or_values] = readPageContents(page);

    // If the page we read is a Leaf Node, then retrieve all of the values at keys between key1 and key2
    if (is_leaf) {
        for (int i = 0; i < num_keys; ++i) {
            if (keys[i] > key2)
                break;
            if (keys[i] >= key1)
                results.emplace_back(keys[i], pages_or_values[i]);
        }
    }
    // If the page we read is an Internal Node, then search the page we have read to determine if key1 and key2 exist in it or if they exist in another Internal Node
    else {
        int key1_pos = binarySearch(keys.data(), num_keys, key1);
        int key2_pos = binarySearch(keys.data(), num_keys, key2);

        // If neither key1 nor key2 exists in the Internal Node or no index to another Internal Node was output from binarySearch, that means that key1 or key2 simply do not exist in the B-Tree so we must return an error
        if (key1_pos == -1 || key2_pos == -1) {
            std::cerr << "Please input a valid range of keys to scan over." << std::endl;
            return results;
        }

        // If key1 or key2 exist in the Internal Node or an index to another Internal Node was output from binarySearch, then we recursively call the scan function to retrieve all of the values at keys between key1 and key2
        for (int i = key1_pos; i <= key2_pos && i < MAX_PAIRS; ++i) {
            auto child_results = scan(page_index + pages_or_values[i], key1, key2, buffer_pool, prev_page);
            results.insert(results.end(), child_results.begin(), child_results.end());
        }
    }

    // Return the list of key-value pairs
    return results;
}

////////////////////////////////////////////////////////////////////////////
// Private: Disk I/O Functions

/*
    Loads a page from the B-Tree file.

    Input:
        filename            Filename of the B-Tree file.
        page_index          Index of the page to load.
        buffer              Buffer to store the page content.

    Returns:
        0 on success, -1 on failure.
*/
int StaticBTree::loadPage(const std::string &filename, int page_index, void *buffer)
{
    if (!buffer) {
        std::cerr << "Error: Null buffer passed to loadPage." << std::endl;
        return -1;
    }

    int fd = open(filename.c_str(), O_RDONLY | O_DIRECT);
    if (fd < 0) {
        std::cerr << "Error: Could not open file " << filename << std::endl;
        close(fd);
        return -1;
    }

    off_t offset = page_index * PAGE_SIZE;

    off_t file_size = lseek(fd, 0, SEEK_END);
    if (file_size < offset + PAGE_SIZE) {
        close(fd);
        return -1;
    }

    ssize_t bytes_read = pread(fd, buffer, PAGE_SIZE, offset);

    if (bytes_read != PAGE_SIZE) {
        close(fd);
        return -1;
    }

    close(fd);
    return 0;
}

/*
    Reads and extracts the contents of a B-Tree page.

    Input:
        page                Buffer containing the page data.

    Returns:
        A tuple with the leaf status, number of keys, key vector, and pages/values vector.
*/
std::tuple<bool, int, std::vector<long>, std::vector<long>> StaticBTree::readPageContents(char page[PAGE_SIZE])
{
    char *curr_offset = page;
    std::array<long, MAX_PAIRS> keys, pages_or_values;
    bool is_leaf = (*reinterpret_cast<long *>(page + 4080) != (long)-1);

    int curr_key = 0;
    while (curr_key < MAX_PAIRS) {
        memcpy(&keys[curr_key], curr_offset, sizeof(long));
        curr_offset += sizeof(long) * 2;
        curr_key++;
    }

    int curr_val = 0;
    curr_offset = page + sizeof(long);
    while (curr_val < MAX_PAIRS) {
        memcpy(&pages_or_values[curr_val], curr_offset, sizeof(long));
        curr_offset += sizeof(long) * 2;
        curr_val++;
    }

    int num_keys = getNumKeys(keys);

    if (num_keys < 0 || num_keys > MAX_PAIRS) {
        std::cerr << "Error: Invalid number of keys read from page" << std::endl;
        num_keys = 0; // Prevent further undefined behavior
    }

    return {is_leaf, num_keys, std::vector<long>(keys.begin(), keys.end()),
            std::vector<long>(pages_or_values.begin(), pages_or_values.end())};
}

////////////////////////////////////////////////////////////////////////////
// Public: B-Tree Creation Functions

/*
    Insert function for adding a key-page pair to the B-Tree.

    Input:
        key                 The key to insert.
        page                The page associated with the key.

    Behavior:
        Checks if a new Internal Node is required. If so, creates one and adds it to the nodes vector.
        Inserts the key-page pair into the current Internal Node and updates `num_keys`.
*/
void StaticBTree::insertInternalNode(long key, long page) {
    // If the nodes vector for the B-Tree is empty, then create a new Internal Node and add it to the nodes vector
    if (nodes.empty()) {
        BTreeNode new_internal = BTreeNode(false);
        nodes.push_back(new_internal);
    }
    // If the nodes vector for the B-Tree is not empty and the last Internal Node added to the nodes vector is full, then create a new Internal Node and add it to the nodes vector
    else if (nodes.back().num_keys + 1 == MAX_PAIRS) {
        nodes.back().pages_or_values[nodes.back().num_keys] = page;
        BTreeNode new_internal = BTreeNode(false);
        nodes.push_back(new_internal);
        return;
    }

    // Retrieve the last Internal Node added to the nodes vector and add the key-page pair to it
    BTreeNode &curr_internal = nodes.back();
    curr_internal.keys[curr_internal.num_keys] = key;
    curr_internal.pages_or_values[curr_internal.num_keys] = page;
    curr_internal.num_keys++;
}

/*
    Insert function for adding a key-value pair to the B-Tree.

    Input:
        key                 The key to insert.
        value               The value associated with the key.

    Behavior:
        Checks if a new Leaf Node is required. If so, creates one.
        Inserts the key-value pair into the current Leaf Node and updates `num_keys`.
*/
void StaticBTree::insertLeafNode(int fd, void *buffer, BTreeNode &leaf_node, size_t &write_offset, long key, long value)
{
    // If the input Leaf Node has space in it, add the key-value pair to it
    if (leaf_node.num_keys < MAX_PAIRS) {
        leaf_node.keys[leaf_node.num_keys] = key;
        leaf_node.pages_or_values[leaf_node.num_keys] = value;
        leaf_node.num_keys++;
    } 
    // Send an error message if somehow we tried to add the key-value pair to the input Leaf Node when it was full
    else {
        std::cerr << "Error: Attempted to insert into a full Leaf Node." << std::endl;
    }

    // If the input Leaf Node is full, write it to disk and reset its variables
    if (leaf_node.num_keys == MAX_PAIRS) {
        writeNode(fd, buffer, leaf_node, write_offset);
        leaf_node.keys.fill(-1);
        leaf_node.pages_or_values.fill(-1);
        leaf_node.num_keys = 0;
    }
}

/*
    Function for inserting layers of Internal Nodes if needed and ensuring the Root Node and Internal Nodes are correct and in the correct order.
    
    Behavior:
        Finalizes the B-Tree structure by creating necessary Internal Nodes and setting the Root Page index.
*/
void StaticBTree::finalizeTree()
{
    // If the last Internal Node in nodes is empty, then just remove it from the vector
    if (nodes.back().num_keys == 0) {
        nodes.pop_back();
    }

    // If there are no nodes or if there is only a single Internal Node, return early
    if (nodes.empty() || nodes.size() == 1) {
        return;
    }

    // Create variables to iterate through the Internal Nodes, create new layers of Internal Nodes, and adding a new Root Node
    std::vector<BTreeNode> current_level = nodes; // Start with the last level (e.g., Leaf Nodes)
    std::vector<BTreeNode> next_level;
    bool add_root = false;
    long num_internal_nodes = nodes.size() - 1;

    while (current_level.size() > 1) {
        next_level.clear();

        for (size_t i = 0; i < current_level.size(); i += MAX_PAIRS) {
            BTreeNode internal_node(false); // Create a new Internal Node

            // Populate the Internal Node with keys and pointers from the current level
            for (size_t j = i; j < i + MAX_PAIRS && j < current_level.size(); ++j) {
                internal_node.pages_or_values[internal_node.num_keys] = j + 1;
                if (j < i + MAX_PAIRS - 1 && j < current_level.size() - 1) {
                    // Use the largest key from each child node (except the last one)
                    internal_node.keys[internal_node.num_keys] = current_level[j].keys[current_level[j].num_keys - 1];
                    internal_node.num_keys++;
                }
            }

            // Push the new Internal Node to the next level
            next_level.push_back(internal_node);
            num_internal_nodes++;
        }

        // Move to the next higher level
        current_level = next_level;
        add_root = true;
    }

    // The single remaining node is the Root Node
    if (!current_level.empty() && add_root) {
        nodes.insert(nodes.begin(), current_level[0]);
        root_page_index = 0; // The root is always the first node
    }

    // Iterate through all of the Internal Nodes (excluding the Root Node) and ensure that the page index for the Internal Nodes are all updated correctly
    for (int i = 1; i < nodes.size(); ++i) {
        BTreeNode &internal_node = nodes[i];
        std::transform(
            internal_node.pages_or_values.begin(), 
            internal_node.pages_or_values.end(), 
            internal_node.pages_or_values.begin(), 
            [num_internal_nodes](long page) { 
                return num_internal_nodes + page; 
        });
    }
}

/*
    Writes a series of BTreeNode nodes to a file.

    Input:
        fd                  The file descriptor for the output file.
        buffer              Buffer to write the Node content to.
        write_offset        Offset for writing into the buffer.
*/
void StaticBTree::writeNodes(int fd, void *buffer, size_t &write_offset)
{
    // Open the file using low-level POSIX open with Direct I/O
    if (fd < 0) {
        std::cerr << "Error: Could not open file for writing." << std::endl;
        return;
    }

    for (const auto &node : nodes) {
        writeNode(fd, buffer, node, write_offset);
    }
}

/*
    Write a single BTreeNode node to a file.

    Input:
        fd                  The file descriptor for the output file.
        buffer              Buffer to write the Node content to.
        node                The Node we want to write to the buffer.
        write_offset        Offset for writing into the buffer.
*/
void StaticBTree::writeNode(int fd, void *buffer, const BTreeNode &node, size_t &write_offset)
{
    // Clear the buffer
    std::memset(buffer, -1, PAGE_SIZE);

    // Populate the aligned buffer with node data
    char *buffer_offset = static_cast<char *>(buffer);
    int index = 0;
    while (index < MAX_PAIRS) {
        std::memcpy(buffer_offset, &node.keys[index], sizeof(long));
        buffer_offset += sizeof(long);
        std::memcpy(buffer_offset, &node.pages_or_values[index], sizeof(long));
        buffer_offset += sizeof(long);
        index++;
    }

    // Write the page to the file
    ssize_t bytes_written = pwrite(fd, buffer, PAGE_SIZE, write_offset);
    if (bytes_written != PAGE_SIZE) {
        std::cerr << "Error: Incomplete write to disk." << std::endl;
    }

    // Increment the write_offset by PAGE_SIZE to ensure that all pages written to disk are the same size
    write_offset += PAGE_SIZE;
}

////////////////////////////////////////////////////////////////////////////
// Public: Primary Functions

/*
    Retrieves a value associated with a key from the B-Tree.

    Input:
        key                 The key to search for.
        buffer_pool         The BufferPool containing recently read pages.

    Returns:
        Value associated with the key, or -1 if not found.
*/
long StaticBTree::get(long key, BufferPool *buffer_pool)
{
    return get(root_page_index, key, buffer_pool, nullptr);
}

/*
    Scans a range of keys in the B-Tree.

    Input:
        key1                Start of the key range.
        key2                End of the key range.
        buffer_pool         The BufferPool containing recently read pages.

    Returns:
        Vector of key-value pairs within the specified range.
*/
std::vector<std::pair<long, long>> StaticBTree::scan(long key1, long key2, BufferPool *buffer_pool)
{
    return scan(root_page_index, key1, key2, buffer_pool, nullptr);
}

////////////////////////////////////////////////////////////////////////////
// Public: Helper Functions

/*
    Getter for all nodes in the B-Tree.

    Returns:
        Vector of all BTreeNode instances in the B-Tree.
*/
std::vector<BTreeNode> StaticBTree::getNodes()
{
    return nodes;
}

/*
    Getter for the number of Keys in the input Node.

    Returns:
        Integer stating the number of keys in the Node.
*/
int StaticBTree::getNumKeys(const std::array<long, MAX_PAIRS> &keys) {
    return std::count_if(keys.begin(), keys.end(), [](long key) { return (key != -1 && key != LEAF); });
}

/*
    Getter for whether the input Node is an Internal Node or Leaf Node.

    Returns:
        Boolean stating whether the Node is an Internal Node or Leaf Node.
*/
bool StaticBTree::isLeaf(const std::array<long, MAX_PAIRS> &keys) {
    return keys[MAX_PAIRS - 1] != -1;
}