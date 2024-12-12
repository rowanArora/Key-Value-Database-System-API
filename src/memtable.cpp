#include <iostream>
#include "memtable.h"
#include "sst.h"
////////////////////////////////////////////////////////////////////////////
// Define the Node struct's constructor and destructor.
Node::Node(long k, long v)
{
    key = k;
    value = v;
    left = nullptr;
    right = nullptr;
    height = 1;
}

Node::~Node()
{
    // Add code here to free up dynamically allocated memory, but as there
    // is no dynamically allocated memory right now, there is no code.
}
////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////
// Define the Memtable class's constructor and destructor.
Memtable::Memtable(int memtable_s)
{
    root_node = nullptr;
    memtable_size = memtable_s;
    curr_size = 0;
}

Memtable::~Memtable()
{
    deleteTree(root_node);
}
////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////
// Implement all of the Memtable class's private functions.
// Implementation of the getHeight function.
int Memtable::getHeight(Node *node)
{
    if (node == nullptr)
    {
        return 0;
    }
    return node->height;
}

// Implementation of the getBalanceFunction function.
int Memtable::getBalanceFactor(Node *node)
{
    if (node == nullptr)
    {
        return 0;
    }
    return getHeight(node->left) - getHeight(node->right);
}

// Implementation of the rotateRight function.
Node *Memtable::rotateRight(Node *curr_root)
{
    if (curr_root == nullptr || curr_root->left == nullptr)
    {
        std::cerr << "Error: rotateRight called on a null or invalid node" << std::endl;
        return curr_root; // Return the root as-is if rotation is invalid
    }

    // Save the Nodes which are being moved
    Node *new_root = curr_root->left;
    Node *move_right = new_root->right;

    // Complete the actual rotation
    new_root->right = curr_root;
    curr_root->left = move_right;

    // Update the height values for the moved Nodes
    curr_root->height = 1 + std::max(getHeight(curr_root->left), getHeight(curr_root->right));
    new_root->height = 1 + std::max(getHeight(new_root->left), getHeight(new_root->right));

    // Return the new root
    return new_root;
}

// Implementation of the rotateLeft function.
Node *Memtable::rotateLeft(Node *curr_root)
{
    if (curr_root == nullptr || curr_root->right == nullptr)
    {
        std::cerr << "Error: rotateLeft called on a null or invalid node" << std::endl;
        return curr_root;
    }

    // Save the Nodes which are being moved
    Node *new_root = curr_root->right;
    Node *move_left = new_root->left;

    // Complete the actual rotation
    new_root->left = curr_root;
    curr_root->right = move_left;

    // Update the height values for the moved Nodes
    curr_root->height = 1 + std::max(getHeight(curr_root->left), getHeight(curr_root->right));
    new_root->height = 1 + std::max(getHeight(new_root->left), getHeight(new_root->right));

    // Return the new root
    return new_root;
}

// Implementation of the insert function.
Node *Memtable::insert(Node *curr_root, long key, long value)
{
    if (curr_root == nullptr)
    {
        curr_size++;
        return new Node(key, value);
    }

    if (key < curr_root->key)
    {
        curr_root->left = insert(curr_root->left, key, value);
    }
    else if (key > curr_root->key)
    {
        curr_root->right = insert(curr_root->right, key, value);
    }
    else
    {
        // Update the value to its new value
        curr_root->value = value;
        return curr_root;
    }

    curr_root->height = 1 + std::max(getHeight(curr_root->left), getHeight(curr_root->right));
    int balance = getBalanceFactor(curr_root);

    // Debug: Print node balance and key
    // std::cerr << "Node Key: " << curr_root->key << ", Balance: " << balance << std::endl;

    // Handle rotations
    if (balance > 1 && key < curr_root->left->key)
    {
        // std::cerr << "Performing rotateRight on key: " << curr_root->key << std::endl;
        return rotateRight(curr_root);
    }

    if (balance > 1 && key > curr_root->left->key)
    {
        // std::cerr << "Performing rotateLeftRight on key: " << curr_root->key << std::endl;
        curr_root->left = rotateLeft(curr_root->left);
        return rotateRight(curr_root);
    }

    if (balance < -1 && key > curr_root->right->key)
    {
        // std::cerr << "Performing rotateLeft on key: " << curr_root->key << std::endl;
        return rotateLeft(curr_root);
    }

    if (balance < -1 && key < curr_root->right->key)
    {
        // std::cerr << "Performing rotateRightLeft on key: " << curr_root->key << std::endl;
        curr_root->right = rotateRight(curr_root->right);
        return rotateLeft(curr_root);
    }

    return curr_root;
}

// Implementation of the get function.
Node *Memtable::get(Node *curr_root, long key)
{
    if (curr_size == 0 || curr_root == nullptr)
    {
        // std::cerr << "Error: No Node exists with the given key value." << std::endl;
        return nullptr;
    }

    if (curr_root->key == key)
    {
        return curr_root;
    }
    else if (curr_root->left != nullptr && key < curr_root->key)
    {
        return get(curr_root->left, key);
    }
    else if (curr_root->right != nullptr && key > curr_root->key)
    {
        return get(curr_root->right, key);
    }
    else
    {
        return nullptr;
    }
}

// Implementation of the deleteTree function.
void Memtable::deleteTree(Node *curr_root)
{
    if (curr_root != nullptr)
    {
        deleteTree(curr_root->left);
        deleteTree(curr_root->right);
        delete curr_root;
    }
}
// Implementation of the scan function.
void Memtable::scan(Node *curr_root, long key1, long key2, std::vector<std::pair<long, long>> *found_nodes)
{
    // If the current node is null, then return
    if (curr_root == nullptr)
    {
        return;
    }
    // If the current node is not null, then return
    long curr_key = curr_root->key;
    long curr_value = curr_root->value;
    std::pair<long, long> key_value_pair(curr_key, curr_value);
    // Traverse and scan
    if (key1 < curr_key)
    {
        scan(curr_root->left, key1, key2, found_nodes);
    }

    if (key1 <= curr_key && curr_key <= key2)
    {
        found_nodes->push_back(key_value_pair);
    }

    if (curr_key < key2)
    {
        scan(curr_root->right, key1, key2, found_nodes);
    }

    return;
}
////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////
// Implement all of the Memtable class's public functions.
// Implementation of the put function.
void Memtable::put(long key, long value)
{
    if (curr_size < memtable_size)
    {
        root_node = insert(root_node, key, value);
        return;
    }
    std::cerr << ""
                 "Error: The Memtable is completely full. Please convert it to an SST and create a new Memtable to insert this Node."
                 ""
              << std::endl;
}

// Implementation of the get function.
Node *Memtable::get(long key)
{
    return get(root_node, key);
}
NodeFileOffset *Memtable::get(long key, const std::string current_database, BufferPool *buffer_pool)
{
    // Search in the memtable
    Node *result = get(root_node, key);
    if (result != nullptr)
    {
        NodeFileOffset *ret = new NodeFileOffset(result, "", -1);
        return ret; // If key is found in memtable, return the result
    }

    // If not found in the memtable, search the SST files
    std::vector<std::string> sst_files = getDataFiles(current_database, "sst"); // Pass current_database directly

    for (const auto &sst_file : sst_files)
    {
        NodeFileOffset *node_file_offset = binarySearch(sst_file, key, buffer_pool);
        if (node_file_offset != nullptr)
        {
            return node_file_offset; // Return if found in the SST
        }
    }
    return nullptr;
}

// Implementation of the scan function.
// You must free up the array of key-value pairs after receiving them.
std::pair<std::pair<long, long> *, int> Memtable::scan(long key1, long key2)
{
    std::vector<std::pair<long, long>> found_nodes;

    // Call the internal scan function
    scan(root_node, key1, key2, &found_nodes);

    // Copy results into dynamically allocated array
    std::pair<long, long> *arr_values = new std::pair<long, long>[found_nodes.size()];
    std::copy(found_nodes.begin(), found_nodes.end(), arr_values);

    // Return array and its size
    return {arr_values, static_cast<int>(found_nodes.size())};
}

// Public scan function: scans memtable and SSTs
std::pair<std::pair<long, long> *, int> Memtable::scan(long key1, long key2, const std::string current_database, BufferPool *buffer_pool)
{

    std::vector<std::pair<long, long>> results;

    // Scan the memtable
    std::vector<std::pair<long, long>> memtable_results;
    scan(root_node, key1, key2, &memtable_results);
    results.insert(results.end(), memtable_results.begin(), memtable_results.end());

    // Scan all SST files
    std::vector<std::string> sst_files = getDataFiles(current_database, "sst");
    for (const auto &sst_file : sst_files)
    {
        std::vector<std::pair<long, long>> sst_results = binarySearchScan(sst_file, key1, key2, buffer_pool);
        results.insert(results.end(), sst_results.begin(), sst_results.end());
    }

    // Dynamically allocate memory for the result array
    std::pair<long, long> *arr_values = new std::pair<long, long>[results.size()];
    std::copy(results.begin(), results.end(), arr_values);

    return {arr_values, static_cast<int>(results.size())};
}

// Get memtable size
int Memtable::getMemtableSize()
{
    return memtable_size;
}
// Get current size
int Memtable::getCurrSize()
{
    return curr_size;
}
////////////////////////////////////////////////////////////////////////////
