#ifndef STATIC_B_TREE_H
#define STATIC_B_TREE_H

#include <vector>
#include <string>
#include <optional>
#include <cstring>
#include <tuple>
#include "sst.h"
#include "global.h"

/*
    Represents a base node in the Static B-Tree structure.

    Input:
        leaf                    The boolean flag indicating if the node is a leaf or internal node

    Attributes:
        num_keys                The number of keys stored in the node
        is_leaf                 Boolean flag indicating if the node is a leaf (true) or internal (false)
        keys                    Array storing the keys in the node
        pages_or_values         Array storing either page references (internal nodes) or values (leaf nodes)
*/
struct BTreeNode
{
    int num_keys;
    bool is_leaf;
    std::array<long, MAX_PAIRS> keys;
    std::array<long, MAX_PAIRS> pages_or_values;

    BTreeNode(bool leaf);
    virtual ~BTreeNode() = default;
};

/*
    Represents a Static B-Tree structure, allowing efficient storage and retrieval
    of key-value pairs through a disk-based B-Tree implementation.

    Inputs:
        sst_filename            The string containing the name of the binary SST file
        btree_filename          The string containing the name of the binary B-Tree file

    Attributes:
        nodes                   Vector of BTreeNode instances that make up the tree structure
        root_page_index         Index of the root page in the nodes vector
        sst_filename            Name of the SST file used in the B-Tree construction
        btree_filename          Filename for the serialized B-Tree on disk

    Functions:
        get                     Retrieves the value associated with a key from a specified page
        scan                    Finds and returns key-value pairs within a specified range
        binarySearch            Performs binary search on a sorted array of keys
        loadPage                Loads a page from disk into memory
        readPageContents        Reads the content of a page, returning keys and page/value info
        insertInternalNode      Creates a BTreeNode Internal Node instance and addes it to the nodes vector
        insertLeafNode          Create a BTreeNode Leaf Node instance and writes it to disk
        finalizeTree            Completes the B-Tree structure before saving to disk
        writeNodes              Writes BTreeNode instances to disk
        writeNode               Writes a BTreeNode instance to disk
        getNodes                Returns a vector of all BTreeNode instances
        getNumKeys              Returns the num_keys value of a BTreeNode instance
        isLeaf                  Returns the is_leaf value of a BTreeNode instance
*/
class StaticBTree
{
private:
    std::vector<BTreeNode> nodes;
    int root_page_index;
    std::string sst_filename;
    std::string btree_filename;

    // Primary Functions:
    long get(long page_index, long key, BufferPool *buffer_pool, Page *prev_page);
    std::vector<std::pair<long, long>> scan(long page_index, long key1, long key2, BufferPool *buffer_pool, Page *prev_page);
    int binarySearch(const long *keys, int num_keys, long key);

    // Disk I/O Functions:
    int loadPage(const std::string &filename, int page_index, void *buffer);
    std::tuple<bool, int, std::vector<long>, std::vector<long>> readPageContents(char page[PAGE_SIZE]);

public:
    // Constructors
    StaticBTree();
    StaticBTree(std::string sst_filename, std::string btree_filename);

    // Primary Functions:
    long get(long key, BufferPool *buffer_pool = nullptr);
    std::vector<std::pair<long, long>> scan(long key1, long key2, BufferPool *buffer_pool = nullptr);

    // Disk I/O Functions:
    void insertInternalNode(long key, long page);
    void insertLeafNode(int fd, void *buffer, BTreeNode &leaf_node, size_t &write_offset, long key, long value);
    void finalizeTree();
    void writeNodes(int fd, void *buffer, size_t &write_offset);
    void writeNode(int fd, void *buffer, const BTreeNode &node, size_t &write_offset);

    // Getter Functions:
    std::vector<BTreeNode> getNodes();
    int getNumKeys(const std::array<long, MAX_PAIRS> &keys);
    bool isLeaf(const std::array<long, MAX_PAIRS> &keys);
};

#endif