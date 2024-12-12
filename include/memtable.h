#ifndef MEMTABLE_H
#define MEMTABLE_H

#include "global.h"
#include "buffer_pool.h"
#include <utility> // for pair
#include <vector>  // for tuple
#include <string>

/*
    Create an Node that stores key-value pairs.

    Input:
        key                 8-byte int (long) associated with the value input.
        value               8-byte int (long) associated with the key input.

    Attributes:
        left                pointer to the Node's left child
        right               pointer to the Node's right child
        height              the height of the Node
*/
struct Node
{
    long key;
    long value;
    Node *left;
    Node *right;
    int height;

    Node(long k, long v);
    ~Node();
};

/*
    Represents the 3-value return value for the get and binarySearch function.
*/
struct NodeFileOffset
{
    Node *node;
    std::string file;
    long offset;
    NodeFileOffset(Node *node, std::string file, long offset) : node(node), file(file), offset(offset) {}
    ~NodeFileOffset()
    {
        delete node;
    }
};
typedef struct NodeFileOffset NodeFileOffset;

/*
    Create an AVL Binary Tree (Memtable) that stores instances of the Node class.

    Input:
        memtable_size   the max size of the newly initialized Memtable

    Attributes:
        rootNode            the root Node of the entire tree
        memtable_size       the max size that the Memtable can be
        currSize            the current number of Nodes stored in the tree

    Functions:
        getHeight           gets the height of the input Node
        getBalanceFactor    computes whether the input Node is balanced or not
        rotateRight         rotates the sub-tree at the input Node right
        rotateLeft          rotates the sub-tree at the input Node left
        insert              inserts the input Node into the tree if there is enough space
        get                 finds all of the Nodes with the input key and returns an array of values
        deleteTree          recursively deletes the input Node and all of its children Nodes
*/
class Memtable
{
private:
    Node *root_node;
    int memtable_size;
    int curr_size;
    int getHeight(Node *node);
    int getBalanceFactor(Node *node);
    Node *rotateRight(Node *curr_root);
    Node *rotateLeft(Node *curr_root);
    Node *insert(Node *curr_root, long key, long value);
    Node *get(Node *curr_root, long key);
    void scan(Node *curr_root, long key1, long key2, std::vector<std::pair<long, long>> *found_nodes);

public:
    Memtable(int memtable_size);
    ~Memtable();
    void put(long key, long value);
    Node *get(long key);
    NodeFileOffset *get(long key, const std::string current_database, BufferPool *buffer_pool);
    std::pair<std::pair<long, long> *, int> scan(long key1, long key2);
    std::pair<std::pair<long, long> *, int> scan(long key1, long key2, const std::string current_database, BufferPool *buffer_pool);
    // void delete(long key); // Need to implement a delete function given a key
    int getMemtableSize();
    int getCurrSize();
    void deleteTree(Node *curr_root);
};

#endif