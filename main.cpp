#include "main.h"

// Minimal main.cpp
int main()
{
    // Setup needed variables
    int memtable_size;
    std::cout << "Size of Memtable (# of key-value pairs): ";
    std::cin >> memtable_size;

    std::string command;

    Memtable *current_memtable = nullptr;
    std::string current_database = "";

    BufferPool *buffer_pool = new BufferPool(BUFFER_POOL_MAX_PAGES);

    std::string sst_filename = "";
    std::string btree_filename = "";

    std::map<std::string, LSMTree *> lsm_tree_map;
    LSMTree *lsm_tree = nullptr;

    // Iterate to receive API calls.
    while (true)
    {
        //
        std::cout << "$" << current_database << ": ";
        std::cin >> command;

        std::smatch match;

        // Handle Quit command
        if (command == "Quit()")
        {
            // Clean up resources
            if (current_memtable != nullptr)
            {
                // Write memtable to SST if there is unsaved data
                if (current_memtable->getCurrSize() != 0)
                {
                    std::pair<std::string, std::string> filenames = close(current_memtable, current_database, lsm_tree);
                    std::cout << "Wrote new SST: " << filenames.first << std::endl;
                }
                free(current_memtable);
            }

            // Free buffer pool
            delete buffer_pool;

            // Free all LSM trees
            for (auto &[name, tree] : lsm_tree_map)
            {
                delete tree;
            }

            std::cout << "Exiting program. Goodbye!" << std::endl;
            break; // Exit the loop
        }

        // Handle Open("database_name") command
        else if (std::regex_match(command, match, std::regex("Open\\(\"([^\"]+)\"\\)")))
        {
            std::string name = match[1];
            std::cout << "Open command with: " << name << std::endl;

            // Call the dbOpen method
            current_memtable = dbOpen(name, memtable_size);
            current_database = name;

            // If there is already an in memory LSM tree then load it in
            if (lsm_tree_map.find(name) != lsm_tree_map.end())
            {
                lsm_tree = lsm_tree_map[name];
                lsm_tree->changeMemtable(current_memtable);
            }
            // If not then create new LSM tree
            else
            {
                LSMTree *new_LSM_tree = new LSMTree(memtable_size, current_database, current_memtable);
                lsm_tree_map[name] = new_LSM_tree;
                lsm_tree = new_LSM_tree;
            }
            // Print LSM tree
            lsm_tree->printLSMTree();
        }

        // Handle Close() command
        else if (command == "Close()")
        {
            // Check if we are currently in a database.
            if (current_memtable == nullptr || current_database.empty())
            {
                std::cout << "You must first open a database to use this operation." << std::endl;
                continue;
            }
            // If current memtable has data in it, then we write it to disk before closing database
            if (current_memtable->getCurrSize() != 0)
            {
                // Write current memtable to SST by calling close (which will write to disk)
                std::pair<std::string, std::string> filenames = close(current_memtable, current_database, lsm_tree);
                // Free the current memtable as its no longer in use
                free(current_memtable);
                // Let user know about new SST written to disk
                sst_filename = filenames.first;
                btree_filename = filenames.second;
                std::cout << "Wrote new SST: " << sst_filename << std::endl;
            }
            // Since we exit the database, reset these values to default.
            current_memtable = nullptr;
            current_database = "";
            lsm_tree = nullptr;
        }

        // Handle Put(key,value) command
        else if (std::regex_match(command, match, std::regex("Put\\(([0-9]+),([0-9]+)\\)")))
        {
            // Check if we are currently in a database.
            if (current_memtable == nullptr || current_database.empty())
            {
                std::cout << "You must first open a database to use this operation." << std::endl;
                continue;
            }

            // Get the key and value from regular expression
            long key = std::stol(match[1]);
            long value = std::stol(match[2]);

            lsm_tree->put(key, value);
        }

        // Handle Get(key) command
        else if (std::regex_match(command, match, std::regex("Get\\(([0-9]+)\\)")))
        {
            // Check if we are currently in a database.
            if (current_memtable == nullptr || current_database.empty())
            {
                std::cout << "You must first open a database to use this operation." << std::endl;
                continue;
            }

            // If we are in a database, then get the value of the key.
            long key = std::stol(match[1]);

            // Ask if the Get function should be called with Binary Search or B-Tree Search.
            std::cout << "$" << current_database << " Call Get(key) with Binary Search (1), B-Tree Search (2): ";
            std::cin >> command;

            std::smatch match;
            if (std::regex_match(command, match, std::regex("[12]")))
            {
                std::string name = match[0];

                NodeFileOffset *node_file_offset = nullptr;

                // Do Binary Search
                if (name == "1")
                {
                    std::cout << "Get command with Binary Search: " << name << std::endl;

                    node_file_offset = lsm_tree->get(key, buffer_pool, false);
                }
                // Do Btree Search
                else if (name == "2")
                {
                    std::cout << "Get command with B-Tree Search: " << name << std::endl;

                    node_file_offset = lsm_tree->get(key, buffer_pool, true);
                }
                // If returned value is nullptr then key not found
                if (node_file_offset == nullptr)
                {
                    std::cout << "No value with key " << key << "." << std::endl;
                }
                else
                {
                    Node *node = node_file_offset->node;
                    // If value returned is LONG_MIN then we found a tombstone and this key was deleted.
                    if (node->value == LONG_MIN)
                    {
                        std::cout << key << " was deleted from the database." << std::endl;
                    }
                    // If we do find the key, then output "Got value <value>."
                    else
                    {
                        std::cout << "Got value " << node->value << "." << std::endl;
                    }
                }
            }
            // In case of Invalid data we prompt user with useful tips
            else
            {
                std::cout << "You must select 1 for Get with Binary Search over the leaves or 2 for Get with B-Tree search." << std::endl;
                continue;
            }
        }
        // Handle Scan(key1, key2) command
        else if (std::regex_match(command, match, std::regex("Scan\\(([0-9]+),([0-9]+)\\)")))
        {
            // Check if we are currently in a database.
            if (current_memtable == nullptr || current_database.empty())
            {
                std::cout << "You must first open a database to use this operation." << std::endl;
                continue;
            }

            // Get the key1, key2 from the regular expression match.
            long key1 = std::stol(match[1]);
            long key2 = std::stol(match[2]);

            // Ask if the Scan function should be called with Binary Search or B-Tree Search.
            std::cout << "$" << current_database << " Call Scan(key) with Binary Search (1) or B-Tree Search (2): ";
            std::cin >> command;

            std::smatch match;
            if (std::regex_match(command, match, std::regex("[12]")))
            {
                std::string name = match[0];

                std::pair<std::pair<long, long> *, int> array_size_pair;
                std::pair<long, long> *key_value_pairs = nullptr;
                int scanned_size = 0;
                // Do Binary Search
                if (name == "1")
                {
                    std::cout << "Scan command with Binary Search: " << name << std::endl;

                    // Scan the memtable with key1, key2 and print the output.
                    array_size_pair = lsm_tree->scan(key1, key2, buffer_pool, false);
                    key_value_pairs = array_size_pair.first;
                    scanned_size = array_size_pair.second;
                }
                // Do Btree Search
                else if (name == "2")
                {
                    std::cout << "Scan command with B-Tree Search: " << name << "\n";

                    array_size_pair = lsm_tree->scan(key1, key2, buffer_pool, true);
                    key_value_pairs = array_size_pair.first;
                    scanned_size = array_size_pair.second;
                }
                // Print out the values found
                std::cout << "Scanned " << scanned_size << " key-value pairs:\n";
                for (int i = 0; i < scanned_size; i++)
                {
                    // If value is not a tombstone then print (key, value)
                    if (key_value_pairs[i].second != LONG_MIN)
                    {
                        std::cout << "(" << key_value_pairs[i].first << ", " << key_value_pairs[i].second << ")" << std::endl;
                    }
                    // If value is a tombstone then print (key, Deleted)
                    else
                    {
                        std::cout << "(" << key_value_pairs[i].first << ", Deleted)" << std::endl;
                    }
                }
                free(key_value_pairs);
            }
        }
        // Handle Delete(key) command
        else if (std::regex_match(command, match, std::regex("Delete\\(([0-9]+)\\)")))
        {
            // Check if we are currently in a database.
            if (current_memtable == nullptr || current_database.empty())
            {
                std::cout << "You must first open a database to use this operation." << std::endl;
                continue;
            }
            long key = std::stol(match[1]);
            long value = LONG_MIN;
            // Call lsmTree put function with the LONG_MIN value representing a tombstone
            lsm_tree->put(key, value);
        }
        // In case of Invalid data we prompt user with useful tips on using the API
        else
        {
            std::cout << "Invalid Input: use Open(\"database name\"), Put(key, value), Get(key), Scan(key1, key2), Delete(key), Close(), Quit()." << std::endl;
        }
    }
    return 0;
}

/*
    The function for the close command-line argument for our DB.
*/
std::pair<std::string, std::string> close(Memtable *current_memtable, std::string current_database, LSMTree *lsm_tree)
{
    // Write current memtable to SST
    std::pair<std::string, std::string> filenames = writeMemtableToDisk(current_memtable, current_database);
    lsm_tree->insertSST(filenames.first, filenames.second);
    return filenames;
}