#include "test_helpers.h"

/*
    The function for the open command-line argument for our DB.
*/
Memtable *dbOpen(std::string database_name, int memtable_size)
{
    std::string filepath = DATA_FILE_PATH + database_name;
    if (std::filesystem::create_directory(filepath))
    {
        std::cerr << "Created Directory: " << filepath << std::endl;
    }
    // Newly created database

    Memtable *new_memtable = new Memtable(memtable_size);
    return new_memtable;
}

/*
    The function for the close command-line argument for our DB.
*/
std::pair<std::string, std::string> dbClose(Memtable *current_memtable, std::string current_database)
{
    // Write current memtable to SST
    std::pair<std::string, std::string> filenames = writeMemtableToDisk(current_memtable, current_database);

    return filenames;
}

/*
    Removes all files in the database folder.
*/
void dbClear(std::string current_database)
{
    std::string target_directory = DATA_FILE_PATH + current_database;
    // std::filesystem::remove_all(target_directory);
    for (const auto &entry : std::filesystem::directory_iterator(target_directory))
    {
        if (std::filesystem::is_regular_file(entry.path()))
        {
            std::filesystem::remove(entry.path()); // Delete the file
            // std::cout << "Deleted file: " << entry.path() << std::endl;
        }
    }
}
