#ifndef BLOOM_FILTER_H
#define BLOOM_FILTER_H

#include <vector>
#include <string>
#include <cstring>

class BloomFilter {
private:
    std::vector<uint8_t> bit_array; // Compact bit storage
    size_t size;                   // Total number of bits
    int num_hash_functions;          // Number of hash functions

    // Private helper to compute a hash value
    size_t hash(const std::string& key, int seed) const;

    // Private helpers for bit manipulation
    void setBit(size_t index);
    bool getBit(size_t index) const;

public:
    // Constructor
    BloomFilter(size_t num_bits, int num_hashes);

    void put(const std::string& key);
    bool mightContain(const std::string& key) const;
    void serialize(const std::string& filename) const;
    void loadBitArrayFromFile(const std::string& filename);
    void loadBitArrayFromBuffer(const void* buffer, size_t buffer_size);
    void copyBitArrayToBuffer(void* buffer) const;
    const void* getRawBitArray() const;
    size_t getSizeInBytes() const;
};

#endif // BLOOM_FILTER_H

