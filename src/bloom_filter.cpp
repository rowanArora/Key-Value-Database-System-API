#include "bloom_filter.h"
#include <fstream>
#include <stdexcept>
#include <functional>
#include <iterator>
#include <iostream>
#include <cstring> 

// Constructor
BloomFilter::BloomFilter(size_t num_bits, int num_hashes)
    : bit_array((num_bits + 7) / 8, 0), size(num_bits), num_hash_functions(num_hashes) {
    if (num_bits == 0 || num_hashes <= 0) {
        throw std::invalid_argument("Number of bits and hash functions must be greater than 0.");
    }
}

// Compute a hash value for the given key and seed
size_t BloomFilter::hash(const std::string& key, int seed) const {
    std::hash<std::string> hash_fn;
    return (hash_fn(key) + seed) % size;
}

//  Set a bit in the bit array
void BloomFilter::setBit(size_t index) {
    bit_array[index / 8] |= (1 << (index % 8));
}

//  Get a bit from the bit array
bool BloomFilter::getBit(size_t index) const {
    return bit_array[index / 8] & (1 << (index % 8));
}

// Insert a key into the Bloom filter
void BloomFilter::put(const std::string& key) {
    for (int i = 0; i < num_hash_functions; ++i) {
        size_t hash_value = hash(key, i);
        setBit(hash_value);
    }
}

// Check if a key might exist in the Bloom filter
bool BloomFilter::mightContain(const std::string& key) const {
    for (int i = 0; i < num_hash_functions; ++i) {
        size_t hash_value = hash(key, i);
        if (!getBit(hash_value)) {
            return false; // Key is definitely not present
        }
    }
    return true; // Key might be present
}

// Serialize the Bloom filter to a binary file
void BloomFilter::serialize(const std::string& filename) const {
    std::ofstream out(filename, std::ios::binary);
    if (!out) {
        throw std::ios_base::failure("Unable to open Bloom filter file for writing.");
    }
    out.write(reinterpret_cast<const char*>(bit_array.data()), bit_array.size());
}

// Load a Bloom filter from a binary file
void BloomFilter::loadBitArrayFromFile(const std::string& filename) {
    std::ifstream in(filename, std::ios::binary);
    if (!in) {
        throw std::ios_base::failure("Unable to open Bloom filter file for reading.");
    }

    // Read the bit array from the file
    bit_array.assign(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
    size = bit_array.size() * 8; // Recalculate size in bits
}

// Ensure buffer size matches Bloom filter size
void BloomFilter::loadBitArrayFromBuffer(const void* buffer, size_t buffer_size) {
    if (buffer_size != bit_array.size()) {
        throw std::invalid_argument("Buffer size does not match Bloom filter size.");
    }
    std::memcpy(bit_array.data(), buffer, buffer_size);
}


// Copy the Bloom filter bit array into a memory buffer
void BloomFilter::copyBitArrayToBuffer(void* buffer) const {
    std::memcpy(buffer, bit_array.data(), bit_array.size());
}
