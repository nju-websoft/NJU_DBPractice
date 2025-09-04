/*------------------------------------------------------------------------------
 - Copyright (c) 2024. Websoft research group, Nanjing University.
 -
 - This program is free software: you can redistribute it and/or modify
 - it under the terms of the GNU General Public License as published by
 - the Free Software Foundation, either version 3 of the License, or
 - (at your option) any later version.
 -
 - This program is distributed in the hope that it will be useful,
 - but WITHOUT ANY WARRANTY; without even the implied warranty of
 - MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 - GNU General Public License for more details.
 -
 - You should have received a copy of the GNU General Public License
 - along with this program.  If not, see <https://www.gnu.org/licenses/>.
 -----------------------------------------------------------------------------*/

#ifndef NJUDB_BLOOM_FILTER_H
#define NJUDB_BLOOM_FILTER_H

#include <vector>
#include <functional>
#include <algorithm>

namespace njudb {

/**
 * Simple Bloom Filter implementation for hash join optimization
 * 
 * A bloom filter is a space-efficient probabilistic data structure that is used to test
 * whether an element is a member of a set. False positive matches are possible, but false
 * negatives are not – in other words, a query returns either "possibly in set" or 
 * "definitely not in set".
 * 
 * Features:
 * - Configurable size and number of hash functions
 * - No false negatives (if element exists, bloom filter will always say "might contain")
 * - Small false positive rate (configurable based on size and hash functions)
 * - Space efficient compared to storing actual elements
 */
class BloomFilter
{
public:
  /**
   * Constructor
   * @param size Number of bits in the bloom filter
   * @param num_hash_functions Number of hash functions to use (default: 3)
   */
  explicit BloomFilter(size_t size, size_t num_hash_functions = 3);
  
  /**
   * Insert a hash value into the bloom filter
   * @param hash The hash value to insert
   */
  void Insert(size_t hash);
  
  /**
   * Check if a hash value might be contained in the bloom filter
   * @param hash The hash value to check
   * @return true if might be present (could be false positive), false if definitely not present
   */
  [[nodiscard]] auto MightContain(size_t hash) const -> bool;
  
  /**
   * Clear all bits in the bloom filter
   */
  void Clear();
  
private:
  std::vector<bool> bits_;
  size_t num_hash_functions_;
  
  /**
   * Generate hash value using different seeds to create multiple hash functions
   * @param value The input value to hash
   * @param seed The seed to create different hash functions
   * @return Hash value
   */
  [[nodiscard]] auto Hash(size_t value, size_t seed) const -> size_t;
};

// ===== Implementation =====

inline BloomFilter::BloomFilter(size_t size, size_t num_hash_functions)
    : bits_(size, false), num_hash_functions_(num_hash_functions)
{}

inline void BloomFilter::Insert(size_t hash)
{
  // Use multiple hash functions with different seeds to set multiple bits
  for (size_t i = 0; i < num_hash_functions_; ++i) {
    size_t index = Hash(hash, i) % bits_.size();  // Each i creates a different hash function
    bits_[index] = true;
  }
}

inline auto BloomFilter::MightContain(size_t hash) const -> bool
{
  // Check all hash functions - if ANY bit is 0, the element is definitely not present
  for (size_t i = 0; i < num_hash_functions_; ++i) {
    size_t index = Hash(hash, i) % bits_.size();  // Same hash functions as Insert()
    if (!bits_[index]) {
      return false;  // Definitely not present
    }
  }
  return true;  // Might be present (could be false positive)
}

inline void BloomFilter::Clear()
{
  std::fill(bits_.begin(), bits_.end(), false);
}

inline auto BloomFilter::Hash(size_t value, size_t seed) const -> size_t
{
  // Simple hash function combination using different seeds
  // This creates multiple independent hash functions from a single hash value
  return std::hash<size_t>{}(value + seed * 0x9e3779b9);
}

}  // namespace njudb

#endif  // NJUDB_BLOOM_FILTER_H