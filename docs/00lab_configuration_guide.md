# NJUDB Lab Configuration - Practical Usage Guide

## TL;DR - Quick Reference

### Most Common Use Cases

1. **Safe default (always works):**
   ```bash
   ./configure.sh --all-source
   ```

2. **Working on Lab01 (Buffer Pool):**
   ```bash
   # to check the expected begavior
   ./configure.sh --lab01-gold
   ```

3. **Working on Lab02 (Executor Basic):**
   ```bash
   ./configure.sh --lab01-gold
   ```

4. **Working on Lab03 (Executor Analysis):**
   ```bash
   ./configure.sh --lab01-gold --lab02-gold
   ```

5. **Working on Lab04 (Index):**
   ```bash
   ./configure.sh --lab01-gold --lab02-gold --lab03-gold
   ```

## Recommended Development Workflow

### Phase 1: Initial Setup
```bash
# Start with all source to ensure everything builds
./configure.sh --all-source
cd build && make -j$(nproc) && ctest
```

### Phase 2: Lab01 Development
```bash
# No dependencies
./configure.sh --clean
cd build && make -j$(nproc)
# Focus on buffer pool implementation
# Test with: ./bin/buffer_pool_test, ./bin/replacer_test
```

### Phase 3: Lab02 Development
```bash
# Use lab01 as dependency while working on lab02
./configure.sh --lab01-gold --clean
cd build && make -j$(nproc)
# Focus on basic executor implementation
```

### Phase 4: Lab03 Development
```bash
# Use lab01+lab02 as dependencies while working on lab03
./configure.sh --lab01-gold --lab02-gold --clean
cd build && make -j$(nproc)
# Focus on advanced executor implementation (joins, aggregates)
```

### Phase 5: Lab04 Development
```bash
# Use lab01+lab02+lab03 as dependencies while working on lab04
./configure.sh --lab01-gold --lab02-gold --lab03-gold --clean
cd build && make -j$(nproc)
# Focus on index implementation
```

### Phase 6: Final Verification
```bash
# Verify everything works together from source
./configure.sh --all-source --clean
cd build && make -j$(nproc) && ctest
```

### Build Fails After Configuration Change
```bash
# Always clean when switching configurations
./configure.sh --your-new-config --clean
cd build && make -j$(nproc)
```

### Linking Errors with Gold Libraries
```bash
# Fall back to source compilation
./configure.sh --all-source --clean
cd build && make -j$(nproc)
```

### CMake Warnings About Circular Dependencies
These warnings are usually safe to ignore, but if you see build failures:
```bash
# Use fewer gold libraries
./configure.sh --lab01-gold  # instead of multiple gold labs
```

## Testing Strategy

### Test Individual Labs
```bash
# After configuring and building:
cd build

# Test Lab01 components:
./bin/buffer_pool_test
./bin/replacer_test
./bin/page_guard_test

# Test Lab04 components:
./bin/b_plus_tree_test
./bin/hash_index_test

# Test all:
ctest
```

### Performance Testing
```bash
# Use Release build for performance evaluation
./configure.sh --all-source --build-type Release
cd build && make -j$(nproc)
# Run your performance tests
```

## Lab Dependencies Summary

Understanding which labs depend on others helps choose good configurations:

- **Lab01** (Buffer Pool): Foundation - no dependencies on other labs
- **Lab02** (Executor Basic): Depends on Lab01 (buffer pool)
- **Lab03** (Executor Analysis): Depends on Lab01, Lab02
- **Lab04** (Index): Depends on Lab01, lab02

This means:
- Working on Lab01: Can use Lab02,03,04 as gold
- Working on Lab02: Should use Lab01 as gold
- Working on Lab03: Should use Lab01,02 as gold  
- Working on Lab04: Should use Lab01, lab02 as gold (Lab02,03 optional)

