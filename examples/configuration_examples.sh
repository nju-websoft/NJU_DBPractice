#!/bin/bash

# NJUDB Configuration Examples
# This script demonstrates various configuration scenarios for NJUDB labs

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.."

echo "=== NJUDB Lab Configuration Examples ==="
echo

echo "Example 1: Working on Lab01 (Buffer Pool)"
echo "Use gold libraries for lab02, lab03, lab04 while compiling lab01 from source"
echo "Command: ./configure.sh --lab02-gold --lab03-gold --lab04-gold"
echo

echo "Example 2: Working on Lab02 (Executor Basic)"
echo "Use gold libraries for lab01, lab03, lab04 while compiling lab02 from source"
echo "Command: ./configure.sh --lab01-gold --lab03-gold --lab04-gold"
echo

echo "Example 3: Working on Lab03 (Executor Analysis)"
echo "Use gold libraries for lab01, lab02, lab04 while compiling lab03 from source"
echo "Command: ./configure.sh --lab01-gold --lab02-gold --lab04-gold"
echo

echo "Example 4: Working on Lab04 (Executor Index & Storage Index)"
echo "Use gold libraries for lab01, lab02, lab03 while compiling lab04 from source"
echo "Command: ./configure.sh --lab01-gold --lab02-gold --lab03-gold"
echo

echo "Example 5: All labs from source (default)"
echo "Compile all labs from source - useful for final verification"
echo "Command: ./configure.sh --all-source"
echo

echo "Example 6: Progressive development"
echo "Start with dependencies and work your way up:"
echo "  1. ./configure.sh --lab02-gold --lab03-gold --lab04-gold  # Work on Lab01"
echo "  2. ./configure.sh --lab03-gold --lab04-gold              # Work on Lab02"
echo "  3. ./configure.sh --lab04-gold                           # Work on Lab03"
echo "  4. ./configure.sh --all-source                           # Work on Lab04"
echo

echo "Example 7: Release build"
echo "Build optimized version for performance testing"
echo "Command: ./configure.sh --all-source --build-type Release"
echo

echo "Example 8: Clean build"
echo "Start fresh with a clean build directory"
echo "Command: ./configure.sh --all-source --clean"
echo

echo "=== Running Example 1: Working on Lab01 ==="
read -p "Press Enter to run Example 1 or Ctrl+C to exit..."

./configure.sh --lab02-gold --lab03-gold --lab04-gold

echo
echo "Configuration complete! The project is now set up to:"
echo "- Compile Lab01 (Buffer Pool) from source"
echo "- Use gold libraries for Lab02, Lab03, and Lab04"
echo
echo "To build: cd build && make -j\$(nproc)"
echo "To test:  cd build && ctest"
