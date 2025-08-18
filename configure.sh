#!/bin/bash

# WSDB Lab Configuration Script
# This script helps configure which labs to compile from source or use gold libraries

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="${SCRIPT_DIR}/build"

# Default values
LAB01_GOLD=false
LAB02_GOLD=false
LAB03_GOLD=false
LAB04_GOLD=false
BUILD_TYPE="Debug"
CLEAN_BUILD=false

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

usage() {
    echo "Usage: $0 [OPTIONS]"
    echo
    echo "Configure WSDB labs to compile from source or use gold libraries"
    echo
    echo "Options:"
    echo "  --lab01-gold          Use gold library for Lab01 (Buffer Pool)"
    echo "  --lab02-gold          Use gold library for Lab02 (Executor Basic)"
    echo "  --lab03-gold          Use gold library for Lab03 (Executor Analysis)"
    echo "  --lab04-gold          Use gold library for Lab04 (Executor Index & Storage Index)"
    echo "  --all-gold            Use gold libraries for all labs (experimental - may have linking issues)"
    echo "  --all-source          Compile all labs from source (default, recommended)"
    echo "  --build-type TYPE     Build type: Debug (default) or Release"
    echo "  --clean               Clean build directory before building"
    echo "  -h, --help            Show this help message"
    echo
    echo "Examples:"
    echo "  $0 --lab01-gold --lab02-gold    # Use gold for lab01 and lab02, compile lab03 and lab04"
    echo "  $0 --lab02-gold --lab03-gold --lab04-gold  # Working on Lab01"
    echo "  $0 --all-source                 # Compile all from source (safest option)"
    echo
    echo "Note: --all-gold may encounter linking issues due to dependency complexities."
    echo "      It's recommended to use individual lab gold libraries or --all-source."
}

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --lab01-gold)
            LAB01_GOLD=true
            shift
            ;;
        --lab02-gold)
            LAB02_GOLD=true
            shift
            ;;
        --lab03-gold)
            LAB03_GOLD=true
            shift
            ;;
        --lab04-gold)
            LAB04_GOLD=true
            shift
            ;;
        --all-gold)
            LAB01_GOLD=true
            LAB02_GOLD=true
            LAB03_GOLD=true
            LAB04_GOLD=true
            shift
            ;;
        --all-source)
            LAB01_GOLD=false
            LAB02_GOLD=false
            LAB03_GOLD=false
            LAB04_GOLD=false
            shift
            ;;
        --build-type)
            BUILD_TYPE="$2"
            if [[ "$BUILD_TYPE" != "Debug" && "$BUILD_TYPE" != "Release" ]]; then
                log_error "Invalid build type: $BUILD_TYPE. Must be Debug or Release."
                exit 1
            fi
            shift 2
            ;;
        --clean)
            CLEAN_BUILD=true
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            log_error "Unknown option: $1"
            usage
            exit 1
            ;;
    esac
done

# Print configuration
echo
log_info "WSDB Lab Configuration:"
echo "  Lab01 (Buffer Pool): $([ "$LAB01_GOLD" = true ] && echo "Gold Library" || echo "Compile from Source")"
echo "  Lab02 (Executor Basic): $([ "$LAB02_GOLD" = true ] && echo "Gold Library" || echo "Compile from Source")"
echo "  Lab03 (Executor Analysis): $([ "$LAB03_GOLD" = true ] && echo "Gold Library" || echo "Compile from Source")"
echo "  Lab04 (Executor Index & Storage Index): $([ "$LAB04_GOLD" = true ] && echo "Gold Library" || echo "Compile from Source")"
echo "  Build Type: $BUILD_TYPE"
echo

# Check if gold libraries exist when needed
if [[ "$LAB01_GOLD" = true || "$LAB02_GOLD" = true || "$LAB03_GOLD" = true || "$LAB04_GOLD" = true ]]; then
    PLATFORM="macos"
    if [[ "$OSTYPE" == "linux-gnu"* ]]; then
        PLATFORM="linux"
    fi
    
    BUILD_TYPE_LOWER=$(echo "$BUILD_TYPE" | tr '[:upper:]' '[:lower:]')
    GOLD_LIB_PATH="${SCRIPT_DIR}/lib-gold/${PLATFORM}/${BUILD_TYPE_LOWER}"
    
    if [[ ! -d "$GOLD_LIB_PATH" ]]; then
        log_error "Gold library directory not found: $GOLD_LIB_PATH"
        exit 1
    fi
    
    log_info "Gold libraries will be loaded from: $GOLD_LIB_PATH"
fi

# Clean build directory if requested
if [[ "$CLEAN_BUILD" = true ]]; then
    log_info "Cleaning build directory..."
    rm -rf "$BUILD_DIR"
fi

# Create build directory
mkdir -p "$BUILD_DIR"
cd "$BUILD_DIR"

# Configure CMake with lab options
CMAKE_ARGS=(
    "-DCMAKE_BUILD_TYPE=$BUILD_TYPE"
    "-DUSE_GOLD_LAB01=$([ "$LAB01_GOLD" = true ] && echo "ON" || echo "OFF")"
    "-DUSE_GOLD_LAB02=$([ "$LAB02_GOLD" = true ] && echo "ON" || echo "OFF")"
    "-DUSE_GOLD_LAB03=$([ "$LAB03_GOLD" = true ] && echo "ON" || echo "OFF")"
    "-DUSE_GOLD_LAB04=$([ "$LAB04_GOLD" = true ] && echo "ON" || echo "OFF")"
)

log_info "Configuring with CMake..."
cmake "${CMAKE_ARGS[@]}" ..

if [[ $? -eq 0 ]]; then
    log_success "CMake configuration completed successfully!"
    echo
    log_info "To build the project, run:"
    echo "  cd build && make -j\$(nproc)"
    echo
    log_info "To run tests, run:"
    echo "  cd build && ctest"
else
    log_error "CMake configuration failed!"
    exit 1
fi
