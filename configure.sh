#!/bin/bash

# NJUDB Lab Configuration Script
# This script helps configure which labs to compile from source or use gold libraries

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="${SCRIPT_DIR}/build"

# ==================== Student configuration ====================
# Labs listed here compile from source; every other lab uses gold libraries.
# Examples: "05", "05 06", "all" (all source), or "" (all gold).
SOURCE_LABS="all"
ENABLE_LAB05_MVCC=false
ENABLE_LAB06_WAL=false
BUILD_TYPE="Debug"
CLEAN_BUILD=false
# ===============================================================

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

usage() {
    echo "Usage: $0 [OPTIONS]"
    echo
    echo "Configure NJUDB labs to compile from source or use gold libraries"
    echo
    echo "Preferred workflow: edit SOURCE_LABS and feature flags at the top"
    echo "of configure.sh, then run the script without arguments."
    echo
    echo "Options:"
    echo "  --lab01-gold          Use gold library for Lab01 (Buffer Pool)"
    echo "  --lab02-gold          Use gold library for Lab02 (Executor Basic)"
    echo "  --lab03-gold          Use gold library for Lab03 (Executor Analysis)"
    echo "  --lab04-gold          Use gold library for Lab04 (Executor Index & Storage Index)"
    echo "  --lab05-gold          Use gold libraries for Lab05 (Concurrency and MVCC Bonus)"
    echo "  --lab06-gold          Use gold libraries for Lab06 (WAL and Recovery)"
    echo "  --all-gold            Use gold libraries for all labs"
    echo "  --all-source          Compile all labs from source (repository default)"
    echo "  --enable-lab05-mvcc   Enable the Lab05 MVCC bonus runtime path"
    echo "  --disable-lab05-mvcc  Disable the Lab05 MVCC bonus runtime path"
    echo "  --enable-lab06-wal    Enable the Lab06 WAL/recovery runtime path"
    echo "  --disable-lab06-wal   Disable the Lab06 WAL/recovery runtime path"
    echo "  --build-type TYPE     Build type: Debug (default) or Release"
    echo "  --clean               Clean build directory before building"
    echo "  -h, --help            Show this help message"
    echo
    echo "Examples:"
    echo "  SOURCE_LABS=\"05\" in this file; $0       # Work on Lab05"
    echo "  SOURCE_LABS=\"05 06\" in this file; $0    # Work on Lab05 and Lab06"
    echo "  $0 --all-source                           # Temporary CLI override"
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

# Internal source/gold state derived from SOURCE_LABS. Command-line arguments
# below may override these values without changing the editable defaults.
LAB01_GOLD=true
LAB02_GOLD=true
LAB03_GOLD=true
LAB04_GOLD=true
LAB05_GOLD=true
LAB06_GOLD=true

validate_boolean() {
    local variable_name=$1
    local variable_value=$2
    case "$variable_value" in
        true|false) ;;
        *)
            log_error "$variable_name must be true or false (got: $variable_value)"
            exit 1
            ;;
    esac
}

validate_boolean "ENABLE_LAB05_MVCC" "$ENABLE_LAB05_MVCC"
validate_boolean "ENABLE_LAB06_WAL" "$ENABLE_LAB06_WAL"
validate_boolean "CLEAN_BUILD" "$CLEAN_BUILD"

SOURCE_LAB_TOKENS=()
if [[ -n "${SOURCE_LABS//[[:space:]]/}" ]]; then
    read -r -a SOURCE_LAB_TOKENS <<< "$SOURCE_LABS"
fi

if [[ ${#SOURCE_LAB_TOKENS[@]} -gt 0 && "${SOURCE_LAB_TOKENS[0]}" == "all" ]]; then
    if [[ ${#SOURCE_LAB_TOKENS[@]} -ne 1 ]]; then
        log_error "SOURCE_LABS value 'all' cannot be combined with lab numbers"
        exit 1
    fi
    LAB01_GOLD=false
    LAB02_GOLD=false
    LAB03_GOLD=false
    LAB04_GOLD=false
    LAB05_GOLD=false
    LAB06_GOLD=false
else
    for lab in "${SOURCE_LAB_TOKENS[@]}"; do
        case "$lab" in
            01) LAB01_GOLD=false ;;
            02) LAB02_GOLD=false ;;
            03) LAB03_GOLD=false ;;
            04) LAB04_GOLD=false ;;
            05) LAB05_GOLD=false ;;
            06) LAB06_GOLD=false ;;
            all)
                log_error "SOURCE_LABS value 'all' cannot be combined with lab numbers"
                exit 1
                ;;
            *)
                log_error "Invalid SOURCE_LABS entry: $lab (expected 01-06 or all)"
                exit 1
                ;;
        esac
    done
fi

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
        --lab05-gold)
            LAB05_GOLD=true
            shift
            ;;
        --lab06-gold)
            LAB06_GOLD=true
            shift
            ;;
        --all-gold)
            LAB01_GOLD=true
            LAB02_GOLD=true
            LAB03_GOLD=true
            LAB04_GOLD=true
            LAB05_GOLD=true
            LAB06_GOLD=true
            shift
            ;;
        --all-source)
            LAB01_GOLD=false
            LAB02_GOLD=false
            LAB03_GOLD=false
            LAB04_GOLD=false
            LAB05_GOLD=false
            LAB06_GOLD=false
            shift
            ;;
        --enable-lab05-mvcc)
            ENABLE_LAB05_MVCC=true
            shift
            ;;
        --disable-lab05-mvcc)
            ENABLE_LAB05_MVCC=false
            shift
            ;;
        --enable-lab06-wal)
            ENABLE_LAB06_WAL=true
            shift
            ;;
        --disable-lab06-wal)
            ENABLE_LAB06_WAL=false
            shift
            ;;
        --build-type)
            if [[ $# -lt 2 ]]; then
                log_error "--build-type requires Debug or Release"
                exit 1
            fi
            BUILD_TYPE="$2"
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

if [[ "$BUILD_TYPE" != "Debug" && "$BUILD_TYPE" != "Release" ]]; then
    log_error "Invalid build type: $BUILD_TYPE. Must be Debug or Release."
    exit 1
fi

# Print configuration
echo
log_info "NJUDB Lab Configuration:"
echo "  Lab01 (Buffer Pool): $([ "$LAB01_GOLD" = true ] && echo "Gold Library" || echo "Compile from Source")"
echo "  Lab02 (Executor Basic): $([ "$LAB02_GOLD" = true ] && echo "Gold Library" || echo "Compile from Source")"
echo "  Lab03 (Executor Analysis): $([ "$LAB03_GOLD" = true ] && echo "Gold Library" || echo "Compile from Source")"
echo "  Lab04 (Executor Index & Storage Index): $([ "$LAB04_GOLD" = true ] && echo "Gold Library" || echo "Compile from Source")"
echo "  Lab05 (Concurrency and MVCC Bonus): $([ "$LAB05_GOLD" = true ] && echo "Gold Library" || echo "Compile from Source")"
echo "  Lab06 (WAL and Recovery): $([ "$LAB06_GOLD" = true ] && echo "Gold Library" || echo "Compile from Source")"
echo "  Lab05 MVCC Bonus: $([ "$ENABLE_LAB05_MVCC" = true ] && echo "Enabled" || echo "Disabled")"
echo "  Lab06 WAL/Recovery: $([ "$ENABLE_LAB06_WAL" = true ] && echo "Enabled" || echo "Disabled")"
echo "  Build Type: $BUILD_TYPE"
echo "  Clean Build: $([ "$CLEAN_BUILD" = true ] && echo "Yes" || echo "No")"
echo

# Check if gold libraries exist when needed
if [[ "$LAB01_GOLD" = true || "$LAB02_GOLD" = true || "$LAB03_GOLD" = true || "$LAB04_GOLD" = true || "$LAB05_GOLD" = true || "$LAB06_GOLD" = true ]]; then
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
    "-DUSE_GOLD_LAB05=$([ "$LAB05_GOLD" = true ] && echo "ON" || echo "OFF")"
    "-DUSE_GOLD_LAB06=$([ "$LAB06_GOLD" = true ] && echo "ON" || echo "OFF")"
    "-DENABLE_LAB05_MVCC=$([ "$ENABLE_LAB05_MVCC" = true ] && echo "ON" || echo "OFF")"
    "-DENABLE_LAB06_WAL=$([ "$ENABLE_LAB06_WAL" = true ] && echo "ON" || echo "OFF")"
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
