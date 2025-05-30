#!/bin/bash

# Spark GraphX Strongly Connected Components (SCC) Runner Script
# This script runs the SCC algorithm with various options

# Color output functions
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

print_header() {
    echo -e "${CYAN}=================================================${NC}"
    echo -e "${CYAN}         Spark GraphX SCC Algorithm Runner       ${NC}"
    echo -e "${CYAN}=================================================${NC}"
}

print_usage() {
    echo "Usage: $0 [OPTIONS]"
    echo
    echo "Options:"
    echo "  -v, --vertices FILE     Vertices file (default: sample_vertices.txt)"
    echo "  -e, --edges FILE        Edges file (default: sample_edges.txt)"
    echo "  -a, --algorithm ALG     Algorithm: builtin, kosaraju, pregel, all (default: all)"
    echo "  -h, --help             Show this help message"
    echo
    echo "Algorithms:"
    echo "  builtin    - Use GraphFrames built-in SCC algorithm"
    echo "  kosaraju   - Custom Kosaraju's algorithm implementation"
    echo "  pregel     - Pregel-style iterative message passing"
    echo "  all        - Run all algorithms and compare results"
    echo
    echo "Examples:"
    echo "  $0                                           # Run with default settings"
    echo "  $0 -a builtin                              # Use only built-in algorithm"
    echo "  $0 -v my_vertices.txt -e my_edges.txt      # Use custom files"
    echo "  $0 -a kosaraju -v graph_vertices.txt       # Kosaraju with custom vertices"
    echo
    echo "File formats:"
    echo "  Vertices file: Each line should contain 'id name'"
    echo "  Edges file: Each line should contain 'src dst' (directed edge)"
}

# Default values
VERTICES_FILE="sample_vertices.txt"
EDGES_FILE="sample_edges.txt"
ALGORITHM="all"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -v|--vertices)
            VERTICES_FILE="$2"
            shift 2
            ;;
        -e|--edges)
            EDGES_FILE="$2"
            shift 2
            ;;
        -a|--algorithm)
            ALGORITHM="$2"
            shift 2
            ;;
        -h|--help)
            print_usage
            exit 0
            ;;
        *)
            echo -e "${RED}Error: Unknown option $1${NC}"
            print_usage
            exit 1
            ;;
    esac
done

# Validate algorithm choice
if [[ ! "$ALGORITHM" =~ ^(builtin|kosaraju|pregel|all)$ ]]; then
    echo -e "${RED}Error: Invalid algorithm '$ALGORITHM'. Choose from: builtin, kosaraju, pregel, all${NC}"
    exit 1
fi

print_header

echo -e "${BLUE}Configuration:${NC}"
echo -e "  Vertices file: ${YELLOW}$VERTICES_FILE${NC}"
echo -e "  Edges file: ${YELLOW}$EDGES_FILE${NC}"
echo -e "  Algorithm: ${YELLOW}$ALGORITHM${NC}"
echo -e "  Working directory: ${YELLOW}$SCRIPT_DIR${NC}"
echo

# Check if files exist
if [[ ! -f "$VERTICES_FILE" ]]; then
    echo -e "${RED}Error: Vertices file '$VERTICES_FILE' not found${NC}"
    exit 1
fi

if [[ ! -f "$EDGES_FILE" ]]; then
    echo -e "${RED}Error: Edges file '$EDGES_FILE' not found${NC}"
    exit 1
fi

# Check Python availability
if ! command -v python3 &> /dev/null; then
    echo -e "${RED}Error: python3 is not installed or not in PATH${NC}"
    exit 1
fi

# Check if PySpark is available
echo -e "${BLUE}Checking PySpark installation...${NC}"
if ! python3 -c "import pyspark" 2>/dev/null; then
    echo -e "${YELLOW}Warning: PySpark not found. Installing...${NC}"
    pip3 install pyspark==3.5.0 || {
        echo -e "${RED}Error: Failed to install PySpark${NC}"
        exit 1
    }
fi

# Check if GraphFrames is available
if ! python3 -c "import graphframes" 2>/dev/null; then
    echo -e "${YELLOW}Note: GraphFrames will be downloaded automatically via Spark packages${NC}"
fi

echo -e "${GREEN}✓ Environment check completed${NC}"
echo

# Run the SCC algorithm
echo -e "${BLUE}Starting Spark GraphX SCC Algorithm...${NC}"
echo -e "${PURPLE}======================================${NC}"

cd "$SCRIPT_DIR"

# Set JAVA_HOME if not set (common issue)
if [[ -z "$JAVA_HOME" ]]; then
    # Try to find Java installation
    if command -v java &> /dev/null; then
        JAVA_PATH=$(which java)
        export JAVA_HOME=$(dirname $(dirname $(readlink -f $JAVA_PATH)))
        echo -e "${YELLOW}Setting JAVA_HOME to: $JAVA_HOME${NC}"
    else
        echo -e "${RED}Warning: JAVA_HOME not set and java not found in PATH${NC}"
    fi
fi

# Execute the Python script
START_TIME=$(date +%s)
python3 scc_graphx.py "$VERTICES_FILE" "$EDGES_FILE" "$ALGORITHM"
EXIT_CODE=$?
END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))

echo
echo -e "${PURPLE}======================================${NC}"

if [[ $EXIT_CODE -eq 0 ]]; then
    echo -e "${GREEN}✓ SCC algorithm completed successfully!${NC}"
    echo -e "${BLUE}Execution time: ${DURATION} seconds${NC}"
else
    echo -e "${RED}✗ SCC algorithm failed with exit code: $EXIT_CODE${NC}"
    echo
    echo -e "${YELLOW}Troubleshooting tips:${NC}"
    echo "1. Check if Java 8+ is installed: java -version"
    echo "2. Verify PySpark installation: python3 -c 'import pyspark; print(pyspark.__version__)'"
    echo "3. Check file formats and content"
    echo "4. Ensure sufficient memory is available"
    echo "5. Check the error messages above for specific issues"
fi

echo
echo -e "${BLUE}For more options, run: $0 --help${NC}" 