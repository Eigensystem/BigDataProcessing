#!/bin/bash

# GraphX DFS Runner Script
# Usage: ./run_dfs.sh [vertices_file] [edges_file] [start_node]

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default files
DEFAULT_VERTICES="sample_vertices.txt"
DEFAULT_EDGES="sample_edges.txt"
DEFAULT_START="A"

# Function to print colored output
print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to show usage
show_usage() {
    echo "GraphX DFS Implementation - Usage Guide"
    echo "====================================="
    echo
    echo "Usage: $0 [vertices_file] [edges_file] [start_node]"
    echo
    echo "Arguments:"
    echo "  vertices_file   - Path to vertices file (default: $DEFAULT_VERTICES)"
    echo "  edges_file      - Path to edges file (default: $DEFAULT_EDGES)"
    echo "  start_node      - Starting node for DFS (default: $DEFAULT_START)"
    echo
    echo "File formats:"
    echo "  Vertices file: Each line should contain 'id,name'"
    echo "  Edges file: Each line should contain 'src,dst,relationship'"
    echo
    echo "Examples:"
    echo "  $0                                    # Use default files"
    echo "  $0 my_vertices.txt my_edges.txt      # Custom files, default start"
    echo "  $0 vertices.csv edges.csv B          # Custom files and start node"
    echo
    echo "Options:"
    echo "  -h, --help      Show this help message"
    echo "  -s, --sample    Create sample input files"
    echo "  -c, --check     Check if required files exist"
}

# Function to create sample files
create_sample_files() {
    print_info "Creating sample input files..."
    
    cat > "$DEFAULT_VERTICES" << EOF
# Sample vertices file
# Format: id,name
A,Node A
B,Node B
C,Node C
D,Node D
E,Node E
F,Node F
G,Node G
H,Node H
I,Node I
EOF

    cat > "$DEFAULT_EDGES" << EOF
# Sample edges file
# Format: src,dst,relationship
A,B,edge_AB
A,C,edge_AC
B,D,edge_BD
B,E,edge_BE
C,F,edge_CF
D,G,edge_DG
E,H,edge_EH
F,I,edge_FI
EOF

    print_success "Sample files created:"
    print_success "  - $DEFAULT_VERTICES"
    print_success "  - $DEFAULT_EDGES"
}

# Function to check if files exist
check_files() {
    local vertices_file="$1"
    local edges_file="$2"
    
    if [[ ! -f "$vertices_file" ]]; then
        print_error "Vertices file not found: $vertices_file"
        return 1
    fi
    
    if [[ ! -f "$edges_file" ]]; then
        print_error "Edges file not found: $edges_file"
        return 1
    fi
    
    print_success "Input files found:"
    print_success "  - Vertices: $vertices_file"
    print_success "  - Edges: $edges_file"
    return 0
}

# Function to validate Python environment
check_python_env() {
    print_info "Checking Python environment..."
    
    # Check if python3 is available
    if ! command -v python3 &> /dev/null; then
        print_error "python3 not found. Please install Python 3."
        exit 1
    fi
    
    # Check if pip is available
    if ! command -v pip3 &> /dev/null && ! command -v pip &> /dev/null; then
        print_warning "pip not found. You may need to install dependencies manually."
    fi
    
    print_success "Python environment OK"
}

# Function to install dependencies
install_dependencies() {
    print_info "Installing Python dependencies..."
    
    if [[ -f "requirements.txt" ]]; then
        if command -v pip3 &> /dev/null; then
            pip3 install -r requirements.txt
        elif command -v pip &> /dev/null; then
            pip install -r requirements.txt
        else
            print_warning "pip not available. Please install dependencies manually."
        fi
    else
        print_warning "requirements.txt not found. Assuming dependencies are installed."
    fi
}

# Main execution function
run_dfs() {
    local vertices_file="$1"
    local edges_file="$2"
    local start_node="$3"
    
    print_info "Starting GraphX DFS execution..."
    print_info "Vertices file: $vertices_file"
    print_info "Edges file: $edges_file"
    print_info "Start node: $start_node"
    echo
    
    # Run the Python script
    if [[ -n "$start_node" ]]; then
        python3 graph_dfs.py "$vertices_file" "$edges_file" "$start_node"
    else
        python3 graph_dfs.py "$vertices_file" "$edges_file"
    fi
    
    local exit_code=$?
    
    if [[ $exit_code -eq 0 ]]; then
        print_success "DFS execution completed successfully!"
    else
        print_error "DFS execution failed with exit code $exit_code"
    fi
    
    return $exit_code
}

# Parse command line arguments
case "$1" in
    -h|--help)
        show_usage
        exit 0
        ;;
    -s|--sample)
        create_sample_files
        exit 0
        ;;
    -c|--check)
        vertices_file="${2:-$DEFAULT_VERTICES}"
        edges_file="${3:-$DEFAULT_EDGES}"
        check_files "$vertices_file" "$edges_file"
        exit $?
        ;;
esac

# Set file paths
vertices_file="${1:-$DEFAULT_VERTICES}"
edges_file="${2:-$DEFAULT_EDGES}"
start_node="$3"

print_info "GraphX DFS Implementation"
print_info "========================"

# Environment checks
check_python_env

# Check if sample files need to be created
if [[ "$vertices_file" == "$DEFAULT_VERTICES" && ! -f "$DEFAULT_VERTICES" ]]; then
    print_warning "Default vertices file not found. Creating sample files..."
    create_sample_files
fi

if [[ "$edges_file" == "$DEFAULT_EDGES" && ! -f "$DEFAULT_EDGES" ]]; then
    print_warning "Default edges file not found. Creating sample files..."
    create_sample_files
fi

# Validate input files
if ! check_files "$vertices_file" "$edges_file"; then
    print_error "File validation failed. Use -s option to create sample files."
    exit 1
fi

# Install dependencies if needed
install_dependencies

# Run DFS
run_dfs "$vertices_file" "$edges_file" "$start_node"
