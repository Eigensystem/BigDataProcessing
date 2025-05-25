#!/bin/zsh

# Download SNAP dataset for PageRank testing
echo "========================================="
echo "Downloading SNAP Dataset"
echo "========================================="

# Create data directory
mkdir -p data

# Download web-Google dataset (small version)
DATASET_URL="https://snap.stanford.edu/data/web-Google.txt.gz"
DATASET_FILE="data/web-Google.txt.gz"

echo "Downloading web-Google dataset..."
echo "URL: $DATASET_URL"

if command -v wget > /dev/null; then
    wget -O "$DATASET_FILE" "$DATASET_URL"
elif command -v curl > /dev/null; then
    curl -L -o "$DATASET_FILE" "$DATASET_URL"
else
    echo "Error: Neither wget nor curl is available"
    echo "Please install wget or curl to download the dataset"
    echo "Alternative: Use the sample_graph.txt for testing"
    exit 1
fi

if [ $? -eq 0 ]; then
    echo "Dataset downloaded successfully!"
    
    # Extract the file
    echo "Extracting dataset..."
    gunzip "$DATASET_FILE"
    
    EXTRACTED_FILE="data/web-Google.txt"
    
    if [ -f "$EXTRACTED_FILE" ]; then
        echo "Dataset extracted successfully!"
        echo "File: $EXTRACTED_FILE"
        
        # Show dataset statistics
        echo "Dataset statistics:"
        echo "Total lines: $(wc -l < $EXTRACTED_FILE)"
        echo "First 10 lines:"
        head -10 "$EXTRACTED_FILE"
        
        # Count unique nodes
        echo "Counting unique nodes..."
        NODES=$(cat "$EXTRACTED_FILE" | grep -v "^#" | awk '{print $1"\n"$2}' | sort -n | uniq | wc -l)
        echo "Total nodes: $NODES"
        
        # Save node count for later use
        echo "$NODES" > data/node_count.txt
        echo "Node count saved to data/node_count.txt"
        
    else
        echo "Error: Failed to extract dataset"
        exit 1
    fi
else
    echo "Error: Failed to download dataset"
    echo "Using sample graph instead..."
    
    # Count nodes in sample graph
    NODES=$(cat sample_graph.txt | grep -v "^#" | awk '{print $1"\n"$2}' | sort -n | uniq | wc -l)
    echo "$NODES" > data/node_count.txt
    echo "Sample graph nodes: $NODES"
fi

echo "========================================="
echo "Data preparation completed!"
echo "=========================================" 