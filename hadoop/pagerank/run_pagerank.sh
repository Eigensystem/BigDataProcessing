#!/bin/zsh

# Run PageRank MapReduce job
echo "========================================="
echo "Running PageRank MapReduce Job"
echo "========================================="

# Configuration
INPUT_DIR="/user/pagerank/input"
PROCESSED_DIR="/user/pagerank/processed"
OUTPUT_DIR="/user/pagerank/output"
ITERATIONS=10

# Check if JAR files exist
if [ ! -f "pagerank.jar" ] || [ ! -f "preprocessor.jar" ]; then
    echo "Error: JAR files not found. Please run ./compile.sh first"
    exit 1
fi

# Determine input file and node count
INPUT_FILE=""
NODES=0

if [ -f "data/web-Google.txt" ]; then
    INPUT_FILE="data/web-Google.txt"
    if [ -f "data/node_count.txt" ]; then
        NODES=$(cat data/node_count.txt)
    else
        echo "Counting nodes in dataset..."
        NODES=$(cat "$INPUT_FILE" | grep -v "^#" | awk '{print $1"\n"$2}' | sort -n | uniq | wc -l)
    fi
    echo "Using SNAP web-Google dataset with $NODES nodes"
elif [ -f "sample_graph.txt" ]; then
    INPUT_FILE="sample_graph.txt"
    NODES=$(cat "$INPUT_FILE" | grep -v "^#" | awk '{print $1"\n"$2}' | sort -n | uniq | wc -l)
    echo "Using sample graph with $NODES nodes"
else
    echo "Error: No input data found. Please run ./download_data.sh or ensure sample_graph.txt exists"
    exit 1
fi

echo "Input file: $INPUT_FILE"
echo "Total nodes: $NODES"
echo "Iterations: $ITERATIONS"

echo "Step 1: Preparing HDFS directories..."

# Clean HDFS directories
hdfs dfs -rm -r $INPUT_DIR 2>/dev/null || true
hdfs dfs -rm -r $PROCESSED_DIR 2>/dev/null || true
hdfs dfs -rm -r $OUTPUT_DIR 2>/dev/null || true

# Create directories
hdfs dfs -mkdir -p $INPUT_DIR
hdfs dfs -mkdir -p $(dirname $PROCESSED_DIR)
hdfs dfs -mkdir -p $(dirname $OUTPUT_DIR)

echo "Step 2: Uploading input data to HDFS..."

# Upload input file
hdfs dfs -put "$INPUT_FILE" $INPUT_DIR/
if [ $? -ne 0 ]; then
    echo "Error: Failed to upload input file"
    exit 1
fi

echo "Files in HDFS input directory:"
hdfs dfs -ls $INPUT_DIR

echo "Step 3: Preprocessing graph data..."

# Run graph preprocessor
echo "Converting graph to PageRank format..."
hadoop jar preprocessor.jar GraphPreprocessor $INPUT_DIR $PROCESSED_DIR $NODES

if [ $? -ne 0 ]; then
    echo "Error: Graph preprocessing failed"
    exit 1
fi

echo "Preprocessed data:"
hdfs dfs -ls $PROCESSED_DIR
echo "Sample preprocessed records:"
hdfs dfs -cat $PROCESSED_DIR/part-r-00000 | head -5

echo "Step 4: Running PageRank algorithm..."

# Run PageRank iterations
hadoop jar pagerank.jar PageRank $PROCESSED_DIR $OUTPUT_DIR $ITERATIONS $NODES

if [ $? -ne 0 ]; then
    echo "Error: PageRank algorithm failed"
    exit 1
fi

echo "Step 5: Displaying results..."

# Show output directory contents
echo "Files in HDFS output directory:"
hdfs dfs -ls $OUTPUT_DIR

echo "Iteration directories:"
hdfs dfs -ls $OUTPUT_DIR/ | grep iter

echo "Final results directory:"
hdfs dfs -ls $OUTPUT_DIR/final

# Display top PageRank results
echo "========================================="
echo "Top 20 PageRank Results:"
echo "========================================="
hdfs dfs -cat $OUTPUT_DIR/final/part-r-00000 | \
    awk '{print $2"\t"$1}' | \
    sort -nr | \
    head -20 | \
    awk '{print "Node " $2 ": " $1}'

echo "========================================="
echo "PageRank Job completed successfully!"
echo "========================================="

# Optional: Download results to local file
echo "Downloading results to local file 'pagerank_results.txt'..."
hdfs dfs -get $OUTPUT_DIR/final/part-r-00000 pagerank_results.txt

if [ $? -eq 0 ]; then
    echo "Results saved to: pagerank_results.txt"
    echo "Total nodes processed: $(wc -l < pagerank_results.txt)"
    
    # Show statistics
    echo ""
    echo "PageRank Statistics:"
    echo "==================="
    
    # Calculate average PageRank
    AVG_PR=$(awk -F'\t' '{sum += $2; count++} END {print sum/count}' pagerank_results.txt)
    echo "Average PageRank: $AVG_PR"
    
    # Find max PageRank
    MAX_PR=$(awk -F'\t' '{if ($2 > max) {max = $2; node = $1}} END {print node "\t" max}' pagerank_results.txt)
    echo "Highest PageRank: Node $(echo $MAX_PR | cut -f1) with value $(echo $MAX_PR | cut -f2)"
    
    # Find min PageRank
    MIN_PR=$(awk -F'\t' 'NR==1 {min = $2; node = $1} {if ($2 < min) {min = $2; node = $1}} END {print node "\t" min}' pagerank_results.txt)
    echo "Lowest PageRank: Node $(echo $MIN_PR | cut -f1) with value $(echo $MIN_PR | cut -f2)"
fi 