#!/bin/zsh

# Run WordCount MapReduce job
echo "========================================="
echo "Running WordCount MapReduce Job"
echo "========================================="

# Define paths
INPUT_DIR="/user/input"
OUTPUT_DIR="/user/output"
LOCAL_INPUT_DIR="."

# Check if JAR file exists
if [ ! -f "wordcount.jar" ]; then
    echo "Error: wordcount.jar not found. Please run ./compile.sh first"
    exit 1
fi

# Check if input files exist
if [ ! -f "input1.txt" ] || [ ! -f "input2.txt" ]; then
    echo "Error: Input files not found"
    exit 1
fi

echo "Step 1: Preparing HDFS directories..."

# Remove existing directories (ignore errors if they don't exist)
hdfs dfs -rm -r $INPUT_DIR 2>/dev/null || true
hdfs dfs -rm -r $OUTPUT_DIR 2>/dev/null || true

# Create input directory
hdfs dfs -mkdir -p $INPUT_DIR

if [ $? -ne 0 ]; then
    echo "Error: Failed to create input directory"
    exit 1
fi

echo "Step 2: Uploading input files to HDFS..."

# Upload input files
hdfs dfs -put input1.txt $INPUT_DIR/
hdfs dfs -put input2.txt $INPUT_DIR/

if [ $? -ne 0 ]; then
    echo "Error: Failed to upload input files"
    exit 1
fi

# Verify files were uploaded
echo "Files in HDFS input directory:"
hdfs dfs -ls $INPUT_DIR

echo "Step 3: Running MapReduce job..."

# Run the WordCount job
hadoop jar wordcount.jar WordCount $INPUT_DIR $OUTPUT_DIR

if [ $? -ne 0 ]; then
    echo "Error: MapReduce job failed"
    exit 1
fi

echo "Step 4: Displaying results..."

# Show output directory contents
echo "Files in HDFS output directory:"
hdfs dfs -ls $OUTPUT_DIR

# Display the results
echo "========================================="
echo "WordCount Results:"
echo "========================================="
hdfs dfs -cat $OUTPUT_DIR/part-r-00000

echo "========================================="
echo "Job completed successfully!"
echo "========================================="

# Optional: Download results to local file
echo "Downloading results to local file 'wordcount_results.txt'..."
hdfs dfs -get $OUTPUT_DIR/part-r-00000 wordcount_results.txt

if [ $? -eq 0 ]; then
    echo "Results saved to: wordcount_results.txt"
    echo "Total unique words found: $(wc -l < wordcount_results.txt)"
fi 