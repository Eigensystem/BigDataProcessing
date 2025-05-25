#!/bin/zsh

# Display HDFS file content to terminal
# Usage: ./cat_hdfs_file.sh <hdfs_file_path>

if [ $# -ne 1 ]; then
    echo "Usage: $0 <hdfs_file_path>"
    echo "Example: $0 /user/data/test.txt"
    exit 1
fi

HDFS_PATH=$1

# Check if file exists in HDFS
if ! hdfs dfs -test -e "$HDFS_PATH"; then
    echo "Error: File '$HDFS_PATH' does not exist in HDFS"
    exit 1
fi

# Check if it's a file (not directory)
if hdfs dfs -test -d "$HDFS_PATH"; then
    echo "Error: '$HDFS_PATH' is a directory, not a file"
    exit 1
fi

echo "========================================="
echo "Content of HDFS file: $HDFS_PATH"
echo "========================================="

# Display file content
hdfs dfs -cat "$HDFS_PATH"

if [ $? -ne 0 ]; then
    echo "Failed to read file content"
    exit 1
fi

echo "========================================="
echo "End of file content"
echo "=========================================" 