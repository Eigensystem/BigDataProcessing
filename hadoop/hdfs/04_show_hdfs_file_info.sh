#!/bin/zsh

# Show HDFS file information (permissions, size, creation time, path)
# Usage: ./show_hdfs_file_info.sh <hdfs_file_path>

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

echo "========================================="
echo "HDFS File Information"
echo "========================================="
echo "File Path: $HDFS_PATH"
echo "========================================="

# Show detailed file information
hdfs dfs -ls -h "$HDFS_PATH"

echo "========================================="

# Show additional statistics
echo "Additional Statistics:"
hdfs dfs -stat "Block Size: %o bytes, Replication: %r, Modification Time: %Y" "$HDFS_PATH"

# Check if it's a directory or file
if hdfs dfs -test -d "$HDFS_PATH"; then
    echo "Type: Directory"
    # Count files in directory
    FILE_COUNT=$(hdfs dfs -ls "$HDFS_PATH" 2>/dev/null | grep -v "^Found" | wc -l)
    echo "Contains: $FILE_COUNT items"
else
    echo "Type: File"
    # Show file size in different units
    SIZE_BYTES=$(hdfs dfs -stat %b "$HDFS_PATH")
    echo "Size in bytes: $SIZE_BYTES"
fi

echo "=========================================" 