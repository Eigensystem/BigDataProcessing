#!/bin/zsh

# List HDFS directory recursively with file information
# Usage: ./list_hdfs_dir_recursive.sh <hdfs_directory_path>

if [ $# -ne 1 ]; then
    echo "Usage: $0 <hdfs_directory_path>"
    echo "Example: $0 /user/data"
    exit 1
fi

HDFS_PATH=$1

# Check if path exists in HDFS
if ! hdfs dfs -test -e "$HDFS_PATH"; then
    echo "Error: Path '$HDFS_PATH' does not exist in HDFS"
    exit 1
fi

echo "========================================="
echo "Recursive listing of HDFS directory: $HDFS_PATH"
echo "========================================="

# Check if it's a directory
if hdfs dfs -test -d "$HDFS_PATH"; then
    echo "Listing directory recursively..."
    echo ""
    
    # List recursively with details
    hdfs dfs -ls -R -h "$HDFS_PATH"
    
    if [ $? -eq 0 ]; then
        echo ""
        echo "========================================="
        
        # Count total files and directories
        TOTAL_ITEMS=$(hdfs dfs -ls -R "$HDFS_PATH" 2>/dev/null | grep -v "^Found" | wc -l)
        FILE_COUNT=$(hdfs dfs -ls -R "$HDFS_PATH" 2>/dev/null | grep -v "^Found" | grep "^-" | wc -l)
        DIR_COUNT=$(hdfs dfs -ls -R "$HDFS_PATH" 2>/dev/null | grep -v "^Found" | grep "^d" | wc -l)
        
        echo "Summary:"
        echo "Total items: $TOTAL_ITEMS"
        echo "Files: $FILE_COUNT"
        echo "Directories: $DIR_COUNT"
        echo "========================================="
    else
        echo "Failed to list directory contents"
        exit 1
    fi
else
    echo "'$HDFS_PATH' is a file, not a directory"
    echo "Showing file information instead:"
    echo ""
    hdfs dfs -ls -h "$HDFS_PATH"
fi 