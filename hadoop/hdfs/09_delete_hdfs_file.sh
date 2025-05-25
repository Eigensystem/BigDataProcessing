#!/bin/zsh

# Delete HDFS file
# Usage: ./delete_hdfs_file.sh <hdfs_file_path>

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
    echo "Use delete_hdfs_dir.sh or manage_hdfs_dir.sh to delete directories"
    exit 1
fi

echo "========================================="
echo "File to be deleted:"
echo "========================================="
hdfs dfs -ls -h "$HDFS_PATH"
echo "========================================="

# Confirm deletion
echo "Are you sure you want to delete this file? (y/N)"
read -r CONFIRM

case "$CONFIRM" in
    [yY]|[yY][eE][sS])
        echo "Deleting file: $HDFS_PATH"
        hdfs dfs -rm "$HDFS_PATH"
        
        if [ $? -eq 0 ]; then
            echo "Successfully deleted file: $HDFS_PATH"
        else
            echo "Failed to delete file"
            exit 1
        fi
        ;;
    *)
        echo "Deletion cancelled"
        exit 0
        ;;
esac 