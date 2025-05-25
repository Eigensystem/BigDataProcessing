#!/bin/zsh

# Delete HDFS directory
# Usage: ./delete_hdfs_dir.sh <hdfs_directory_path> [force]
# force: use 'force' to delete non-empty directories

if [ $# -lt 1 ]; then
    echo "Usage: $0 <hdfs_directory_path> [force]"
    echo ""
    echo "Options:"
    echo "  force - Delete directory even if it contains files"
    echo ""
    echo "Examples:"
    echo "  $0 /user/data/emptydir"
    echo "  $0 /user/data/nonemptydir force"
    exit 1
fi

HDFS_PATH=$1
FORCE_DELETE=${2:-""}

# Check if directory exists in HDFS
if ! hdfs dfs -test -e "$HDFS_PATH"; then
    echo "Error: Directory '$HDFS_PATH' does not exist in HDFS"
    exit 1
fi

# Check if it's a directory (not file)
if ! hdfs dfs -test -d "$HDFS_PATH"; then
    echo "Error: '$HDFS_PATH' is not a directory"
    echo "Use delete_hdfs_file.sh or manage_hdfs_file.sh to delete files"
    exit 1
fi

echo "========================================="
echo "Directory to be deleted:"
echo "========================================="
hdfs dfs -ls -h -d "$HDFS_PATH"
echo ""
echo "Directory contents:"
hdfs dfs -ls "$HDFS_PATH" 2>/dev/null || echo "Directory is empty"
echo "========================================="

# Check if directory is empty
ITEM_COUNT=$(hdfs dfs -ls "$HDFS_PATH" 2>/dev/null | grep -v "^Found" | wc -l)

if [ $ITEM_COUNT -gt 0 ]; then
    echo "Warning: Directory contains $ITEM_COUNT items"
    
    if [ "$FORCE_DELETE" = "force" ]; then
        echo "Force delete mode enabled"
    else
        echo "Directory is not empty. Use 'force' parameter to delete non-empty directory"
        echo "Example: $0 $HDFS_PATH force"
        exit 1
    fi
else
    echo "Directory is empty"
fi

# Confirm deletion
echo ""
if [ $ITEM_COUNT -gt 0 ] && [ "$FORCE_DELETE" = "force" ]; then
    echo "Are you sure you want to delete this directory and ALL its contents? (y/N)"
else
    echo "Are you sure you want to delete this directory? (y/N)"
fi

read -r CONFIRM

case "$CONFIRM" in
    [yY]|[yY][eE][sS])
        if [ $ITEM_COUNT -gt 0 ] && [ "$FORCE_DELETE" = "force" ]; then
            echo "Force deleting directory and all contents: $HDFS_PATH"
            hdfs dfs -rm -r "$HDFS_PATH"
        else
            echo "Deleting empty directory: $HDFS_PATH"
            hdfs dfs -rmdir "$HDFS_PATH"
        fi
        
        if [ $? -eq 0 ]; then
            echo "Successfully deleted directory: $HDFS_PATH"
        else
            echo "Failed to delete directory"
            exit 1
        fi
        ;;
    *)
        echo "Deletion cancelled"
        exit 0
        ;;
esac 