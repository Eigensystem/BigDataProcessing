#!/bin/zsh

# Move file in HDFS from source to destination
# Usage: ./move_hdfs_file.sh <source_path> <destination_path>

if [ $# -ne 2 ]; then
    echo "Usage: $0 <source_hdfs_path> <destination_hdfs_path>"
    echo ""
    echo "Examples:"
    echo "  $0 /user/data/file1.txt /user/backup/file1.txt"
    echo "  $0 /user/data/file1.txt /user/newdir/"
    exit 1
fi

SOURCE_PATH=$1
DEST_PATH=$2

# Check if source file exists
if ! hdfs dfs -test -e "$SOURCE_PATH"; then
    echo "Error: Source file '$SOURCE_PATH' does not exist in HDFS"
    exit 1
fi

echo "========================================="
echo "Source file information:"
echo "========================================="
hdfs dfs -ls -h "$SOURCE_PATH"
echo "========================================="

# Check if destination is a directory
if hdfs dfs -test -d "$DEST_PATH"; then
    echo "Destination is a directory: $DEST_PATH"
    SOURCE_FILENAME=$(basename "$SOURCE_PATH")
    FINAL_DEST_PATH="${DEST_PATH%/}/${SOURCE_FILENAME}"
    echo "Final destination will be: $FINAL_DEST_PATH"
else
    FINAL_DEST_PATH="$DEST_PATH"
    echo "Destination file: $FINAL_DEST_PATH"
fi

# Check if destination file already exists
if hdfs dfs -test -e "$FINAL_DEST_PATH"; then
    echo ""
    echo "Warning: Destination file '$FINAL_DEST_PATH' already exists!"
    echo "========================================="
    echo "Existing destination file:"
    echo "========================================="
    hdfs dfs -ls -h "$FINAL_DEST_PATH"
    echo "========================================="
    echo ""
    echo "Do you want to overwrite the existing file? (y/N)"
    read -r OVERWRITE
    
    case "$OVERWRITE" in
        [yY]|[yY][eE][sS])
            echo "Will overwrite existing file"
            ;;
        *)
            echo "Move operation cancelled"
            exit 0
            ;;
    esac
fi

# Ensure destination directory exists
DEST_DIR=$(dirname "$FINAL_DEST_PATH")
if [ "$DEST_DIR" != "." ] && [ "$DEST_DIR" != "/" ]; then
    echo "Ensuring destination directory exists: $DEST_DIR"
    hdfs dfs -mkdir -p "$DEST_DIR"
    
    if [ $? -ne 0 ]; then
        echo "Failed to create destination directory"
        exit 1
    fi
fi

# Perform the move operation
echo "Moving file from '$SOURCE_PATH' to '$FINAL_DEST_PATH'..."
hdfs dfs -mv "$SOURCE_PATH" "$FINAL_DEST_PATH"

if [ $? -eq 0 ]; then
    echo "Successfully moved file!"
    echo "========================================="
    echo "New file location:"
    echo "========================================="
    hdfs dfs -ls -h "$FINAL_DEST_PATH"
    echo "========================================="
else
    echo "Failed to move file"
    exit 1
fi 