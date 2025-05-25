#!/bin/zsh

# Download file from HDFS
# Usage: ./download_from_hdfs.sh <hdfs_path> <local_target_path>
# If local file exists, it will be automatically renamed

if [ $# -ne 2 ]; then
    echo "Usage: $0 <hdfs_source_path> <local_target_path>"
    echo "Example: $0 /user/data/test.txt /tmp/test.txt"
    exit 1
fi

HDFS_PATH=$1
LOCAL_PATH=$2

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

# Ensure local target directory exists
LOCAL_DIR=$(dirname "$LOCAL_PATH")
mkdir -p "$LOCAL_DIR"

# If local file exists, rename it
if [ -f "$LOCAL_PATH" ]; then
    echo "Local file '$LOCAL_PATH' already exists"
    
    # Generate a new name with timestamp
    TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
    BASENAME=$(basename "$LOCAL_PATH")
    DIRNAME=$(dirname "$LOCAL_PATH")
    EXTENSION="${BASENAME##*.}"
    FILENAME="${BASENAME%.*}"
    
    if [ "$EXTENSION" = "$BASENAME" ]; then
        # No extension
        NEW_LOCAL_PATH="${DIRNAME}/${FILENAME}_${TIMESTAMP}"
    else
        # Has extension
        NEW_LOCAL_PATH="${DIRNAME}/${FILENAME}_${TIMESTAMP}.${EXTENSION}"
    fi
    
    echo "Renaming download to: '$NEW_LOCAL_PATH'"
    LOCAL_PATH="$NEW_LOCAL_PATH"
fi

# Download the file
echo "Downloading '$HDFS_PATH' to '$LOCAL_PATH'..."
hdfs dfs -get "$HDFS_PATH" "$LOCAL_PATH"

if [ $? -eq 0 ]; then
    echo "Successfully downloaded file to '$LOCAL_PATH'"
    # Show file info
    ls -lh "$LOCAL_PATH"
else
    echo "Failed to download file"
    exit 1
fi 