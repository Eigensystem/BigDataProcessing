#!/bin/zsh

# Create or delete HDFS file
# Usage: ./manage_hdfs_file.sh <operation> <hdfs_file_path> [content]
# operation: create or delete
# For create: optionally provide content as third parameter

if [ $# -lt 2 ]; then
    echo "Usage: $0 <operation> <hdfs_file_path> [content_for_create]"
    echo "Operations:"
    echo "  create - Create a new file (optionally with content)"
    echo "  delete - Delete an existing file"
    echo ""
    echo "Examples:"
    echo "  $0 create /user/data/newfile.txt"
    echo "  $0 create /user/data/newfile.txt 'Hello World'"
    echo "  $0 delete /user/data/newfile.txt"
    exit 1
fi

OPERATION=$1
HDFS_PATH=$2
CONTENT=${3:-""}

case "$OPERATION" in
    "create")
        echo "Creating HDFS file: $HDFS_PATH"
        
        # Check if file already exists
        if hdfs dfs -test -e "$HDFS_PATH"; then
            echo "Error: File '$HDFS_PATH' already exists"
            exit 1
        fi
        
        # Create parent directories if they don't exist
        HDFS_DIR=$(dirname "$HDFS_PATH")
        echo "Ensuring parent directory exists: $HDFS_DIR"
        hdfs dfs -mkdir -p "$HDFS_DIR"
        
        if [ $? -ne 0 ]; then
            echo "Failed to create parent directory"
            exit 1
        fi
        
        # Create the file
        if [ -n "$CONTENT" ]; then
            echo "Creating file with provided content..."
            echo "$CONTENT" | hdfs dfs -put - "$HDFS_PATH"
        else
            echo "Creating empty file..."
            hdfs dfs -touchz "$HDFS_PATH"
        fi
        
        if [ $? -eq 0 ]; then
            echo "Successfully created file: $HDFS_PATH"
            # Show file info
            hdfs dfs -ls -h "$HDFS_PATH"
        else
            echo "Failed to create file"
            exit 1
        fi
        ;;
        
    "delete")
        echo "Deleting HDFS file: $HDFS_PATH"
        
        # Check if file exists
        if ! hdfs dfs -test -e "$HDFS_PATH"; then
            echo "Error: File '$HDFS_PATH' does not exist"
            exit 1
        fi
        
        # Check if it's a file (not directory)
        if hdfs dfs -test -d "$HDFS_PATH"; then
            echo "Error: '$HDFS_PATH' is a directory, not a file. Use manage_hdfs_dir.sh for directories"
            exit 1
        fi
        
        # Delete the file
        hdfs dfs -rm "$HDFS_PATH"
        
        if [ $? -eq 0 ]; then
            echo "Successfully deleted file: $HDFS_PATH"
        else
            echo "Failed to delete file"
            exit 1
        fi
        ;;
        
    *)
        echo "Error: Invalid operation '$OPERATION'"
        echo "Valid operations: create, delete"
        exit 1
        ;;
esac 