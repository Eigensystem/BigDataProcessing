#!/bin/zsh

# Upload file to HDFS
# Usage: ./upload_to_hdfs.sh <local_file> <hdfs_path> [mode]
# mode: overwrite or append, default is overwrite

if [ $# -lt 2 ]; then
    echo "Usage: $0 <local_file_path> <hdfs_target_path> [mode:overwrite|append]"
    echo "Example: $0 /tmp/test.txt /user/data/test.txt overwrite"
    exit 1
fi

LOCAL_FILE=$1
HDFS_PATH=$2
MODE=${3:-"overwrite"}  # Default to overwrite mode

# Check if local file exists
if [ ! -f "$LOCAL_FILE" ]; then
    echo "Error: Local file '$LOCAL_FILE' does not exist"
    exit 1
fi

# Check if file exists in HDFS
if hdfs dfs -test -e "$HDFS_PATH"; then
    echo "File '$HDFS_PATH' already exists in HDFS"
    
    if [ "$MODE" = "append" ]; then
        echo "Append mode: Appending content to existing file"
        hdfs dfs -appendToFile "$LOCAL_FILE" "$HDFS_PATH"
        if [ $? -eq 0 ]; then
            echo "Successfully appended file '$LOCAL_FILE' to '$HDFS_PATH'"
        else
            echo "Failed to append file"
            exit 1
        fi
    elif [ "$MODE" = "overwrite" ]; then
        echo "Overwrite mode: Overwriting existing file"
        hdfs dfs -put -f "$LOCAL_FILE" "$HDFS_PATH"
        if [ $? -eq 0 ]; then
            echo "Successfully overwritten file '$LOCAL_FILE' to '$HDFS_PATH'"
        else
            echo "Failed to overwrite file"
            exit 1
        fi
    else
        echo "Error: Unknown mode '$MODE', please use 'overwrite' or 'append'"
        exit 1
    fi
else
    echo "File does not exist in HDFS, uploading..."
    # Ensure target directory exists
    HDFS_DIR=$(dirname "$HDFS_PATH")
    hdfs dfs -mkdir -p "$HDFS_DIR"
    
    hdfs dfs -put "$LOCAL_FILE" "$HDFS_PATH"
    if [ $? -eq 0 ]; then
        echo "Successfully uploaded file '$LOCAL_FILE' to '$HDFS_PATH'"
    else
        echo "Failed to upload file"
        exit 1
    fi
fi 