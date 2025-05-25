#!/bin/zsh

# Append content to HDFS file
# Usage: ./append_to_hdfs_file.sh <hdfs_file_path> <content> [position]
# position: beginning or end (default: end)

if [ $# -lt 2 ]; then
    echo "Usage: $0 <hdfs_file_path> <content> [position]"
    echo "Position options:"
    echo "  end       - Append to end of file (default)"
    echo "  beginning - Prepend to beginning of file"
    echo ""
    echo "Examples:"
    echo "  $0 /user/data/test.txt 'New line content'"
    echo "  $0 /user/data/test.txt 'Header content' beginning"
    echo "  $0 /user/data/test.txt 'Footer content' end"
    exit 1
fi

HDFS_PATH=$1
CONTENT=$2
POSITION=${3:-"end"}

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

case "$POSITION" in
    "end")
        echo "Appending content to end of file: $HDFS_PATH"
        echo "$CONTENT" | hdfs dfs -appendToFile - "$HDFS_PATH"
        
        if [ $? -eq 0 ]; then
            echo "Successfully appended content to end of file"
        else
            echo "Failed to append content to file"
            exit 1
        fi
        ;;
        
    "beginning")
        echo "Prepending content to beginning of file: $HDFS_PATH"
        
        # Create temporary file paths
        TEMP_HDFS_PATH="${HDFS_PATH}.tmp.$(date +%s)"
        TEMP_LOCAL_FILE="/tmp/hdfs_prepend_$(date +%s).txt"
        
        # Download original file to local temporary file
        echo "Downloading original file..."
        hdfs dfs -get "$HDFS_PATH" "$TEMP_LOCAL_FILE"
        
        if [ $? -ne 0 ]; then
            echo "Failed to download original file"
            exit 1
        fi
        
        # Create new content with original content appended
        echo "Creating new file with prepended content..."
        {
            echo "$CONTENT"
            cat "$TEMP_LOCAL_FILE"
        } > "${TEMP_LOCAL_FILE}.new"
        
        # Upload the new file to temporary HDFS location
        hdfs dfs -put "${TEMP_LOCAL_FILE}.new" "$TEMP_HDFS_PATH"
        
        if [ $? -eq 0 ]; then
            # Replace original file with new file
            echo "Replacing original file..."
            hdfs dfs -rm "$HDFS_PATH"
            hdfs dfs -mv "$TEMP_HDFS_PATH" "$HDFS_PATH"
            
            if [ $? -eq 0 ]; then
                echo "Successfully prepended content to beginning of file"
            else
                echo "Failed to replace original file"
                # Cleanup temporary file
                hdfs dfs -rm "$TEMP_HDFS_PATH" 2>/dev/null
                exit 1
            fi
        else
            echo "Failed to upload new file"
            exit 1
        fi
        
        # Cleanup local temporary files
        rm -f "$TEMP_LOCAL_FILE" "${TEMP_LOCAL_FILE}.new"
        ;;
        
    *)
        echo "Error: Invalid position '$POSITION'"
        echo "Valid positions: beginning, end"
        exit 1
        ;;
esac

echo "========================================="
echo "Updated file content:"
echo "========================================="
hdfs dfs -cat "$HDFS_PATH"
echo "=========================================" 