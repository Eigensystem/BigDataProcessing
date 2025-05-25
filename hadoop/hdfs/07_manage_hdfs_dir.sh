#!/bin/zsh

# Create or delete HDFS directory
# Usage: ./manage_hdfs_dir.sh <operation> <hdfs_directory_path> [force]
# operation: create or delete
# force: for delete operation, use 'force' to delete non-empty directories

if [ $# -lt 2 ]; then
    echo "Usage: $0 <operation> <hdfs_directory_path> [force]"
    echo "Operations:"
    echo "  create - Create a new directory (with parent directories if needed)"
    echo "  delete - Delete a directory"
    echo ""
    echo "For delete operation:"
    echo "  Add 'force' as third parameter to delete non-empty directories"
    echo ""
    echo "Examples:"
    echo "  $0 create /user/data/newdir"
    echo "  $0 delete /user/data/newdir"
    echo "  $0 delete /user/data/newdir force"
    exit 1
fi

OPERATION=$1
HDFS_PATH=$2
FORCE_DELETE=${3:-""}

case "$OPERATION" in
    "create")
        echo "Creating HDFS directory: $HDFS_PATH"
        
        # Check if directory already exists
        if hdfs dfs -test -e "$HDFS_PATH"; then
            if hdfs dfs -test -d "$HDFS_PATH"; then
                echo "Directory '$HDFS_PATH' already exists"
                exit 0
            else
                echo "Error: '$HDFS_PATH' exists but is not a directory"
                exit 1
            fi
        fi
        
        # Create the directory with parent directories
        hdfs dfs -mkdir -p "$HDFS_PATH"
        
        if [ $? -eq 0 ]; then
            echo "Successfully created directory: $HDFS_PATH"
            # Show directory info
            hdfs dfs -ls -h -d "$HDFS_PATH"
        else
            echo "Failed to create directory"
            exit 1
        fi
        ;;
        
    "delete")
        echo "Deleting HDFS directory: $HDFS_PATH"
        
        # Check if directory exists
        if ! hdfs dfs -test -e "$HDFS_PATH"; then
            echo "Error: Directory '$HDFS_PATH' does not exist"
            exit 1
        fi
        
        # Check if it's a directory (not file)
        if ! hdfs dfs -test -d "$HDFS_PATH"; then
            echo "Error: '$HDFS_PATH' is not a directory"
            exit 1
        fi
        
        # Check if directory is empty
        ITEM_COUNT=$(hdfs dfs -ls "$HDFS_PATH" 2>/dev/null | grep -v "^Found" | wc -l)
        
        if [ $ITEM_COUNT -gt 0 ]; then
            echo "Directory '$HDFS_PATH' is not empty (contains $ITEM_COUNT items)"
            
            if [ "$FORCE_DELETE" = "force" ]; then
                echo "Force delete enabled. Deleting directory and all contents..."
                hdfs dfs -rm -r "$HDFS_PATH"
            else
                echo "Use 'force' parameter to delete non-empty directory"
                echo "Example: $0 delete $HDFS_PATH force"
                exit 1
            fi
        else
            echo "Directory is empty. Deleting..."
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
        echo "Error: Invalid operation '$OPERATION'"
        echo "Valid operations: create, delete"
        exit 1
        ;;
esac 