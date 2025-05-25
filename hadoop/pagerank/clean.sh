#!/bin/zsh

# Clean up PageRank project files and HDFS directories
echo "========================================="
echo "Cleaning PageRank Project"
echo "========================================="

# Clean local build files
echo "Cleaning local build files..."
rm -rf build
rm -f pagerank.jar
rm -f preprocessor.jar
rm -f pagerank_results.txt

# Clean downloaded data (optional)
echo "Do you want to remove downloaded data? (y/N)"
read -r REMOVE_DATA

if [ "$REMOVE_DATA" = "y" ] || [ "$REMOVE_DATA" = "Y" ]; then
    echo "Removing downloaded data..."
    rm -rf data
else
    echo "Keeping downloaded data..."
fi

# Clean HDFS directories
echo "Cleaning HDFS directories..."
hdfs dfs -rm -r /user/pagerank 2>/dev/null || true

echo "========================================="
echo "Cleanup completed!"
echo "========================================="

# Show remaining files
echo "Remaining files in current directory:"
ls -la 