#!/bin/zsh

# Clean up WordCount project files and HDFS directories
echo "========================================="
echo "Cleaning WordCount Project"
echo "========================================="

# Clean local build files
echo "Cleaning local build files..."
rm -rf build
rm -f wordcount.jar
rm -f wordcount_results.txt

# Clean HDFS directories
echo "Cleaning HDFS directories..."
hdfs dfs -rm -r /user/input 2>/dev/null || true
hdfs dfs -rm -r /user/output 2>/dev/null || true

echo "========================================="
echo "Cleanup completed!"
echo "========================================="

# Show remaining files
echo "Remaining files in current directory:"
ls -la 