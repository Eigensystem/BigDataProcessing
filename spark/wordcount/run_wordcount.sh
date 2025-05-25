#!/bin/bash

# PySpark WordCount Runner Script
# This script helps run the wordcount program with different configurations

echo "PySpark WordCount Runner"
echo "======================="

# Check if Python is available
if ! command -v python3 &> /dev/null; then
    echo "Error: Python3 is not installed or not in PATH"
    exit 1
fi

# Check if PySpark is installed
if ! python3 -c "import pyspark" &> /dev/null; then
    echo "PySpark is not installed. Installing from requirements.txt..."
    pip3 install -r requirements.txt
fi

# Default values
INPUT_PATH="input/"
OUTPUT_PATH="output"
METHOD="dataframe"

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -i|--input)
            INPUT_PATH="$2"
            shift 2
            ;;
        -o|--output)
            OUTPUT_PATH="$2"
            shift 2
            ;;
        -m|--method)
            METHOD="$2"
            shift 2
            ;;
        -h|--help)
            echo "Usage: $0 [OPTIONS]"
            echo "Options:"
            echo "  -i, --input PATH    Input path (default: input/)"
            echo "  -o, --output PATH   Output path (default: output)"
            echo "  -m, --method TYPE   Method: dataframe or rdd (default: dataframe)"
            echo "  -h, --help          Show this help message"
            echo ""
            echo "Examples:"
            echo "  $0                                    # Use defaults"
            echo "  $0 -i input/ -o results -m dataframe # Custom settings"
            echo "  $0 -i input/*.txt -m rdd             # Use RDD method"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use -h or --help for usage information"
            exit 1
            ;;
    esac
done

# Clean up previous output
if [ -d "$OUTPUT_PATH" ]; then
    echo "Cleaning up previous output directory: $OUTPUT_PATH"
    rm -rf "$OUTPUT_PATH"
fi

if [ -d "${OUTPUT_PATH}_text" ]; then
    echo "Cleaning up previous text output directory: ${OUTPUT_PATH}_text"
    rm -rf "${OUTPUT_PATH}_text"
fi

# Run the wordcount program
echo "Running WordCount with:"
echo "  Input: $INPUT_PATH"
echo "  Output: $OUTPUT_PATH"
echo "  Method: $METHOD"
echo ""

python3 wordcount_pyspark.py "$INPUT_PATH" "$OUTPUT_PATH" "$METHOD"

# Check if the program ran successfully
if [ $? -eq 0 ]; then
    echo ""
    echo "WordCount completed successfully!"
    echo "Results are saved in: $OUTPUT_PATH"
    
    # Show output files
    if [ -d "$OUTPUT_PATH" ]; then
        echo ""
        echo "Output files:"
        ls -la "$OUTPUT_PATH"
    fi
    
    # Show a sample of the results if CSV file exists
    CSV_FILE=$(find "$OUTPUT_PATH" -name "*.csv" | head -1)
    if [ -n "$CSV_FILE" ]; then
        echo ""
        echo "Sample results (first 10 lines):"
        head -10 "$CSV_FILE"
    fi
else
    echo "WordCount failed with exit code: $?"
    exit 1
fi 