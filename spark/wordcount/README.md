# PySpark WordCount Program

This is a complete PySpark WordCount implementation that demonstrates distributed text processing using Apache Spark.

## Features

- **Two Implementation Methods**: DataFrame API and RDD API
- **Text Preprocessing**: Automatic lowercase conversion and punctuation removal
- **Multiple Output Formats**: CSV and text formats
- **Flexible Input**: Supports single files, directories, and wildcard patterns
- **Statistics**: Shows total words, unique words, and top frequent words

## Files Structure

```
spark/
├── wordcount_pyspark.py    # Main PySpark program
├── run_wordcount.sh        # Shell script runner
├── requirements.txt        # Python dependencies
├── input/                  # Sample input files
│   ├── sample1.txt
│   └── sample2.txt
└── README.md              # This file
```

## Prerequisites

1. **Python 3.6+**
2. **Java 8 or 11** (required by Spark)
3. **PySpark** (will be installed automatically)

## Installation

1. Install dependencies:
```bash
pip3 install -r requirements.txt
```

2. Make the shell script executable:
```bash
chmod +x run_wordcount.sh
```

## Usage

### Method 1: Using the Shell Script (Recommended)

```bash
# Basic usage with defaults
./run_wordcount.sh

# Custom input and output paths
./run_wordcount.sh -i input/ -o results

# Use RDD method instead of DataFrame
./run_wordcount.sh -m rdd

# Show help
./run_wordcount.sh -h
```

### Method 2: Direct Python Execution

```bash
# Basic usage
python3 wordcount_pyspark.py input/

# With custom output path
python3 wordcount_pyspark.py input/ output

# Using RDD method
python3 wordcount_pyspark.py input/ output rdd

# Process specific files
python3 wordcount_pyspark.py input/*.txt results dataframe
```

## Examples

### Example 1: Process all files in input directory
```bash
./run_wordcount.sh -i input/ -o wordcount_results
```

### Example 2: Process specific text files using RDD
```bash
./run_wordcount.sh -i "input/*.txt" -o rdd_results -m rdd
```

### Example 3: Direct Python execution
```bash
python3 wordcount_pyspark.py input/ my_output dataframe
```

## Output

The program generates two types of output:

1. **CSV Format** (`output/`): Structured data with headers
   - `word,count`
   - Suitable for further analysis

2. **Text Format** (`output_text/`): Plain text format
   - Each line: `[word,count]`
   - Human-readable format

## Sample Output

```
Top 20 most frequent words:
+----------+-----+
|word      |count|
+----------+-----+
|data      |8    |
|and       |7    |
|spark     |6    |
|for       |5    |
|processing|4    |
|apache    |4    |
|...       |...  |
+----------+-----+

WordCount completed successfully!
Results saved to: output
Total words: 156
Unique words: 89
```

## Configuration

The program includes several Spark optimizations:
- Adaptive Query Execution enabled
- Partition coalescing for better performance
- Configurable Spark session settings

## Troubleshooting

### Common Issues

1. **Java not found**: Install Java 8 or 11
   ```bash
   # Ubuntu/Debian
   sudo apt install openjdk-11-jdk
   
   # CentOS/RHEL
   sudo yum install java-11-openjdk-devel
   ```

2. **PySpark import error**: Install PySpark
   ```bash
   pip3 install pyspark
   ```

3. **Permission denied**: Make script executable
   ```bash
   chmod +x run_wordcount.sh
   ```

4. **Input file not found**: Check file paths and permissions

### Environment Variables

If you have Spark installed separately, you may need to set:
```bash
export SPARK_HOME=/path/to/spark
export PYTHONPATH=$SPARK_HOME/python:$PYTHONPATH
```

## Performance Tips

1. **For large files**: Use the DataFrame API (default)
2. **For small files**: RDD API might be faster
3. **Multiple files**: Use directory input rather than wildcards
4. **Memory issues**: Adjust Spark configuration in the code

## License

This is a sample educational program for learning PySpark and distributed computing concepts. 