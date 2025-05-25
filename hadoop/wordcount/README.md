# Hadoop MapReduce WordCount Example

This directory contains a complete implementation of the classic WordCount example using Hadoop MapReduce.

## Files Description

| File | Description |
|------|-------------|
| `WordCount.java` | Main MapReduce program with Mapper, Reducer, and Driver |
| `compile.sh` | Script to compile Java code and create JAR file |
| `run_wordcount.sh` | Script to run the complete WordCount job |
| `clean.sh` | Script to clean up generated files and HDFS directories |
| `input1.txt` | Sample input data file 1 |
| `input2.txt` | Sample input data file 2 |

## How to Run

### Step 1: Make scripts executable
```bash
chmod +x *.sh
```

### Step 2: Compile the program
```bash
./compile.sh
```

### Step 3: Run the WordCount job
```bash
./run_wordcount.sh
```

### Step 4: (Optional) Clean up
```bash
./clean.sh
```

## Program Architecture

### Mapper Class (`TokenizerMapper`)
- **Input**: Text lines from input files
- **Processing**: 
  - Converts text to lowercase
  - Removes punctuation
  - Tokenizes into individual words
- **Output**: `<word, 1>` pairs

### Reducer Class (`IntSumReducer`)
- **Input**: `<word, list_of_counts>` from mappers
- **Processing**: Sums up all counts for each word
- **Output**: `<word, total_count>` pairs

### Driver Class (`WordCount.main`)
- **Function**: Configures and submits the MapReduce job
- **Configuration**: Sets input/output paths, mapper/reducer classes

## Expected Output

The program will count the frequency of each word in the input files and produce output like:
```
algorithms	1
analytics	2
apache	2
big	2
data	5
hadoop	6
mapreduce	3
...
```

## HDFS Directories Used

- **Input**: `/user/input/` - Contains uploaded input files
- **Output**: `/user/output/` - Contains job results

## Prerequisites

- Hadoop cluster running
- HDFS accessible via `hdfs dfs` commands
- Java compiler (`javac`) available
- Hadoop JAR command available

## Troubleshooting

### Common Issues:

1. **Compilation Fails**
   - Check if Hadoop classpath is accessible
   - Verify Java compiler is installed

2. **Job Submission Fails**
   - Ensure Hadoop services are running
   - Check HDFS is accessible

3. **Permission Denied**
   - Verify HDFS write permissions
   - Check if directories already exist

### Verification Commands:
```bash
# Check Hadoop services
jps

# Check HDFS connectivity
hdfs dfs -ls /

# Check job history
mapred job -list
```

## Performance Notes

- Uses `IntSumReducer` as both combiner and reducer for efficiency
- Handles large datasets by distributed processing
- Automatically handles data locality optimization
- Fault-tolerant through Hadoop's built-in mechanisms 