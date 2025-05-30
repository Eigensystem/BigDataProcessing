# Spark Structured Streaming WordCount

This is a complete implementation of Spark Structured Streaming WordCount that demonstrates various capabilities of real-time stream data processing.

## Features

- **Multiple Input Sources**: File streams, Socket streams
- **Real-time Processing**: Continuous monitoring and processing of newly arrived data
- **Window Operations**: Support for time-windowed sliding aggregations
- **Fault Tolerance**: Built-in checkpointing and failure recovery mechanisms
- **Flexible Configuration**: Support for various triggers and output modes
- **Data Generator**: Built-in data generator for testing

## File Structure

```
spark/structured_streaming/
├── streaming_wordcount.py    # Main program: Structured Streaming WordCount
├── data_generator.py         # Data generator: Simulates streaming data
├── run_streaming.sh          # Run script: Simplifies program execution
├── requirements.txt          # Python dependencies
├── input/                    # Input directory
│   ├── sample1.txt
│   └── sample2.txt
└── README.md                # This document
```

## Prerequisites

1. **Python 3.6+**
2. **Java 8** (compatible with PySpark 3.5.x)
3. **PySpark** (will be installed automatically)

## Installation

1. Install dependencies:
```bash
pip3 install -r requirements.txt
```

2. Set script execution permissions:
```bash
chmod +x run_streaming.sh
chmod +x data_generator.py
chmod +x streaming_wordcount.py
```

## Usage

### Method 1: Using Run Script (Recommended)

```bash
# 1. File stream mode (default)
./run_streaming.sh

# 2. File stream mode with custom parameters
./run_streaming.sh -m file -i input/ -o output/ -t 5

# 3. Window mode
./run_streaming.sh -m window -w "2 minutes" -s "1 minute"

# 4. Socket mode
./run_streaming.sh -m socket --host localhost --port 9999

# View help
./run_streaming.sh -h
```

### Method 2: Direct Python Program Execution

```bash
# File stream mode
python3 streaming_wordcount.py file input/ output/ 10

# Window mode
python3 streaming_wordcount.py window input/ output/ "1 minute" "30 seconds"

# Socket mode
python3 streaming_wordcount.py socket localhost 9999
```

## Running Modes Explained

### 1. File Stream Mode

Monitors a specified directory and processes newly added text files.

**Features:**
- Automatically monitors input directory
- Processes newly arrived files
- Supports batch processing of multiple files
- Suitable for log file processing scenarios

**Usage Example:**
```bash
./run_streaming.sh -m file -i input/ -o output/ -t 5
```

**How to Test:**
1. After program startup, the data generator will start automatically
2. Data generator creates new files in `input/` directory every 8 seconds
3. Observe real-time word statistics in console output

### 2. Window Mode

Stream processing based on time windows, supporting sliding window aggregations.

**Features:**
- Time window aggregations
- Sliding window support
- Event-time processing
- Suitable for time series analysis

**Usage Example:**
```bash
./run_streaming.sh -m window -w "2 minutes" -s "1 minute"
```

**Parameter Explanation:**
- `-w "2 minutes"`: Window size is 2 minutes
- `-s "1 minute"`: Slides every 1 minute

### 3. Socket Mode

Receives real-time text data from Socket connections.

**Features:**
- Real-time interactive input
- Low-latency processing
- Suitable for real-time chat, log streams, etc.

**Usage Example:**
```bash
# Terminal 1: Start WordCount program
./run_streaming.sh -m socket

# Terminal 2: Start netcat to send data
nc -lk 9999
# Then input text and press Enter to send
```

## Data Generator Usage

The data generator is used to simulate real-time data streams:

```bash
# Continuously generate data (one file every 5 seconds)
python3 data_generator.py continuous input/ 5 60 3

# Batch generate 10 files
python3 data_generator.py batch input/ 10 5

# Generate single file
python3 data_generator.py single input/ 3
```

## Output Format

The program displays real-time word statistics in the console:

```
+----------+-----+
|word      |count|
+----------+-----+
|streaming |15   |
|spark     |12   |
|data      |10   |
|processing|8    |
|apache    |6    |
+----------+-----+
```

For file stream mode, results are also saved to the specified output directory (Parquet format).

## Configuration Parameters

### Trigger Settings
- **Processing Time**: `trigger(processingTime='10 seconds')`
- **Once**: `trigger(once=True)`
- **Continuous**: `trigger(continuous='1 second')`

### Output Modes
- **Complete**: Output complete result table
- **Append**: Only output new rows
- **Update**: Output updated rows

### Checkpointing
The program automatically creates checkpoint directories for fault recovery:
- Default location: `./checkpoint`
- File output checkpoint: `{output_path}_checkpoint`

## Troubleshooting

### Common Issues

1. **Java Version Issues**
```bash
# Check Java version (requires Java 8)
java -version

# Install Java 8 if needed
sudo apt install openjdk-8-jdk
```

2. **Port Occupied (Socket Mode)**
```bash
# Check port usage
netstat -tulpn | grep 9999

# Kill process occupying the port
sudo kill -9 <PID>
```

3. **Permission Issues**
```bash
# Set script execution permissions
chmod +x run_streaming.sh
chmod +x streaming_wordcount.py
chmod +x data_generator.py
```

4. **Dependency Installation Issues**
```bash
# Manually install PySpark
pip3 install 'pyspark>=3.5.0,<4.0.0'
```

### Debugging Tips

1. **Increase Log Level**
Modify log level in the program:
```python
spark.sparkContext.setLogLevel("INFO")  # or "DEBUG"
```

2. **Check Checkpoints**
Checkpoint directories contain stream processing state information for debugging.

3. **Monitor Resource Usage**
```bash
# Monitor CPU and memory usage
htop

# Monitor disk space
df -h
```

## Performance Optimization

1. **Adjust Trigger Interval**
   - Smaller interval: Lower latency, higher resource consumption
   - Larger interval: Higher throughput, higher latency

2. **Configure Spark Parameters**
```python
spark = SparkSession.builder \
    .config("spark.sql.streaming.metricsEnabled", "true") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()
```

3. **Optimize Checkpointing**
   - Use fast storage (SSD)
   - Regularly clean old checkpoints

## Extended Features

The program supports the following extensions:

1. **Add More Data Sources**
   - Kafka
   - Kinesis  
   - HDFS

2. **Add More Output Formats**
   - JSON
   - CSV
   - Database

3. **Add Complex Processing Logic**
   - Data cleaning
   - Sentiment analysis
   - Anomaly detection

## License

This is an educational example program for learning Spark Structured Streaming concepts. 