#!/usr/bin/env python3
"""
Spark Structured Streaming WordCount Program
Real-time word counting using Spark Structured Streaming
"""

import sys
import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, lower, regexp_replace, col, window, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

def create_spark_session(app_name="StructuredStreamingWordCount"):
    """Create and return a Spark session with streaming configurations"""
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.streaming.checkpointLocation", "./checkpoint") \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
        .getOrCreate()

def create_file_stream_wordcount(spark, input_path, output_path, trigger_interval=10):
    """
    Create a file-based streaming word count
    
    Args:
        spark: SparkSession
        input_path: Path to monitor for new files
        output_path: Path to write output
        trigger_interval: Trigger interval in seconds
    """
    print(f"Monitoring directory: {input_path}")
    print(f"Output path: {output_path}")
    print(f"Trigger interval: {trigger_interval} seconds")
    
    # Read streaming data from files
    lines_df = spark.readStream \
        .format("text") \
        .option("path", input_path) \
        .option("maxFilesPerTrigger", 1) \
        .load()
    
    # Add timestamp for windowing
    lines_with_timestamp = lines_df.select(
        col("value"),
        current_timestamp().alias("timestamp")
    )
    
    # Process text: clean and split into words
    cleaned_df = lines_with_timestamp.select(
        regexp_replace(lower(col("value")), "[^a-zA-Z0-9\\s]", "").alias("cleaned_text"),
        col("timestamp")
    )
    
    words_df = cleaned_df.select(
        explode(split(col("cleaned_text"), "\\s+")).alias("word"),
        col("timestamp")
    )
    
    # Filter empty words and count
    word_counts = words_df \
        .filter(col("word") != "") \
        .groupBy("word") \
        .count() \
        .orderBy(col("count").desc())
    
    # Write to console only (file output has limitations with complete mode)
    console_query = word_counts.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", False) \
        .option("numRows", 20) \
        .trigger(processingTime=f'{trigger_interval} seconds') \
        .start()
    
    return console_query

def create_windowed_wordcount(spark, input_path, output_path, window_duration="1 minute", slide_duration="30 seconds"):
    """
    Create a windowed streaming word count
    
    Args:
        spark: SparkSession
        input_path: Path to monitor for new files
        output_path: Path to write output
        window_duration: Window duration (e.g., "1 minute", "30 seconds")
        slide_duration: Slide duration (e.g., "30 seconds", "10 seconds")
    """
    print(f"Monitoring directory: {input_path}")
    print(f"Output path: {output_path}")
    print(f"Window size: {window_duration}, Slide interval: {slide_duration}")
    
    # Read streaming data
    lines_df = spark.readStream \
        .format("text") \
        .option("path", input_path) \
        .option("maxFilesPerTrigger", 1) \
        .load()
    
    # Add timestamp
    lines_with_timestamp = lines_df.select(
        col("value"),
        current_timestamp().alias("timestamp")
    )
    
    # Process text
    cleaned_df = lines_with_timestamp.select(
        regexp_replace(lower(col("value")), "[^a-zA-Z0-9\\s]", "").alias("cleaned_text"),
        col("timestamp")
    )
    
    words_df = cleaned_df.select(
        explode(split(col("cleaned_text"), "\\s+")).alias("word"),
        col("timestamp")
    )
    
    # Windowed word count
    windowed_counts = words_df \
        .filter(col("word") != "") \
        .groupBy(
            window(col("timestamp"), window_duration, slide_duration),
            col("word")
        ) \
        .count() \
        .orderBy(col("window"), col("count").desc())
    
    # Write to console
    query = windowed_counts.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", False) \
        .option("numRows", 30) \
        .trigger(processingTime='30 seconds') \
        .start()
    
    return query

def create_socket_wordcount(spark, host="localhost", port=9999):
    """
    Create a socket-based streaming word count
    
    Args:
        spark: SparkSession
        host: Socket host
        port: Socket port
    """
    print(f"Connecting to socket: {host}:{port}")
    print("Please run in another terminal: nc -lk 9999")
    print("Then input text for real-time word counting")
    
    # Read from socket
    lines_df = spark.readStream \
        .format("socket") \
        .option("host", host) \
        .option("port", port) \
        .load()
    
    # Process words
    words_df = lines_df.select(
        explode(split(lower(col("value")), "\\s+")).alias("word")
    )
    
    # Count words
    word_counts = words_df \
        .filter(col("word") != "") \
        .groupBy("word") \
        .count() \
        .orderBy(col("count").desc())
    
    # Write to console
    query = word_counts.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", False) \
        .trigger(processingTime='5 seconds') \
        .start()
    
    return query

def main():
    """Main function"""
    if len(sys.argv) < 2:
        print("Usage: python streaming_wordcount.py <mode> [options]")
        print()
        print("Modes:")
        print("  file <input_path> [output_path] [trigger_interval]  - File stream mode")
        print("  window <input_path> [output_path] [window_duration] [slide_duration]  - Window mode")
        print("  socket [host] [port]  - Socket stream mode")
        print()
        print("Examples:")
        print("  python streaming_wordcount.py file input/ output/")
        print("  python streaming_wordcount.py window input/ output/ '2 minutes' '1 minute'")
        print("  python streaming_wordcount.py socket localhost 9999")
        sys.exit(1)
    
    mode = sys.argv[1].lower()
    
    # Create Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        if mode == "file":
            input_path = sys.argv[2] if len(sys.argv) > 2 else "input/"
            output_path = sys.argv[3] if len(sys.argv) > 3 else "output/"
            trigger_interval = int(sys.argv[4]) if len(sys.argv) > 4 else 10
            
            # Create input directory if it doesn't exist
            os.makedirs(input_path, exist_ok=True)
            
            print("Starting File Stream WordCount...")
            print("=" * 60)
            console_query = create_file_stream_wordcount(
                spark, input_path, output_path, trigger_interval
            )
            
            # Wait for termination
            try:
                console_query.awaitTermination()
            except KeyboardInterrupt:
                print("\nStopping stream processing...")
                console_query.stop()
        
        elif mode == "window":
            input_path = sys.argv[2] if len(sys.argv) > 2 else "input/"
            output_path = sys.argv[3] if len(sys.argv) > 3 else "output/"
            window_duration = sys.argv[4] if len(sys.argv) > 4 else "1 minute"
            slide_duration = sys.argv[5] if len(sys.argv) > 5 else "30 seconds"
            
            # Create input directory if it doesn't exist
            os.makedirs(input_path, exist_ok=True)
            
            print("Starting Windowed WordCount...")
            print("=" * 60)
            query = create_windowed_wordcount(
                spark, input_path, output_path, window_duration, slide_duration
            )
            
            try:
                query.awaitTermination()
            except KeyboardInterrupt:
                print("\nStopping stream processing...")
                query.stop()
        
        elif mode == "socket":
            host = sys.argv[2] if len(sys.argv) > 2 else "localhost"
            port = int(sys.argv[3]) if len(sys.argv) > 3 else 9999
            
            print("Starting Socket WordCount...")
            print("=" * 60)
            query = create_socket_wordcount(spark, host, port)
            
            try:
                query.awaitTermination()
            except KeyboardInterrupt:
                print("\nStopping stream processing...")
                query.stop()
        
        else:
            print(f"Unknown mode: {mode}")
            print("Supported modes: file, window, socket")
            sys.exit(1)
    
    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()
        print("Spark session closed")

if __name__ == "__main__":
    main() 