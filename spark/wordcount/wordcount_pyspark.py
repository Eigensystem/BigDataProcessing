#!/usr/bin/env python3
"""
PySpark WordCount Program
A simple word count application using PySpark DataFrame API
"""

import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, lower, regexp_replace, col, desc, concat_ws
from pyspark.sql.types import StringType

def create_spark_session(app_name="WordCount"):
    """Create and return a Spark session"""
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

def wordcount_dataframe(spark, input_path, output_path):
    """
    Perform word count using DataFrame API
    
    Args:
        spark: SparkSession
        input_path: Path to input text files
        output_path: Path to save output results
    """
    print(f"Reading input files from: {input_path}")
    
    # Read text files
    df = spark.read.text(input_path)
    
    # Preprocess text: convert to lowercase and remove punctuation
    cleaned_df = df.select(
        regexp_replace(lower(col("value")), "[^a-zA-Z0-9\\s]", "").alias("cleaned_text")
    )
    
    # Split lines into words and explode into individual rows
    words_df = cleaned_df.select(
        explode(split(col("cleaned_text"), "\\s+")).alias("word")
    )
    
    # Filter out empty strings and count words
    word_counts = words_df \
        .filter(col("word") != "") \
        .groupBy("word") \
        .count() \
        .orderBy(desc("count"))
    
    print("Top 20 most frequent words:")
    word_counts.show(20, truncate=False)
    
    # Save results
    print(f"Saving results to: {output_path}")
    word_counts.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)
    
    # Save as text format - convert to single string column first
    text_output = word_counts.select(
        concat_ws(",", col("word"), col("count")).alias("word_count")
    )
    text_output.coalesce(1).write.mode("overwrite").text(output_path + "_text")
    
    return word_counts

def wordcount_rdd(spark, input_path, output_path):
    """
    Perform word count using RDD API (alternative implementation)
    
    Args:
        spark: SparkSession
        input_path: Path to input text files
        output_path: Path to save output results
    """
    print(f"Reading input files from: {input_path}")
    
    # Read text files as RDD
    text_rdd = spark.sparkContext.textFile(input_path)
    
    # Perform word count
    word_counts = text_rdd \
        .flatMap(lambda line: line.lower().split()) \
        .filter(lambda word: word.strip() != "") \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a + b) \
        .sortBy(lambda x: x[1], ascending=False)
    
    # Convert to DataFrame for display
    word_counts_df = spark.createDataFrame(word_counts, ["word", "count"])
    
    print("Top 20 most frequent words:")
    word_counts_df.show(20, truncate=False)
    
    # Save results
    print(f"Saving results to: {output_path}")
    word_counts_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)
    
    # Save as text format - convert to single string column first
    text_output = word_counts_df.select(
        concat_ws(",", col("word"), col("count")).alias("word_count")
    )
    text_output.coalesce(1).write.mode("overwrite").text(output_path + "_text")
    
    return word_counts_df

def main():
    """Main function"""
    if len(sys.argv) < 2:
        print("Usage: python wordcount_pyspark.py <input_path> [output_path] [method]")
        print("  input_path: Path to input text files (can use wildcards like 'input/*.txt')")
        print("  output_path: Path to save output (default: 'output')")
        print("  method: 'dataframe' or 'rdd' (default: 'dataframe')")
        print()
        print("Examples:")
        print("  python wordcount_pyspark.py input/")
        print("  python wordcount_pyspark.py input/*.txt output dataframe")
        print("  python wordcount_pyspark.py sample_text.txt output rdd")
        sys.exit(1)
    
    input_path = sys.argv[1]
    output_path = sys.argv[2] if len(sys.argv) > 2 else "output"
    method = sys.argv[3] if len(sys.argv) > 3 else "dataframe"
    
    # Check if input path exists
    if not any(os.path.exists(p) for p in [input_path] if not '*' in input_path):
        if '*' not in input_path and not os.path.exists(input_path):
            print(f"Error: Input path '{input_path}' does not exist!")
            sys.exit(1)
    
    # Create Spark session
    spark = create_spark_session("PySpark WordCount")
    
    try:
        print(f"Starting WordCount with method: {method}")
        print("=" * 50)
        
        if method.lower() == "rdd":
            result = wordcount_rdd(spark, input_path, output_path)
        else:
            result = wordcount_dataframe(spark, input_path, output_path)
        
        print("=" * 50)
        print("WordCount completed successfully!")
        print(f"Results saved to: {output_path}")
        
        # Print summary statistics
        total_words = result.agg({"count": "sum"}).collect()[0][0]
        unique_words = result.count()
        print(f"Total words: {total_words}")
        print(f"Unique words: {unique_words}")
        
    except Exception as e:
        print(f"Error occurred: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main() 