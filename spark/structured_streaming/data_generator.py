#!/usr/bin/env python3
"""
Data Generator for Streaming WordCount
Generates text files continuously to simulate streaming data
"""

import os
import time
import random
import uuid
from datetime import datetime

# Sample sentences for generating data
SAMPLE_SENTENCES = [
    "Apache Spark is a unified analytics engine for large-scale data processing",
    "Structured Streaming provides real-time data processing capabilities",
    "PySpark is the Python API for Apache Spark framework",
    "Big data analytics requires distributed computing frameworks",
    "Machine learning algorithms can process streaming data in real-time",
    "Kafka is a popular distributed streaming platform",
    "Data pipelines enable continuous data processing workflows",
    "Stream processing handles unbounded data sets efficiently",
    "Event-time processing is crucial for handling late arriving data",
    "Windowing operations aggregate streaming data over time intervals",
    "Fault tolerance ensures reliable streaming data processing",
    "Checkpointing enables recovery from failures in streaming applications",
    "Watermarking handles late data in event-time based processing",
    "Continuous processing provides low-latency stream processing",
    "Batch processing handles bounded data sets effectively"
]

def generate_file(output_dir, file_prefix="data", sentences_per_file=5):
    """Generate a single text file with random sentences"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    unique_id = str(uuid.uuid4())[:8]
    filename = f"{file_prefix}_{timestamp}_{unique_id}.txt"
    filepath = os.path.join(output_dir, filename)
    
    with open(filepath, 'w', encoding='utf-8') as f:
        for _ in range(sentences_per_file):
            sentence = random.choice(SAMPLE_SENTENCES)
            f.write(sentence + "\n")
    
    print(f"Generated file: {filename} (contains {sentences_per_file} lines)")
    return filepath

def continuous_generation(output_dir, interval=5, duration=300, sentences_per_file=3):
    """
    Continuously generate files
    
    Args:
        output_dir: Output directory for generated files
        interval: Interval between file generation (seconds)
        duration: Total duration to run (seconds, 0 for infinite)
        sentences_per_file: Number of sentences per file
    """
    os.makedirs(output_dir, exist_ok=True)
    
    print(f"Starting to generate data files to directory: {output_dir}")
    print(f"Generation interval: {interval} seconds")
    print(f"Lines per file: {sentences_per_file}")
    if duration > 0:
        print(f"Run duration: {duration} seconds")
    else:
        print("Continuous run (press Ctrl+C to stop)")
    print("=" * 50)
    
    start_time = time.time()
    file_count = 0
    
    try:
        while True:
            generate_file(output_dir, sentences_per_file=sentences_per_file)
            file_count += 1
            
            elapsed_time = time.time() - start_time
            if duration > 0 and elapsed_time >= duration:
                print(f"\nRun completed! Generated {file_count} files in total")
                break
            
            print(f"Waiting {interval} seconds...")
            time.sleep(interval)
            
    except KeyboardInterrupt:
        print(f"\nUser stopped! Generated {file_count} files in total")

def batch_generation(output_dir, num_files=10, sentences_per_file=5):
    """
    Generate a batch of files at once
    
    Args:
        output_dir: Output directory for generated files
        num_files: Number of files to generate
        sentences_per_file: Number of sentences per file
    """
    os.makedirs(output_dir, exist_ok=True)
    
    print(f"Batch generating {num_files} files to directory: {output_dir}")
    print(f"Lines per file: {sentences_per_file}")
    print("=" * 50)
    
    for i in range(num_files):
        generate_file(output_dir, file_prefix=f"batch_{i+1:03d}", sentences_per_file=sentences_per_file)
        time.sleep(0.1)  # Small delay to ensure different timestamps
    
    print(f"\nBatch generation completed! Generated {num_files} files in total")

def main():
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python data_generator.py <mode> [options]")
        print()
        print("Modes:")
        print("  continuous <output_dir> [interval] [duration] [sentences_per_file]")
        print("  batch <output_dir> [num_files] [sentences_per_file]")
        print("  single <output_dir> [sentences_per_file]")
        print()
        print("Examples:")
        print("  python data_generator.py continuous input/ 3 60 5")
        print("  python data_generator.py batch input/ 10 5")
        print("  python data_generator.py single input/ 3")
        sys.exit(1)
    
    mode = sys.argv[1].lower()
    
    if mode == "continuous":
        output_dir = sys.argv[2] if len(sys.argv) > 2 else "input/"
        interval = int(sys.argv[3]) if len(sys.argv) > 3 else 5
        duration = int(sys.argv[4]) if len(sys.argv) > 4 else 0
        sentences_per_file = int(sys.argv[5]) if len(sys.argv) > 5 else 3
        
        continuous_generation(output_dir, interval, duration, sentences_per_file)
    
    elif mode == "batch":
        output_dir = sys.argv[2] if len(sys.argv) > 2 else "input/"
        num_files = int(sys.argv[3]) if len(sys.argv) > 3 else 10
        sentences_per_file = int(sys.argv[4]) if len(sys.argv) > 4 else 5
        
        batch_generation(output_dir, num_files, sentences_per_file)
    
    elif mode == "single":
        output_dir = sys.argv[2] if len(sys.argv) > 2 else "input/"
        sentences_per_file = int(sys.argv[3]) if len(sys.argv) > 3 else 3
        
        generate_file(output_dir, sentences_per_file=sentences_per_file)
    
    else:
        print(f"Unknown mode: {mode}")
        print("Supported modes: continuous, batch, single")
        sys.exit(1)

if __name__ == "__main__":
    main() 