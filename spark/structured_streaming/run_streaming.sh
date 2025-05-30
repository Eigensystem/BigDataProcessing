#!/bin/bash

# Spark Structured Streaming WordCount Runner Script
# Provides multiple running modes and configuration options

echo "Spark Structured Streaming WordCount Runner"
echo "=========================================="

# Check if Python is available
if ! command -v python3 &> /dev/null; then
    echo "Error: Python3 is not installed or not in PATH"
    exit 1
fi

# Check if PySpark is installed
if ! python3 -c "import pyspark" &> /dev/null; then
    echo "PySpark is not installed, installing from requirements.txt..."
    if [ -f "requirements.txt" ]; then
        pip3 install -r requirements.txt
    else
        pip3 install pyspark
    fi
fi

# Default values
MODE="file"
INPUT_PATH="input/"
OUTPUT_PATH="output/"
TRIGGER_INTERVAL=10
WINDOW_DURATION="1 minute"
SLIDE_DURATION="30 seconds"
SOCKET_HOST="localhost"
SOCKET_PORT=9999

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -m|--mode)
            MODE="$2"
            shift 2
            ;;
        -i|--input)
            INPUT_PATH="$2"
            shift 2
            ;;
        -o|--output)
            OUTPUT_PATH="$2"
            shift 2
            ;;
        -t|--trigger)
            TRIGGER_INTERVAL="$2"
            shift 2
            ;;
        -w|--window)
            WINDOW_DURATION="$2"
            shift 2
            ;;
        -s|--slide)
            SLIDE_DURATION="$2"
            shift 2
            ;;
        --host)
            SOCKET_HOST="$2"
            shift 2
            ;;
        --port)
            SOCKET_PORT="$2"
            shift 2
            ;;
        -h|--help)
            echo "Usage: $0 [options]"
            echo "Options:"
            echo "  -m, --mode MODE      Running mode: file, window, socket (default: file)"
            echo "  -i, --input PATH     Input path (default: input/)"
            echo "  -o, --output PATH    Output path (default: output/)"
            echo "  -t, --trigger SEC    Trigger interval in seconds (default: 10)"
            echo "  -w, --window DUR     Window size (default: '1 minute')"
            echo "  -s, --slide DUR      Slide interval (default: '30 seconds')"
            echo "  --host HOST          Socket host (default: localhost)"
            echo "  --port PORT          Socket port (default: 9999)"
            echo "  -h, --help           Show help information"
            echo ""
            echo "Examples:"
            echo "  $0 -m file -i input/ -o output/ -t 5"
            echo "  $0 -m window -w '2 minutes' -s '1 minute'"
            echo "  $0 -m socket --host localhost --port 9999"
            echo ""
            echo "Quick start commands:"
            echo "  ./run_streaming.sh                    # File stream mode, default settings"
            echo "  ./run_streaming.sh -m window          # Window mode"
            echo "  ./run_streaming.sh -m socket          # Socket mode"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use -h or --help to view help information"
            exit 1
            ;;
    esac
done

# Clean previous output
cleanup_output() {
    if [ -d "$OUTPUT_PATH" ]; then
        echo "Cleaning previous output directory: $OUTPUT_PATH"
        rm -rf "$OUTPUT_PATH"
    fi
    
    if [ -d "${OUTPUT_PATH}_checkpoint" ]; then
        echo "Cleaning previous checkpoint directory: ${OUTPUT_PATH}_checkpoint"
        rm -rf "${OUTPUT_PATH}_checkpoint"
    fi
    
    if [ -d "checkpoint" ]; then
        echo "Cleaning default checkpoint directory"
        rm -rf "checkpoint"
    fi
}

# Show runtime information
show_info() {
    echo "Runtime configuration:"
    echo "  Mode: $MODE"
    case $MODE in
        file)
            echo "  Input path: $INPUT_PATH"
            echo "  Output path: $OUTPUT_PATH"
            echo "  Trigger interval: $TRIGGER_INTERVAL seconds"
            ;;
        window)
            echo "  Input path: $INPUT_PATH"
            echo "  Output path: $OUTPUT_PATH"
            echo "  Window size: $WINDOW_DURATION"
            echo "  Slide interval: $SLIDE_DURATION"
            ;;
        socket)
            echo "  Socket address: $SOCKET_HOST:$SOCKET_PORT"
            ;;
    esac
    echo ""
}

# Start data generator (run in background)
start_data_generator() {
    if [ "$MODE" = "file" ] || [ "$MODE" = "window" ]; then
        echo "Starting data generator..."
        if [ -f "data_generator.py" ]; then
            python3 data_generator.py continuous "$INPUT_PATH" 8 0 3 &
            GENERATOR_PID=$!
            echo "Data generator started (PID: $GENERATOR_PID)"
            echo "Data will be generated every 8 seconds to $INPUT_PATH"
            echo ""
            sleep 2
        else
            echo "Warning: Data generator file does not exist, please manually add files to $INPUT_PATH"
        fi
    fi
}

# Stop data generator
stop_data_generator() {
    if [ ! -z "$GENERATOR_PID" ]; then
        echo "Stopping data generator (PID: $GENERATOR_PID)..."
        kill $GENERATOR_PID 2>/dev/null
    fi
}

# Main function
main() {
    cleanup_output
    show_info
    
    # Run program according to mode
    case $MODE in
        file)
            start_data_generator
            echo "Starting File Stream WordCount..."
            echo "Program will monitor new files in $INPUT_PATH directory"
            echo "Press Ctrl+C to stop the program"
            echo "=" * 60
            python3 streaming_wordcount.py file "$INPUT_PATH" "$OUTPUT_PATH" "$TRIGGER_INTERVAL"
            ;;
        window)
            start_data_generator
            echo "Starting Windowed WordCount..."
            echo "Program will use time windows for aggregation"
            echo "Press Ctrl+C to stop the program"
            echo "=" * 60
            python3 streaming_wordcount.py window "$INPUT_PATH" "$OUTPUT_PATH" "$WINDOW_DURATION" "$SLIDE_DURATION"
            ;;
        socket)
            echo "Starting Socket WordCount..."
            echo "Please run the following command in another terminal to send data:"
            echo "  nc -lk $SOCKET_PORT"
            echo "Then input text for real-time word counting"
            echo "Press Ctrl+C to stop the program"
            echo "=" * 60
            python3 streaming_wordcount.py socket "$SOCKET_HOST" "$SOCKET_PORT"
            ;;
        *)
            echo "Error: Unknown mode '$MODE'"
            echo "Supported modes: file, window, socket"
            exit 1
            ;;
    esac
}

# Set signal handling
trap 'echo -e "\nStopping..."; stop_data_generator; exit 0' INT TERM

# Run main function
main

# Cleanup
stop_data_generator 