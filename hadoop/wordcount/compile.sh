#!/bin/zsh

# Compile WordCount MapReduce program
echo "========================================="
echo "Compiling WordCount MapReduce Program"
echo "========================================="

# Clean previous builds
echo "Cleaning previous builds..."
rm -rf build
rm -f wordcount.jar

# Create build directory
mkdir -p build

# Find Hadoop classpath
echo "Getting Hadoop classpath..."
HADOOP_CLASSPATH=$(hadoop classpath)

if [ -z "$HADOOP_CLASSPATH" ]; then
    echo "Error: Could not get Hadoop classpath"
    exit 1
fi

# Compile Java source
echo "Compiling Java source files..."
javac -classpath $HADOOP_CLASSPATH -d build WordCount.java

if [ $? -ne 0 ]; then
    echo "Error: Compilation failed"
    exit 1
fi

# Create JAR file
echo "Creating JAR file..."
jar cf wordcount.jar -C build .

if [ $? -ne 0 ]; then
    echo "Error: JAR creation failed"
    exit 1
fi

echo "========================================="
echo "Compilation successful!"
echo "Created: wordcount.jar"
echo "========================================="

# Show JAR contents
echo "JAR file contents:"
jar tf wordcount.jar 