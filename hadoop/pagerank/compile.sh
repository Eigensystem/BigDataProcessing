#!/bin/zsh

# Compile PageRank MapReduce programs
echo "========================================="
echo "Compiling PageRank MapReduce Programs"
echo "========================================="

# Clean previous builds
echo "Cleaning previous builds..."
rm -rf build
rm -f pagerank.jar
rm -f preprocessor.jar

# Create build directory
mkdir -p build

# Find Hadoop classpath
echo "Getting Hadoop classpath..."
HADOOP_CLASSPATH=$(hadoop classpath)

if [ -z "$HADOOP_CLASSPATH" ]; then
    echo "Error: Could not get Hadoop classpath"
    exit 1
fi

# Compile Java source files
echo "Compiling Java source files..."

# Compile PageRank
javac -classpath $HADOOP_CLASSPATH -d build PageRank.java
if [ $? -ne 0 ]; then
    echo "Error: PageRank compilation failed"
    exit 1
fi

# Compile GraphPreprocessor
javac -classpath $HADOOP_CLASSPATH -d build GraphPreprocessor.java
if [ $? -ne 0 ]; then
    echo "Error: GraphPreprocessor compilation failed"
    exit 1
fi

# Create JAR files
echo "Creating JAR files..."

# Create PageRank JAR
cd build
jar cf ../pagerank.jar PageRank*.class
if [ $? -ne 0 ]; then
    echo "Error: PageRank JAR creation failed"
    exit 1
fi

# Create Preprocessor JAR
jar cf ../preprocessor.jar GraphPreprocessor*.class
if [ $? -ne 0 ]; then
    echo "Error: Preprocessor JAR creation failed"
    exit 1
fi

cd ..

echo "========================================="
echo "Compilation successful!"
echo "Created: pagerank.jar, preprocessor.jar"
echo "========================================="

# Show JAR contents
echo "PageRank JAR contents:"
jar tf pagerank.jar
echo ""
echo "Preprocessor JAR contents:"
jar tf preprocessor.jar 