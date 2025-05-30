# Spark GraphX Strongly Connected Components (SCC)

This directory contains a comprehensive implementation of Strongly Connected Components (SCC) algorithms using Spark GraphX and GraphFrames.

## 📋 Overview

Strongly Connected Components (SCC) is a fundamental graph algorithm that finds maximal sets of vertices where every vertex is reachable from every other vertex in the set through directed edges. This implementation provides three different approaches:

1. **Built-in Algorithm**: Uses GraphFrames' native SCC implementation
2. **Kosaraju's Algorithm**: Custom implementation of the classic two-pass DFS algorithm
3. **Pregel-style Algorithm**: Iterative message-passing approach

## 🚀 Quick Start

### Prerequisites
- Java 8 or higher
- Python 3.7 or higher
- Apache Spark 3.2+ (automatically installed with PySpark)

### Installation

1. **Install dependencies:**
   ```bash
   pip3 install -r requirements.txt
   ```

2. **Make the run script executable:**
   ```bash
   chmod +x run_scc.sh
   ```

3. **Run with sample data:**
   ```bash
   ./run_scc.sh
   ```

## 📊 Usage Examples

### Basic Usage
```bash
# Run all algorithms with default sample data
./run_scc.sh

# Use specific algorithm
./run_scc.sh -a builtin
./run_scc.sh -a kosaraju
./run_scc.sh -a pregel

# Use custom data files
./run_scc.sh -v my_vertices.txt -e my_edges.txt
```

### Direct Python Usage
```bash
# Run all algorithms
python3 scc_graphx.py sample_vertices.txt sample_edges.txt

# Run specific algorithm
python3 scc_graphx.py sample_vertices.txt sample_edges.txt builtin
python3 scc_graphx.py sample_vertices.txt sample_edges.txt kosaraju
python3 scc_graphx.py sample_vertices.txt sample_edges.txt pregel
```

### Command Line Options
```bash
./run_scc.sh [OPTIONS]

Options:
  -v, --vertices FILE     Vertices file (default: sample_vertices.txt)
  -e, --edges FILE        Edges file (default: sample_edges.txt)  
  -a, --algorithm ALG     Algorithm: builtin, kosaraju, pregel, all (default: all)
  -h, --help             Show help message
```

## 📁 File Formats

### Vertices File Format
Each line should contain vertex ID and name separated by space:
```
A WebServer
B Database
C Cache
D LoadBalancer
```

### Edges File Format
Each line should contain source and destination vertex IDs (directed edge):
```
A B
B C
C A
D E
```

Comments starting with `#` are ignored in both files.

## 🔍 Sample Data

The included sample data creates a directed graph with multiple strongly connected components:

**Sample Vertices:**
- A: WebServer
- B: Database  
- C: Cache
- D: LoadBalancer
- E: BackupServer
- F: Logger
- G: Monitor
- H: Gateway

**Sample Edges:**
- Component 1: A ↔ B ↔ C (3-node cycle)
- Component 2: D ↔ E (2-node cycle)
- Component 3: F (self-loop)
- Inter-component edges: A→D, C→F, E→G, G→H

## 🧮 Algorithm Details

### 1. Built-in SCC Algorithm
- Uses GraphFrames' optimized implementation
- Based on iterative label propagation
- Fastest for large graphs
- Time complexity: O(|V| + |E|) per iteration

### 2. Kosaraju's Algorithm
- Classic two-pass DFS approach
- Step 1: DFS on original graph to compute finishing times
- Step 2: DFS on transposed graph in reverse finishing time order
- Time complexity: O(|V| + |E|)

### 3. Pregel-style Algorithm
- Iterative message-passing approach
- Forward and backward propagation phases
- Distributed-friendly implementation
- Time complexity: O(d × |V|) where d is the diameter

## 📈 Expected Output

### Example Output Structure:
```
================================================
         Spark GraphX SCC Algorithm Runner       
================================================

Configuration:
  Vertices file: sample_vertices.txt
  Edges file: sample_edges.txt
  Algorithm: all

Directed Graph Structure:
Vertices: 8
Directed Edges: 13

============================================================
BUILT-IN SCC ALGORITHM
============================================================
Running Built-in SCC Algorithm...

SCC Results (Built-in Algorithm):
+---+--------------+---------+
| id|          name|component|
+---+--------------+---------+
|  A|     WebServer|        A|
|  B|      Database|        A|
|  C|         Cache|        A|
|  D| LoadBalancer|        D|
|  E|  BackupServer|        D|
|  F|        Logger|        F|
|  G|       Monitor|        G|
|  H|       Gateway|        H|
+---+--------------+---------+

SCC Analysis:
Total number of strongly connected components: 4
Component A: 3 nodes - [A, B, C]
Component D: 2 nodes - [D, E]
Component F: 1 node - [F]
Component G: 1 node - [G]
Component H: 1 node - [H]
```

## 🔧 Troubleshooting

### Common Issues

1. **Java Version Error:**
   ```
   Error: java.lang.UnsupportedClassVersionError
   ```
   **Solution:** Install Java 8 or higher:
   ```bash
   # Check Java version
   java -version
   
   # Install Java 8 (Ubuntu/Debian)
   sudo apt-get install openjdk-8-jdk
   ```

2. **PySpark Import Error:**
   ```
   ModuleNotFoundError: No module named 'pyspark'
   ```
   **Solution:**
   ```bash
   pip3 install pyspark==3.5.0
   ```

3. **GraphFrames Package Error:**
   ```
   java.lang.ClassNotFoundException: org.graphframes.GraphFrame
   ```
   **Solution:** GraphFrames is downloaded automatically. Ensure internet connection.

4. **Memory Issues:**
   ```
   OutOfMemoryError: Java heap space
   ```
   **Solution:** Increase memory allocation:
   ```bash
   export SPARK_DRIVER_MEMORY=2g
   export SPARK_EXECUTOR_MEMORY=2g
   ```

5. **File Not Found:**
   ```
   FileNotFoundError: [Errno 2] No such file or directory
   ```
   **Solution:** Check file paths and ensure files exist in the current directory.

### Performance Tips

1. **For Large Graphs:**
   - Use the built-in algorithm for best performance
   - Increase Spark memory settings
   - Consider partitioning large datasets

2. **For Analysis:**
   - Use Kosaraju's algorithm to understand the step-by-step process
   - Pregel-style for distributed processing scenarios

3. **Memory Optimization:**
   ```python
   # Add to Spark configuration
   .config("spark.sql.adaptive.enabled", "true")
   .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
   ```

## 🔗 Graph Properties

The SCC algorithm works on **directed graphs** and finds:
- **Strongly Connected Components**: Maximal sets of vertices where every vertex can reach every other vertex
- **Component Sizes**: Number of vertices in each component
- **Largest Component**: The component with the most vertices
- **Component Connectivity**: How components are connected

### Use Cases
- **Social Networks**: Finding communities with mutual connections
- **Web Graphs**: Identifying strongly connected web page clusters
- **Dependency Analysis**: Finding circular dependencies in software
- **Transportation Networks**: Analyzing route connectivity

## 📚 References

- [Kosaraju's Algorithm](https://en.wikipedia.org/wiki/Kosaraju%27s_algorithm)
- [GraphFrames User Guide](https://graphframes.github.io/graphframes/docs/_site/user-guide.html)
- [Apache Spark GraphX](https://spark.apache.org/docs/latest/graphx-programming-guide.html)
- [Strongly Connected Components](https://en.wikipedia.org/wiki/Strongly_connected_component)

## 🤝 Contributing

Feel free to submit issues, improvements, or additional algorithms for SCC computation!

## 📄 License

This project is part of the bigdata-exp educational repository. 