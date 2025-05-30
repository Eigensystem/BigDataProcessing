# Spark GraphX Depth-First Search (DFS) Implementation

This project implements a Depth-First Search (DFS) algorithm using Apache Spark GraphX framework. The implementation reads graph data from files and performs DFS traversal with depth tracking for each visited node.

## Features

- **File-based Input**: Load graph data from text files (vertices and edges)
- **Two DFS Approaches**:
  - Iterative DFS using traditional stack-based traversal
  - Pregel-style DFS using message-passing paradigm
- **Depth Tracking**: Track and display the depth of each visited node
- **Comprehensive Analysis**: Detailed traversal results and graph structure visualization
- **Error Handling**: Robust file parsing and validation

## Requirements

- Apache Spark 3.2+
- Python 3.7+
- GraphFrames library
- PySpark

## Installation

1. Install dependencies:
```bash
pip install pyspark graphframes
```

2. Or use the provided requirements file:
```bash
pip install -r requirements.txt
```

## File Formats

### Vertices File
Each line should contain: `id,name`

Example (`vertices.txt`):
```
A,Node A
B,Node B
C,Node C
```

### Edges File  
Each line should contain: `src,dst,relationship` (relationship is optional)

Example (`edges.txt`):
```
A,B,edge_AB
A,C,edge_AC
B,D,edge_BD
```

Comments (lines starting with `#`) and empty lines are ignored.

## Usage

### Direct Python Execution

```bash
# Basic usage
python graph_dfs.py vertices.txt edges.txt

# With custom start node
python graph_dfs.py vertices.txt edges.txt A

# Using different file formats
python graph_dfs.py my_vertices.csv my_edges.csv B
```

### Using the Shell Script

The shell script provides additional features like automatic sample file creation and environment checking:

```bash
# Show help
./run_dfs.sh --help

# Create sample files
./run_dfs.sh --sample

# Check if files exist
./run_dfs.sh --check

# Run with default sample files
./run_dfs.sh

# Run with custom files
./run_dfs.sh my_vertices.txt my_edges.txt

# Run with custom start node
./run_dfs.sh vertices.txt edges.txt B
```

## Sample Graph

The default sample creates the following graph structure:

```
    A
   / \
  B   C
 /|   |
D E   F
| |   |
G H   I
```

- **Vertices**: A, B, C, D, E, F, G, H, I
- **Edges**: A→B, A→C, B→D, B→E, C→F, D→G, E→H, F→I

## Output Example

```
Graph Structure:
==================================================
Vertices: 9
Edges: 8

Starting DFS from node: A
==================================================
Visiting Node: A, Depth: 0, Visit Order: 1
Visiting Node: C, Depth: 1, Visit Order: 2
Visiting Node: F, Depth: 2, Visit Order: 3
Visiting Node: I, Depth: 3, Visit Order: 4
Visiting Node: B, Depth: 1, Visit Order: 5
Visiting Node: E, Depth: 2, Visit Order: 6
Visiting Node: H, Depth: 3, Visit Order: 7
Visiting Node: D, Depth: 2, Visit Order: 8
Visiting Node: G, Depth: 3, Visit Order: 9

DFS Traversal Analysis:
==================================================
Total nodes visited: 9
Maximum depth reached: 3

Nodes by depth level:
  Depth 0: A(#1)
  Depth 1: C(#2), B(#5)
  Depth 2: F(#3), E(#6), D(#8)
  Depth 3: I(#4), H(#7), G(#9)
```

## Algorithm Details

### Iterative DFS
- Uses a stack-based approach for traversal
- Maintains visit order and depth tracking
- Processes neighbors in reverse order for consistent traversal

### Pregel-style DFS
- Uses Spark's distributed message-passing paradigm
- Iteratively propagates depth information to unvisited neighbors
- Suitable for large-scale distributed graphs

## File Structure

```
spark/dfs/
├── graph_dfs.py           # Main DFS implementation
├── run_dfs.sh            # Shell script runner
├── requirements.txt      # Python dependencies
├── sample_vertices.txt   # Sample vertices file
├── sample_edges.txt      # Sample edges file
└── README.md            # This documentation
```

## Error Handling

The program handles various error scenarios:

- **File not found**: Clear error messages with file paths
- **Invalid file format**: Skips malformed lines with warnings
- **Missing start node**: Uses first vertex as default
- **Invalid start node**: Shows available vertices
- **Empty graph**: Validates that vertices and edges exist

## Performance Considerations

- For large graphs, use the Pregel-style DFS approach
- Adjust Spark configuration based on your cluster resources
- Consider partitioning strategies for very large datasets
- Monitor memory usage for graphs with deep traversal paths

## Troubleshooting

### Common Issues

1. **GraphFrames not found**:
   ```bash
   pip install graphframes
   ```

2. **Java version conflicts**:
   - Ensure Java 8 or 11 is installed
   - Set JAVA_HOME environment variable

3. **Memory errors**:
   - Increase Spark driver/executor memory in the script
   - Reduce graph size or use sampling

4. **File encoding issues**:
   - Ensure files are UTF-8 encoded
   - Check for special characters in node names

## Examples

### Creating Custom Graph Files

1. Create vertices file:
```bash
cat > my_vertices.txt << EOF
1,User One
2,User Two
3,User Three
EOF
```

2. Create edges file:
```bash
cat > my_edges.txt << EOF
1,2,friend
2,3,follow
1,3,like
EOF
```

3. Run DFS:
```bash
python graph_dfs.py my_vertices.txt my_edges.txt 1
```

### Integration with Other Systems

The program can easily integrate with other data sources:

- **Database export**: Export to CSV format
- **JSON conversion**: Use jq or Python to convert JSON to CSV
- **Hadoop integration**: Read from HDFS using appropriate file paths

## License

This project is for educational purposes and demonstrates graph processing with Apache Spark. 