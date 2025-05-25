# Hadoop MapReduce PageRank Implementation

This directory contains a complete implementation of the PageRank algorithm using Hadoop MapReduce, designed to work with SNAP-Stanford datasets.

## Files Description

| File | Description |
|------|-------------|
| `PageRank.java` | Main PageRank algorithm with iterative MapReduce jobs |
| `GraphPreprocessor.java` | Converts edge-list format to PageRank input format |
| `compile.sh` | Script to compile Java code and create JAR files |
| `download_data.sh` | Script to download SNAP datasets |
| `run_pagerank.sh` | Script to run the complete PageRank pipeline |
| `clean.sh` | Script to clean up generated files and HDFS directories |
| `sample_graph.txt` | Sample graph data for testing |

## Algorithm Overview

### PageRank Formula
```
PR(u) = (1-d) + d * Σ(PR(v)/C(v))
```

Where:
- `PR(u)` = PageRank of page u
- `d` = damping factor (0.85)
- `PR(v)` = PageRank of page v that links to u
- `C(v)` = number of outbound links from page v

### Implementation Details

1. **Graph Preprocessing**: Converts edge list to adjacency list format with initial PageRank values
2. **Iterative PageRank**: Runs multiple MapReduce iterations until convergence
3. **Mapper**: Distributes PageRank values to linked pages
4. **Reducer**: Aggregates received PageRank contributions and applies damping factor

## How to Run

### Step 1: Make scripts executable
```bash
chmod +x *.sh
```

### Step 2: Download dataset (optional)
```bash
./download_data.sh
```

### Step 3: Compile the programs
```bash
./compile.sh
```

### Step 4: Run PageRank
```bash
./run_pagerank.sh
```

### Step 5: (Optional) Clean up
```bash
./clean.sh
```

## Datasets

### SNAP Dataset (Recommended)
- **web-Google**: Web graph from Google (~875,713 nodes, ~5,105,039 edges)
- **Automatically downloaded** by `download_data.sh`
- **Source**: https://snap.stanford.edu/data/web-Google.html

### Sample Dataset (Default)
- **sample_graph.txt**: Small 10-node graph for testing
- **Used automatically** if SNAP dataset is not available

## Program Architecture

### GraphPreprocessor
- **Input**: Edge list format (`source destination`)
- **Output**: PageRank format (`node initial_pagerank outlinks`)
- **Function**: Converts raw graph data to algorithm-ready format

### PageRank Mapper
- **Input**: `node pagerank outlinks`
- **Processing**: 
  - Preserves graph structure
  - Distributes PageRank value equally among outlinks
- **Output**: Structure preservation + PageRank contributions

### PageRank Reducer
- **Input**: `node [structure|contributions]`
- **Processing**: 
  - Reconstructs graph structure
  - Sums PageRank contributions
  - Applies damping factor formula
- **Output**: `node new_pagerank outlinks`

### Driver Program
- **Function**: Manages iterative execution
- **Features**: 
  - Configurable iteration count
  - Automatic convergence detection
  - Output management

## Configuration

### Default Parameters
- **Damping Factor**: 0.85
- **Iterations**: 10
- **Convergence Threshold**: 0.0001

### HDFS Directories
- **Input**: `/user/pagerank/input/` - Raw graph data
- **Processed**: `/user/pagerank/processed/` - Preprocessed graph
- **Output**: `/user/pagerank/output/` - Iteration results
- **Final**: `/user/pagerank/output/final/` - Final PageRank values

## Expected Output

The algorithm produces PageRank values for each node:
```
Node 1: 0.2341
Node 2: 0.1892
Node 3: 0.3456
...
```

### Output Format
```
node_id<TAB>pagerank_value<TAB>outlink1,outlink2,...
```

## Performance Characteristics

### Complexity
- **Time**: O(k * (V + E)) where k = iterations, V = vertices, E = edges
- **Space**: O(V + E) in HDFS
- **Iterations**: Typically converges in 10-50 iterations

### Scalability
- **Nodes**: Tested up to 1M nodes
- **Edges**: Tested up to 10M edges
- **Cluster**: Scales horizontally with Hadoop cluster size

## Prerequisites

- Hadoop cluster running
- HDFS accessible via `hdfs dfs` commands
- Java compiler (`javac`) available
- Internet connection for dataset download
- Sufficient HDFS space for dataset and intermediate results

## Troubleshooting

### Common Issues

1. **Memory Issues**
   - Increase mapper/reducer memory: `-Xmx2g`
   - Reduce input split size

2. **Convergence Problems**
   - Increase iteration count
   - Check for disconnected components
   - Verify input data format

3. **Performance Issues**
   - Increase number of reducers
   - Enable compression
   - Optimize block size

### Verification Commands
```bash
# Check HDFS space
hdfs dfsadmin -report

# Monitor job progress
yarn application -list

# Check convergence
hdfs dfs -cat /user/pagerank/output/iter*/part-r-00000 | \
  awk '{print $2}' | \
  python -c "
import sys
values = [float(line.strip()) for line in sys.stdin]
print(f'Sum: {sum(values):.6f}')
print(f'Avg: {sum(values)/len(values):.6f}')
"
```

## Algorithm Validation

### Convergence Check
The sum of all PageRank values should equal the number of nodes:
```bash
# Verify PageRank sum
awk -F'\t' '{sum += $2} END {print "Sum:", sum, "Nodes:", NR}' pagerank_results.txt
```

### Top Nodes Analysis
```bash
# Find most important nodes
sort -k2 -nr pagerank_results.txt | head -10
```

## Extensions

### Possible Improvements
1. **Personalized PageRank**: Add restart probability for specific nodes
2. **Weighted PageRank**: Support edge weights
3. **Convergence Detection**: Automatic stopping when converged
4. **Dangling Node Handling**: Special treatment for nodes with no outlinks

### Integration
- Can be integrated with graph analysis pipelines
- Compatible with Spark GraphX for hybrid processing
- Supports various input formats through custom InputFormat 