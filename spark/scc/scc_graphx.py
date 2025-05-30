#!/usr/bin/env python3
"""
Spark GraphX Strongly Connected Components (SCC) Implementation
This program implements SCC algorithms using Spark GraphX with both built-in and custom implementations.
"""

import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, greatest, coalesce, min as spark_min, collect_list, size
from pyspark.sql.types import StructType, StructField, LongType, IntegerType, StringType
from graphframes import GraphFrame

def create_spark_session(app_name="GraphX_SCC"):
    """Create and return a Spark session with GraphFrames support"""
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.driver.memory", "1g") \
        .config("spark.driver.maxResultSize", "512m") \
        .config("spark.executor.memory", "1g") \
        .config("spark.sql.execution.arrow.maxRecordsPerBatch", "1000") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.localShuffleReader.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.default.parallelism", "2") \
        .config("spark.jars.packages", "graphframes:graphframes:0.8.2-spark3.2-s_2.12") \
        .getOrCreate()

def load_graph_from_files(spark, vertices_file, edges_file):
    """
    Load directed graph from text files for SCC analysis
    
    Args:
        spark: SparkSession
        vertices_file: Path to vertices file (format: id name)
        edges_file: Path to edges file (format: src dst)
    
    Returns:
        GraphFrame object
    """
    print(f"Loading directed graph from files:")
    print(f"  Vertices: {vertices_file}")
    print(f"  Edges: {edges_file}")
    
    try:
        # Read vertices file
        vertices_data = []
        with open(vertices_file, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    parts = line.split()
                    if len(parts) >= 2:
                        vertex_id = parts[0].strip()
                        vertex_name = ' '.join(parts[1:]).strip()
                        vertices_data.append((vertex_id, vertex_name))
        
        # Read edges file
        edges_data = []
        with open(edges_file, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    parts = line.split()
                    if len(parts) >= 2:
                        src = parts[0].strip()
                        dst = parts[1].strip()
                        relationship = f"directed_edge"
                        edges_data.append((src, dst, relationship))
        
        if not vertices_data:
            raise ValueError(f"No valid vertices found in {vertices_file}")
        if not edges_data:
            raise ValueError(f"No valid edges found in {edges_file}")
        
        print(f"Loaded {len(vertices_data)} vertices and {len(edges_data)} directed edges")
        
        # Create DataFrames
        vertices = spark.createDataFrame(vertices_data, ["id", "name"])
        edges = spark.createDataFrame(edges_data, ["src", "dst", "relationship"])
        
        return GraphFrame(vertices, edges)
        
    except FileNotFoundError as e:
        raise FileNotFoundError(f"File not found: {e.filename}")
    except Exception as e:
        raise Exception(f"Error loading graph from files: {str(e)}")

def scc_builtin(graph):
    """
    Use GraphFrames built-in strongly connected components algorithm
    
    Args:
        graph: GraphFrame object
    
    Returns:
        DataFrame with SCC results
    """
    print("Running Built-in SCC Algorithm...")
    print("=" * 50)
    
    # Run built-in SCC algorithm with reduced iterations to avoid memory issues
    scc_result = graph.stronglyConnectedComponents(maxIter=3)
    
    # Collect results
    scc_df = scc_result.select("id", "name", "component").orderBy("component", "id")
    
    print("SCC Results (Built-in Algorithm):")
    scc_df.show(truncate=False)
    
    return scc_df

def kosaraju_scc(graph):
    """
    Custom implementation of Kosaraju's algorithm for SCC
    Kosaraju's algorithm: 
    1. DFS on original graph to get finishing times
    2. DFS on transposed graph in reverse finishing time order
    
    Args:
        graph: GraphFrame object
    
    Returns:
        DataFrame with SCC results
    """
    print("Running Kosaraju's SCC Algorithm...")
    print("=" * 50)
    
    # Step 1: DFS on original graph to compute finishing times
    print("Step 1: Computing finishing times with DFS...")
    finishing_times = dfs_finishing_times(graph)
    
    # Step 2: Create transposed graph
    print("Step 2: Creating transposed graph...")
    transposed_graph = transpose_graph(graph)
    
    # Step 3: DFS on transposed graph in reverse finishing time order
    print("Step 3: DFS on transposed graph...")
    scc_components = dfs_on_transposed(transposed_graph, finishing_times)
    
    return scc_components

def dfs_finishing_times(graph):
    """Compute finishing times using DFS traversal"""
    vertices_list = [row.id for row in graph.vertices.select("id").collect()]
    edges_dict = {}
    
    # Build adjacency list
    for row in graph.edges.select("src", "dst").collect():
        if row.src not in edges_dict:
            edges_dict[row.src] = []
        edges_dict[row.src].append(row.dst)
    
    visited = set()
    finishing_times = {}
    time_counter = [0]  # Use list to allow modification in nested function
    
    def dfs_visit(node):
        visited.add(node)
        if node in edges_dict:
            for neighbor in edges_dict[node]:
                if neighbor not in visited:
                    dfs_visit(neighbor)
        time_counter[0] += 1
        finishing_times[node] = time_counter[0]
        print(f"  Node {node} finished at time {time_counter[0]}")
    
    for vertex in vertices_list:
        if vertex not in visited:
            dfs_visit(vertex)
    
    return finishing_times

def transpose_graph(graph):
    """Create transposed (reversed) graph"""
    # Reverse the direction of all edges
    transposed_edges = graph.edges.select(
        col("dst").alias("src"),
        col("src").alias("dst"),
        col("relationship")
    )
    
    return GraphFrame(graph.vertices, transposed_edges)

def dfs_on_transposed(transposed_graph, finishing_times):
    """DFS on transposed graph in reverse finishing time order"""
    # Sort vertices by finishing time (descending)
    sorted_vertices = sorted(finishing_times.items(), key=lambda x: x[1], reverse=True)
    
    # Build adjacency list for transposed graph
    edges_dict = {}
    for row in transposed_graph.edges.select("src", "dst").collect():
        if row.src not in edges_dict:
            edges_dict[row.src] = []
        edges_dict[row.src].append(row.dst)
    
    visited = set()
    scc_id = 0
    node_to_scc = {}
    
    def dfs_scc(node, current_scc_id):
        visited.add(node)
        node_to_scc[node] = current_scc_id
        component_nodes = [node]
        
        if node in edges_dict:
            for neighbor in edges_dict[node]:
                if neighbor not in visited:
                    component_nodes.extend(dfs_scc(neighbor, current_scc_id))
        
        return component_nodes
    
    scc_components = []
    for vertex, _ in sorted_vertices:
        if vertex not in visited:
            scc_nodes = dfs_scc(vertex, scc_id)
            scc_components.append((scc_id, scc_nodes))
            print(f"  SCC {scc_id}: {scc_nodes}")
            scc_id += 1
    
    # Create result DataFrame
    spark = transposed_graph.vertices.sql_ctx.sparkSession
    result_data = []
    vertices_info = {row.id: row.name for row in transposed_graph.vertices.collect()}
    
    for node, scc_id in node_to_scc.items():
        result_data.append((node, vertices_info[node], scc_id))
    
    result_df = spark.createDataFrame(result_data, ["id", "name", "component"])
    result_df = result_df.orderBy("component", "id")
    
    print("\nKosaraju SCC Results:")
    result_df.show(truncate=False)
    
    return result_df

def pregel_scc(graph, max_iterations=20):
    """
    Pregel-style SCC algorithm using iterative message passing
    
    Args:
        graph: GraphFrame object
        max_iterations: Maximum number of iterations
    
    Returns:
        DataFrame with SCC results
    """
    print("Running Pregel-style SCC Algorithm...")
    print("=" * 50)
    
    # Initialize vertices with their own ID as component
    vertices_init = graph.vertices.withColumn("component", col("id"))
    current_graph = GraphFrame(vertices_init, graph.edges)
    
    for iteration in range(max_iterations):
        print(f"Iteration {iteration + 1}")
        
        # Forward propagation: send minimum component ID to neighbors
        forward_messages = current_graph.edges.join(
            current_graph.vertices.select("id", "component"),
            col("src") == col("id")
        ).select(
            col("dst").alias("id"),
            col("component").alias("msg_component")
        ).groupBy("id").agg(spark_min("msg_component").alias("min_component"))
        
        # Update vertices with minimum component received
        updated_vertices = current_graph.vertices.join(
            forward_messages, ["id"], "left"
        ).withColumn(
            "component",
            when(
                col("min_component").isNotNull(),
                when(col("min_component") < col("component"), col("min_component"))
                .otherwise(col("component"))
            ).otherwise(col("component"))
        ).drop("min_component")
        
        # Check convergence
        changes = updated_vertices.join(
            current_graph.vertices.select("id", col("component").alias("old_component")),
            ["id"]
        ).filter(col("component") != col("old_component")).count()
        
        current_graph = GraphFrame(updated_vertices, current_graph.edges)
        
        print(f"  Changes made: {changes}")
        
        if changes == 0:
            print("  Converged!")
            break
    
    # Backward propagation for refinement
    print("Starting backward propagation...")
    for iteration in range(max_iterations):
        print(f"Backward iteration {iteration + 1}")
        
        # Backward propagation: send component ID through reverse edges
        backward_messages = current_graph.edges.join(
            current_graph.vertices.select("id", "component"),
            col("dst") == col("id")
        ).select(
            col("src").alias("id"),
            col("component").alias("msg_component")
        ).groupBy("id").agg(spark_min("msg_component").alias("min_component"))
        
        updated_vertices = current_graph.vertices.join(
            backward_messages, ["id"], "left"
        ).withColumn(
            "component",
            when(
                col("min_component").isNotNull(),
                when(col("min_component") < col("component"), col("min_component"))
                .otherwise(col("component"))
            ).otherwise(col("component"))
        ).drop("min_component")
        
        changes = updated_vertices.join(
            current_graph.vertices.select("id", col("component").alias("old_component")),
            ["id"]
        ).filter(col("component") != col("old_component")).count()
        
        current_graph = GraphFrame(updated_vertices, current_graph.edges)
        
        print(f"  Backward changes: {changes}")
        
        if changes == 0:
            print("  Backward converged!")
            break
    
    result_df = current_graph.vertices.select("id", "name", "component").orderBy("component", "id")
    
    print("Pregel-style SCC Results:")
    result_df.show(truncate=False)
    
    return result_df

def analyze_scc_results(scc_df):
    """Analyze and display SCC results"""
    print("\nSCC Analysis:")
    print("=" * 50)
    
    # Count components and their sizes
    component_sizes = scc_df.groupBy("component").agg(
        collect_list("id").alias("nodes"),
        size(collect_list("id")).alias("size")
    ).orderBy("component")
    
    total_components = component_sizes.count()
    print(f"Total number of strongly connected components: {total_components}")
    
    print("\nComponent details:")
    for row in component_sizes.collect():
        nodes_str = ", ".join(sorted(row.nodes))
        print(f"  Component {row.component}: {row.size} nodes - [{nodes_str}]")
    
    # Find largest component
    largest_component = component_sizes.orderBy(col("size").desc()).first()
    print(f"\nLargest component: {largest_component.component} with {largest_component.size} nodes")
    
    return component_sizes

def visualize_graph_structure(graph):
    """Display graph structure information"""
    print("\nDirected Graph Structure:")
    print("=" * 50)
    
    vertices_count = graph.vertices.count()
    edges_count = graph.edges.count()
    
    print(f"Vertices: {vertices_count}")
    print(f"Directed Edges: {edges_count}")
    
    print("\nVertices:")
    graph.vertices.select("id", "name").orderBy("id").show(truncate=False)
    
    print("Directed Edges:")
    graph.edges.select("src", "dst").orderBy("src", "dst").show(truncate=False)
    
    # Calculate in-degree and out-degree
    in_degree = graph.inDegrees.withColumnRenamed("inDegree", "in_deg")
    out_degree = graph.outDegrees.withColumnRenamed("outDegree", "out_deg")
    
    degree_stats = graph.vertices.select("id", "name").join(
        in_degree, ["id"], "left"
    ).join(
        out_degree, ["id"], "left"
    ).fillna(0, ["in_deg", "out_deg"]).orderBy("id")
    
    print("Degree Statistics:")
    degree_stats.show(truncate=False)

def main():
    """Main function"""
    if len(sys.argv) < 3:
        print("Usage: python scc_graphx.py <vertices_file> <edges_file> [algorithm]")
        print()
        print("Arguments:")
        print("  vertices_file    - Path to vertices file (format: id name)")
        print("  edges_file      - Path to directed edges file (format: src dst)")
        print("  algorithm       - SCC algorithm to use: 'builtin', 'kosaraju', 'pregel', or 'all' (default: 'all')")
        print()
        print("File formats:")
        print("  Vertices file: Each line should contain 'id name'")
        print("  Edges file: Each line should contain 'src dst' (directed edge)")
        print()
        print("Examples:")
        print("  python scc_graphx.py sample_vertices.txt sample_edges.txt builtin")
        print("  python scc_graphx.py my_vertices.txt my_edges.txt kosaraju")
        print("  python scc_graphx.py graph_vertices.txt graph_edges.txt all")
        sys.exit(1)
    
    vertices_file = sys.argv[1]
    edges_file = sys.argv[2]
    algorithm = sys.argv[3] if len(sys.argv) > 3 else "all"
    
    if algorithm not in ["builtin", "kosaraju", "pregel", "all"]:
        print(f"Error: Invalid algorithm '{algorithm}'. Choose from: builtin, kosaraju, pregel, all")
        sys.exit(1)
    
    # Create Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        # Load graph from files
        graph = load_graph_from_files(spark, vertices_file, edges_file)
        
        print("Directed graph loaded successfully!")
        visualize_graph_structure(graph)
        
        results = {}
        
        # Run selected algorithm(s)
        if algorithm == "builtin" or algorithm == "all":
            print("\n" + "="*60)
            print("BUILT-IN SCC ALGORITHM")
            print("="*60)
            results["builtin"] = scc_builtin(graph)
            analyze_scc_results(results["builtin"])
        
        if algorithm == "kosaraju" or algorithm == "all":
            print("\n" + "="*60)
            print("KOSARAJU'S SCC ALGORITHM")
            print("="*60)
            results["kosaraju"] = kosaraju_scc(graph)
            analyze_scc_results(results["kosaraju"])
        
        if algorithm == "pregel" or algorithm == "all":
            print("\n" + "="*60)
            print("PREGEL-STYLE SCC ALGORITHM")
            print("="*60)
            results["pregel"] = pregel_scc(graph)
            analyze_scc_results(results["pregel"])
        
        # Compare results if multiple algorithms were run
        if len(results) > 1:
            print("\n" + "="*60)
            print("ALGORITHM COMPARISON")
            print("="*60)
            compare_scc_results(results)

    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()
        print("\nSpark session closed")

def compare_scc_results(results):
    """Compare results from different SCC algorithms"""
    print("Comparing SCC algorithm results...")
    
    algorithms = list(results.keys())
    for i, alg1 in enumerate(algorithms):
        for alg2 in algorithms[i+1:]:
            print(f"\nComparing {alg1} vs {alg2}:")
            
            # Get component counts
            count1 = results[alg1].select("component").distinct().count()
            count2 = results[alg2].select("component").distinct().count()
            
            print(f"  {alg1} found {count1} components")
            print(f"  {alg2} found {count2} components")
            
            if count1 == count2:
                print("  ✓ Same number of components found")
            else:
                print("  ✗ Different number of components found")

if __name__ == "__main__":
    main() 