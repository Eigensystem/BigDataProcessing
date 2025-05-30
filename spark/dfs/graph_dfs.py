#!/usr/bin/env python3
"""
Spark GraphX Depth-First Search (DFS) Implementation
This program implements DFS algorithm using Spark GraphX with depth tracking for each visited node.
"""

import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, greatest, coalesce
from pyspark.sql.types import StructType, StructField, LongType, IntegerType, StringType, BooleanType
from graphframes import GraphFrame

def create_spark_session(app_name="GraphX_DFS"):
    """Create and return a Spark session with GraphFrames support"""
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.jars.packages", "graphframes:graphframes:0.8.2-spark3.2-s_2.12") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .getOrCreate()



def load_graph_from_files(spark, vertices_file, edges_file):
    """
    Load graph from text files
    
    Args:
        spark: SparkSession
        vertices_file: Path to vertices file (format: id name)
        edges_file: Path to edges file (format: src dst)
    
    Returns:
        GraphFrame object
    """
    print(f"Loading graph from files:")
    print(f"  Vertices: {vertices_file}")
    print(f"  Edges: {edges_file}")
    
    try:
        # Read vertices file
        vertices_data = []
        with open(vertices_file, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):  # Skip empty lines and comments
                    parts = line.split()  # Split by whitespace
                    if len(parts) >= 2:
                        vertex_id = parts[0].strip()
                        vertex_name = ' '.join(parts[1:]).strip()  # Join remaining parts as name
                        vertices_data.append((vertex_id, vertex_name, False, -1))
        
        # Read edges file
        edges_data = []
        with open(edges_file, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):  # Skip empty lines and comments
                    parts = line.split()  # Split by whitespace
                    if len(parts) >= 2:
                        src = parts[0].strip()
                        dst = parts[1].strip()
                        relationship = f"edge_{src}_{dst}"  # Auto-generate relationship name
                        edges_data.append((src, dst, relationship))
        
        if not vertices_data:
            raise ValueError(f"No valid vertices found in {vertices_file}")
        if not edges_data:
            raise ValueError(f"No valid edges found in {edges_file}")
        
        print(f"Loaded {len(vertices_data)} vertices and {len(edges_data)} edges")
        
        # Create DataFrames
        vertices = spark.createDataFrame(vertices_data, ["id", "name", "visited", "depth"])
        edges = spark.createDataFrame(edges_data, ["src", "dst", "relationship"])
        
        return GraphFrame(vertices, edges)
        
    except FileNotFoundError as e:
        raise FileNotFoundError(f"File not found: {e.filename}")
    except Exception as e:
        raise Exception(f"Error loading graph from files: {str(e)}")

def dfs_iterative(graph, start_node):
    """
    Perform iterative DFS traversal with depth tracking
    
    Args:
        graph: GraphFrame object
        start_node: Starting node ID for DFS
    
    Returns:
        List of tuples (node_id, depth, visit_order)
    """
    print(f"Starting DFS from node: {start_node}")
    print("=" * 50)
    
    # Initialize result tracking
    visit_order = []
    visited_nodes = set()
    
    # Stack for DFS: (node_id, depth)
    stack = [(start_node, 0)]
    
    # Get edges for adjacency lookup
    edges_df = graph.edges
    
    while stack:
        current_node, current_depth = stack.pop()
        
        if current_node not in visited_nodes:
            # Mark as visited
            visited_nodes.add(current_node)
            visit_order.append((current_node, current_depth, len(visit_order) + 1))
            
            # Output current node with depth
            print(f"Visiting Node: {current_node}, Depth: {current_depth}, Visit Order: {len(visit_order)}")
            
            # Get neighbors (outgoing edges from current node)
            neighbors = edges_df.filter(col("src") == current_node).select("dst").collect()
            
            # Add neighbors to stack in reverse order for consistent traversal
            neighbor_ids = [row.dst for row in neighbors]
            for neighbor in reversed(neighbor_ids):
                if neighbor not in visited_nodes:
                    stack.append((neighbor, current_depth + 1))
    
    print("=" * 50)
    print("DFS traversal completed!")
    return visit_order

def dfs_pregel_style(graph, start_node, max_iterations=10):
    """
    Perform DFS using Pregel-style message passing (alternative approach)
    
    Args:
        graph: GraphFrame object
        start_node: Starting node ID
        max_iterations: Maximum number of iterations
    
    Returns:
        Final graph with DFS results
    """
    print(f"Starting Pregel-style DFS from node: {start_node}")
    print("=" * 50)
    
    # Initialize vertices - set start node depth to 0, others to -1
    vertices_init = graph.vertices.withColumn(
        "depth", 
        when(col("id") == start_node, 0).otherwise(-1)
    ).withColumn(
        "visited",
        when(col("id") == start_node, True).otherwise(False)
    )
    
    current_graph = GraphFrame(vertices_init, graph.edges)
    
    for iteration in range(max_iterations):
        print(f"Iteration {iteration + 1}")
        
        # Get current active nodes (visited but neighbors not processed)
        active_nodes = current_graph.vertices.filter(
            (col("visited") == True) & (col("depth") >= 0)
        )
        
        if active_nodes.count() == 0:
            break
        
        # Send messages to neighbors
        messages = current_graph.edges.join(
            active_nodes.select("id", "depth"), 
            col("src") == col("id")
        ).select(
            col("dst").alias("id"),
            (col("depth") + 1).alias("new_depth")
        )
        
        # Update unvisited neighbors
        updated_vertices = current_graph.vertices.join(
            messages, ["id"], "left"
        ).withColumn(
            "depth",
            when(
                (col("visited") == False) & (col("new_depth").isNotNull()),
                col("new_depth")
            ).otherwise(col("depth"))
        ).withColumn(
            "visited",
            when(
                (col("visited") == False) & (col("new_depth").isNotNull()),
                True
            ).otherwise(col("visited"))
        ).drop("new_depth")
        
        # Check if any updates were made
        new_visited_count = updated_vertices.filter(col("visited") == True).count()
        old_visited_count = current_graph.vertices.filter(col("visited") == True).count()
        
        if new_visited_count == old_visited_count:
            print("No new nodes to visit. DFS completed!")
            break
        
        # Update graph
        current_graph = GraphFrame(updated_vertices, current_graph.edges)
        
        # Show newly visited nodes
        newly_visited = updated_vertices.filter(
            (col("visited") == True) & (col("depth") == iteration + 1)
        ).orderBy("id")
        
        print(f"Nodes visited at depth {iteration + 1}:")
        for row in newly_visited.collect():
            print(f"  Node: {row.id}, Name: {row.name}, Depth: {row.depth}")
    
    print("=" * 50)
    return current_graph

def analyze_dfs_results(visit_order):
    """Analyze and display DFS traversal results"""
    print("\nDFS Traversal Analysis:")
    print("=" * 50)
    
    print(f"Total nodes visited: {len(visit_order)}")
    print(f"Maximum depth reached: {max([depth for _, depth, _ in visit_order])}")
    
    # Group by depth
    depth_groups = {}
    for node_id, depth, order in visit_order:
        if depth not in depth_groups:
            depth_groups[depth] = []
        depth_groups[depth].append((node_id, order))
    
    print("\nNodes by depth level:")
    for depth in sorted(depth_groups.keys()):
        nodes = depth_groups[depth]
        node_info = [f"{node}(#{order})" for node, order in nodes]
        print(f"  Depth {depth}: {', '.join(node_info)}")

def visualize_graph_structure(graph):
    """Display graph structure information"""
    print("\nGraph Structure:")
    print("=" * 50)
    
    vertices_count = graph.vertices.count()
    edges_count = graph.edges.count()
    
    print(f"Vertices: {vertices_count}")
    print(f"Edges: {edges_count}")
    
    print("\nVertices:")
    graph.vertices.select("id", "name").orderBy("id").show(truncate=False)
    
    print("Edges:")
    graph.edges.select("src", "dst", "relationship").orderBy("src", "dst").show(truncate=False)

def main():
    """Main function"""
    if len(sys.argv) < 3:
        print("Usage: python graph_dfs.py <vertices_file> <edges_file> [start_node]")
        print()
        print("Arguments:")
        print("  vertices_file    - Path to vertices file (format: id,name)")
        print("  edges_file      - Path to edges file (format: src,dst,relationship)")
        print("  start_node      - Starting node for DFS (optional, defaults to first vertex)")
        print()
        print("File formats:")
        print("  Vertices file: Each line should contain 'id,name'")
        print("  Edges file: Each line should contain 'src,dst,relationship' (relationship is optional)")
        print()
        print("Examples:")
        print("  python graph_dfs.py sample_vertices.txt sample_edges.txt A")
        print("  python graph_dfs.py my_vertices.csv my_edges.csv")
        sys.exit(1)
    
    vertices_file = sys.argv[1]
    edges_file = sys.argv[2]
    start_node = sys.argv[3] if len(sys.argv) > 3 else None
    
    # Create Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        # Load graph from files
        graph = load_graph_from_files(spark, vertices_file, edges_file)
        
        # If no start node specified, use the first vertex
        if not start_node:
            first_vertex = graph.vertices.select("id").first()
            start_node = first_vertex.id
            print(f"No start node specified, using first vertex: {start_node}")
        
        # Validate start node exists
        vertex_exists = graph.vertices.filter(col("id") == start_node).count() > 0
        if not vertex_exists:
            available_vertices = [row.id for row in graph.vertices.select("id").collect()]
            print(f"Error: Start node '{start_node}' not found in graph")
            print(f"Available vertices: {available_vertices}")
            sys.exit(1)
        
        print("Graph loaded successfully!")
        visualize_graph_structure(graph)
        
        # Perform iterative DFS
        print("\n" + "="*60)
        print("ITERATIVE DFS TRAVERSAL")
        print("="*60)
        visit_order = dfs_iterative(graph, start_node)
        analyze_dfs_results(visit_order)
        
        # Perform Pregel-style DFS
        print("\n" + "="*60)
        print("PREGEL-STYLE DFS TRAVERSAL")
        print("="*60)
        final_graph = dfs_pregel_style(graph, start_node)
        
        print("\nFinal DFS Results:")
        final_graph.vertices.filter(col("visited") == True).orderBy("depth", "id").show()

    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()
        print("\nSpark session closed")

if __name__ == "__main__":
    main() 