#!/bin/bash

# Simple DFS Runner Script
echo "Simple Spark DFS Algorithm Runner"
echo "================================="

# Default start node
START_NODE=${1:-A}

echo "Starting DFS from node: $START_NODE"
echo "Graph structure: A->B,C; B->D,E; C->F; D->G; E->H; F->I"
echo ""

# Run the simple DFS
python3 simple_graph_dfs.py iterative "$START_NODE" 