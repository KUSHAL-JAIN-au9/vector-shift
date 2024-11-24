from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List
from collections import defaultdict, deque
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins. Change to specific domains if needed.
    allow_credentials=True,
    allow_methods=["*"],  # Allow all HTTP methods (GET, POST, etc.)
    allow_headers=["*"],  # Allow all headers
)

# hi

@app.get("/")
def read_root():
    return {"message": "Hello, FastAPI!"}

# Define data model
class PipelineData(BaseModel):
    nodes: List[dict]
    edges: List[dict]

# Helper function to check if a graph is a DAG
def is_dag(nodes, edges):
    # Build adjacency list
    adjacency_list = defaultdict(list)
    in_degree = {node["id"]: 0 for node in nodes}

    for edge in edges:
        source, target = edge["source"], edge["target"]
        adjacency_list[source].append(target)
        in_degree[target] += 1

    # Perform topological sort using Kahn's Algorithm
    queue = deque([node for node in in_degree if in_degree[node] == 0])
    sorted_nodes = []

    while queue:
        current = queue.popleft()
        sorted_nodes.append(current)

        for neighbor in adjacency_list[current]:
            in_degree[neighbor] -= 1
            if in_degree[neighbor] == 0:
                queue.append(neighbor)

    # If the number of sorted nodes equals the total number of nodes, it's a DAG
    return len(sorted_nodes) == len(nodes)

# Define /pipelines/parse endpoint
@app.post("/pipelines/parse")
def parse_pipeline(pipeline: PipelineData):
    nodes = pipeline.nodes
    edges = pipeline.edges

    # Calculate number of nodes and edges
    num_nodes = len(nodes)
    num_edges = len(edges)

    # Check if the graph is a DAG
    is_dag_result = is_dag(nodes, edges)

    # Return the response
    return {"num_nodes": num_nodes, "num_edges": num_edges, "is_dag": is_dag_result}
