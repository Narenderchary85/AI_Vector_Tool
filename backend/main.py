import uvicorn
from fastapi import FastAPI
from pydantic import BaseModel
from typing import List, Dict
from fastapi.middleware.cors import CORSMiddleware
from collections import defaultdict

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ------------------ Models ------------------

class Edge(BaseModel):
    source: str
    target: str

class Pipeline(BaseModel):
    nodes: List[Dict]
    edges: List[Edge]

# ------------------ DAG CHECK ------------------

def is_dag(nodes, edges):
    graph = defaultdict(list)

    # 1️⃣ Initialize all nodes
    for node in nodes:
        graph[node["id"]] = []

    # 2️⃣ Add edges safely
    for edge in edges:
        if edge.source in graph:
            graph[edge.source].append(edge.target)

    visited = set()
    rec_stack = set()

    def dfs(node):
        if node in rec_stack:
            return False
        if node in visited:
            return True

        visited.add(node)
        rec_stack.add(node)

        for neighbor in graph[node]:
            if neighbor in graph:   # safety check
                if not dfs(neighbor):
                    return False

        rec_stack.remove(node)
        return True

    return all(dfs(node["id"]) for node in nodes)

# ------------------ Routes ------------------

@app.get("/")
def read_root():
    return {"Ping": "Pong"}

@app.post("/pipelines/parse")
def parse_pipeline(pipeline: Pipeline):
    return {
        "num_nodes": len(pipeline.nodes),
        "num_edges": len(pipeline.edges),
        "is_dag": is_dag(pipeline.nodes, pipeline.edges)
    }

# ------------------ Run ------------------

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
