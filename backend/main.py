import uvicorn
from fastapi import FastAPI
from pydantic import BaseModel
from typing import List, Dict
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

class Edge(BaseModel):
    source: str
    target: str

class Pipeline(BaseModel):
    nodes: List[Dict]
    edges: List[Edge]

def is_dag(nodes, edges):
    graph = {node["id"]: [] for node in nodes}

    for edge in edges:
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
            if not dfs(neighbor):
                return False

        rec_stack.remove(node)
        return True

    return all(dfs(node["id"]) for node in nodes)

@app.get("/")
def read_root():
    return {"Ping": "Pong"}

@app.post("/pipelines/parse")
def parse_pipeline(pipeline: Pipeline):
    num_nodes = len(pipeline.nodes)
    num_edges = len(pipeline.edges)
    dag = is_dag(pipeline.nodes, pipeline.edges)

    return {
        "num_nodes": num_nodes,
        "num_edges": num_edges,
        "is_dag": dag
    }

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
