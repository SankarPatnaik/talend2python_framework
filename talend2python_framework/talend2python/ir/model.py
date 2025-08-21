from dataclasses import dataclass, field
from typing import Dict, List, Any

@dataclass
class Node:
    id: str
    type: str
    name: str
    config: Dict[str, Any] = field(default_factory=dict)

@dataclass
class Edge:
    source: str
    target: str

@dataclass
class Graph:
    nodes: Dict[str, Node] = field(default_factory=dict)
    edges: List[Edge] = field(default_factory=list)

    def topological_order(self) -> List[Node]:
        indeg = {n:0 for n in self.nodes}
        for e in self.edges:
            indeg[e.target] += 1
        queue = [n for n, d in indeg.items() if d == 0]
        order = []
        while queue:
            nid = queue.pop(0)
            order.append(self.nodes[nid])
            for e in [x for x in self.edges if x.source == nid]:
                indeg[e.target] -= 1
                if indeg[e.target] == 0:
                    queue.append(e.target)
        if len(order) != len(self.nodes):
            raise ValueError("Graph has cycles or is disconnected.")
        return order
