"""
Intermediate representation models for Talend components.

This module defines simple data structures used to represent Talend jobs as a
directed acyclic graph (DAG).  Each component in a job is represented as a
``Node`` with an id, type, name and arbitrary configuration.  Data flows
between components are encoded as ``Edge`` instances connecting source and
target nodes.  The ``Graph`` class provides a ``topological_order`` method
which returns the nodes sorted according to their dependencies.  This is
crucial for generating code in the correct order.
"""

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
        """Return nodes in a topological order.

        This simple Kahn's algorithm implementation collects nodes with zero
        incoming edges, processes them and enqueues any neighbours whose
        indegree drops to zero.  If there are cycles or disconnected nodes,
        a ValueError is raised.
        """
        indeg = {n: 0 for n in self.nodes}
        for e in self.edges:
            indeg[e.target] += 1
        queue = [n for n, d in indeg.items() if d == 0]
        order: List[Node] = []
        while queue:
            nid = queue.pop(0)
            order.append(self.nodes[nid])
            # decrement indegree of downstream neighbours
            for e in [x for x in self.edges if x.source == nid]:
                indeg[e.target] -= 1
                if indeg[e.target] == 0:
                    queue.append(e.target)
        if len(order) != len(self.nodes):
            raise ValueError("Graph has cycles or is disconnected.")
        return order