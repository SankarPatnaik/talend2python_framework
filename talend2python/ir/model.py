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

    def topological_order(self, require_connected: bool = True) -> List[Node]:
        """Return nodes in a topological order.

        Parameters
        ----------
        require_connected:
            When ``True`` (the default), the graph is required to be weakly
            connected and a :class:`ValueError` is raised otherwise.  When set
            to ``False`` the connectivity check is skipped and nodes from all
            disconnected components are returned.

        This simple Kahn's algorithm implementation collects nodes with zero
        incoming edges, processes them and enqueues any neighbours whose
        indegree drops to zero.  If there are cycles a ``ValueError`` is
        raised.  When ``require_connected`` is ``True`` an additional
        breadth‑first search ensures that all nodes are reachable.
        """
        indeg = {n: 0 for n in self.nodes}
        for e in self.edges:
            if e.source not in indeg or e.target not in indeg:
                raise ValueError(
                    f"Edge references unknown node: {e.source!r} -> {e.target!r}"
                )
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
            raise ValueError("Graph has cycles")

        # Ensure the graph is weakly connected if requested.  The simple
        # topological sort above will happily return an order even if the graph
        # consists of multiple disconnected components because all nodes start
        # with an indegree of zero.  When ``require_connected`` is ``True`` we
        # traverse the graph ignoring edge direction and verify that all nodes
        # are reachable from the first node.  If not, the graph is considered
        # disconnected.
        if require_connected and self.nodes:
            from collections import deque

            start = next(iter(self.nodes))
            q: deque[str] = deque([start])
            seen = set()
            while q:
                cur = q.popleft()
                if cur in seen:
                    continue
                seen.add(cur)
                neighbours = [e.target for e in self.edges if e.source == cur]
                neighbours += [e.source for e in self.edges if e.target == cur]
                for n in neighbours:
                    if n not in seen:
                        q.append(n)
            if len(seen) != len(self.nodes):
                raise ValueError("Graph is disconnected")

        return order
