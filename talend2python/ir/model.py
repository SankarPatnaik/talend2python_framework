"""
Intermediate representation models for Talend components.

This module defines simple data structures used to represent Talend jobs as a
directed acyclic graph (DAG).  Each component in a job is represented as a
``Node`` with an id, type, name and arbitrary configuration.  Connections
between components are encoded as ``Edge`` instances linking source and target
nodes.  ``Edge`` objects also capture the Talend ``connector`` type (e.g.
``FLOW``/``REJECT``), allowing the framework to preserve flow‑control semantics
such as "on component ok" without additional coding.  The ``Graph`` class
provides a ``topological_order`` method which returns the nodes sorted according
to their dependencies.  Nodes expose ``inputs`` and ``outputs`` lists which are
automatically populated from the graph's edges, allowing multi‑layer Talend jobs
to be represented without extra wiring.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Any


@dataclass
class Node:
    id: str
    type: str
    name: str
    config: Dict[str, Any] = field(default_factory=dict)
    inputs: List[str] = field(default_factory=list)
    outputs: List[str] = field(default_factory=list)


@dataclass
class Edge:
    """Connection between two nodes.

    The ``connector`` attribute stores the Talend connector type (e.g. ``FLOW``,
    ``FILTER``, ``REJECT``).  It defaults to ``"FLOW"`` so existing tests and
    simple jobs that omit an explicit connector continue to work unchanged.
    """

    source: str
    target: str
    connector: str = "FLOW"


@dataclass
class Graph:
    nodes: Dict[str, Node] = field(default_factory=dict)
    edges: List[Edge] = field(default_factory=list)

    def add_node(self, node: Node) -> None:
        """Add ``node`` to the graph."""
        self.nodes[node.id] = node

    def add_edge(self, edge: Edge) -> None:
        """Add ``edge`` and update input/output lists on the connected nodes."""
        if edge.source not in self.nodes or edge.target not in self.nodes:
            raise ValueError(
                f"Edge references unknown node: {edge.source!r} -> {edge.target!r}"
            )
        self.edges.append(edge)
        self.nodes[edge.target].inputs.append(edge.source)
        self.nodes[edge.source].outputs.append(edge.target)

    def _rebuild_links(self) -> None:
        """Recompute ``inputs`` and ``outputs`` lists from the current edges.

        This is useful when edges are appended directly to ``self.edges``
        instead of using :meth:`add_edge`, such as in tests or older code.
        """
        for n in self.nodes.values():
            n.inputs.clear()
            n.outputs.clear()
        for e in self.edges:
            if e.target in self.nodes:
                self.nodes[e.target].inputs.append(e.source)
            if e.source in self.nodes:
                self.nodes[e.source].outputs.append(e.target)

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
        # Ensure ``inputs``/``outputs`` lists reflect current edges before
        # performing the sort.  This keeps ``Node`` information in sync even
        # when edges are manipulated directly.
        self._rebuild_links()

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
