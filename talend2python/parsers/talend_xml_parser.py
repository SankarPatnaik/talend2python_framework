"""
Parser for Talend .item files into an intermediate representation.

Talend jobs are stored as XML documents.  This module uses lxml to parse
the XML and convert it into the IR defined in ``talend2python.ir.model``.
Components become ``Node`` instances and connections between them become
``Edge`` instances.  Each node's configuration parameters are stored as
strings in the ``config`` dictionary.
"""

from lxml import etree
from ..ir.model import Graph, Node, Edge


def parse_talend_item(path: str) -> Graph:
    """Parse a Talend .item file and return a Graph representation."""
    tree = etree.parse(path)
    root = tree.getroot()
    g = Graph()

    # Create nodes
    for comp in root.findall(".//component"):
        nid = comp.get("id")
        ntype = comp.get("type")
        name = comp.get("name", ntype)
        cfg = {}
        for c in comp.findall("./config/param"):
            k = c.get("name")
            v = c.get("value")
            cfg[k] = v
        g.nodes[nid] = Node(id=nid, type=ntype, name=name, config=cfg)

    # Create edges
    for conn in root.findall(".//connection"):
        src = conn.get("source")
        tgt = conn.get("target")
        g.edges.append(Edge(source=src, target=tgt))

    return g