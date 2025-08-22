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
        # Talend components often have an internal id and a user facing name.
        # Connections may reference either, so keep the id as the primary key
        # but record the display name for later resolution.
        nid = comp.get("id") or comp.get("name")
        ntype = comp.get("type")
        name = comp.get("name", ntype)
        cfg = {}
        for c in comp.findall("./config/param"):
            k = c.get("name")
            v = c.get("value")
            cfg[k] = v
        g.nodes[nid] = Node(id=nid, type=ntype, name=name, config=cfg)

    # Map component display names to their ids for resolving connections
    name_to_id = {n.name: n.id for n in g.nodes.values()}

    # Create edges, resolving names to ids when necessary
    for conn in root.findall(".//connection"):
        src = conn.get("source")
        tgt = conn.get("target")
        src_id = src if src in g.nodes else name_to_id.get(src)
        tgt_id = tgt if tgt in g.nodes else name_to_id.get(tgt)
        if src_id is None or tgt_id is None:
            raise ValueError(
                f"Connection references unknown nodes: {src!r} -> {tgt!r}"
            )
        g.edges.append(Edge(source=src_id, target=tgt_id))

    return g
