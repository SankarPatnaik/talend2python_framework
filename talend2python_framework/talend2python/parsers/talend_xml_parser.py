from lxml import etree
from ..ir.model import Graph, Node, Edge

def parse_talend_item(path: str) -> Graph:
    tree = etree.parse(path)
    root = tree.getroot()
    g = Graph()

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

    for conn in root.findall(".//connection"):
        src = conn.get("source")
        tgt = conn.get("target")
        g.edges.append(Edge(source=src, target=tgt))

    return g
