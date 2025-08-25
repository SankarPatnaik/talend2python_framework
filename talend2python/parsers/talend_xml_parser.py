"""
Parser for Talend .item files into an intermediate representation.

Talend jobs are stored as XML documents.  This module uses lxml to parse
the XML and convert it into the IR defined in ``talend2python.ir.model``.
Components become ``Node`` instances and connections between them become
``Edge`` instances.  Each node's configuration parameters are stored as
strings in the ``config`` dictionary.
"""

from pathlib import Path
from typing import Union

from lxml import etree
import yaml

from ..ir.model import Edge, Graph, Node


# Mapping of elementParameter names used by Talend to the keys expected in
# the Node configuration dictionaries used throughout this project.
_KEY_MAP = {
    "FILENAME": "file_path",
    "FIELDSEPARATOR": "separator",
    "HEADER": "header",
    # Additional parameters for extended component coverage
    "SHEET": "sheet",
    "GROUP_BY": "group_by",
    "AGGREGATIONS": "aggregations",
    "COLUMN": "column",
    "NEW_COLUMNS": "new_columns",
    "NUM_ROWS": "num_rows",
    "SCHEMA": "schema",
}


def _load_component_key_map() -> None:
    """Extend ``_KEY_MAP`` with parameter names from ``component_map.yaml``.

    The YAML mapping defines the public configuration key for each Talend
    parameter.  To make new parameters automatically available in
    :class:`Node` objects, this function reads the mapping file and adds a
    fallback entry mapping the upper-case parameter name to the desired key.
    Existing explicit mappings are left untouched.
    """

    mapping_path = (
        Path(__file__).resolve().parent.parent / "mapping" / "component_map.yaml"
    )
    try:
        data = yaml.safe_load(mapping_path.read_text())
    except FileNotFoundError:
        return

    for comp in data.values():
        for _, key in comp.get("params", {}).items():
            # Only add if not explicitly mapped already
            _KEY_MAP.setdefault(key.upper(), key)


# Populate the key map on import so the parser picks up new parameters
_load_component_key_map()


def parse_talend_item(source: Union[str, Path]) -> Graph:
    """Parse a Talend job definition and return a ``Graph`` representation.

    ``source`` may either be a filesystem path to a ``.item`` file or a raw
    XML string containing the job definition.  When a path is supplied it must
    point to an existing file; otherwise the value is treated as XML content.
    """

    if isinstance(source, (str, Path)) and Path(source).exists():
        tree = etree.parse(str(source))
        root = tree.getroot()
    else:
        root = etree.fromstring(str(source))
    g = Graph()

    # Create nodes from both legacy <component> elements and modern <node>
    # elements.  Older sample jobs bundled with this repository use
    # ``<component>`` while real Talend jobs found in the wild typically use
    # ``<node>`` with nested ``<elementParameter>`` children.  Support both in
    # order to handle a wide range of job definitions.

    # First parse any <component> elements.
    for comp in root.findall(".//component"):
        # Talend components expose several identifying attributes depending on
        # how the job XML was produced.  Real jobs often omit an explicit ``id``
        # and instead rely on a human readable ``label`` such as
        # ``tMap_1``/``tMap_2`` while the ``name`` attribute only describes the
        # component type (e.g. ``tMap").  If we keyed nodes only by ``name`` we
        # would collapse multiple components with the same type and later fail
        # to resolve connections.  Prefer ``id`` when present, fall back to the
        # unique ``label`` and finally to ``name``.
        nid = comp.get("id") or comp.get("label") or comp.get("name")
        # ``type`` is used in the simplified example jobs but real Talend items
        # may use ``componentName`` instead.
        ntype = comp.get("type") or comp.get("componentName")
        # Preserve a display name for later lookups; again prefer ``label``
        # because it tends to be unique.
        name = comp.get("label") or comp.get("name", ntype)
        cfg = {}
        for c in comp.findall("./config/param"):
            k = c.get("name")
            v = c.get("value")
            cfg[k] = v
        g.nodes[nid] = Node(id=nid, type=ntype, name=name, config=cfg)

    # Next parse <node> elements used by more recent Talend versions.  These
    # store most of their metadata inside ``<elementParameter>`` children.
    for node in root.findall(".//node"):
        unique = node.find("./elementParameter[@name='UNIQUE_NAME']")
        if unique is None:
            # Without a unique name we cannot address the node, so skip it.
            continue
        nid = unique.get("value")
        ntype = node.get("componentName")

        label = node.find("./elementParameter[@name='LABEL']")
        name = label.get("value") if label is not None else nid

        cfg = {}
        for p in node.findall("./elementParameter[@value]"):
            key = p.get("name")
            val = p.get("value")
            key = _KEY_MAP.get(key, key)
            cfg[key] = val

        g.nodes[nid] = Node(id=nid, type=ntype, name=name, config=cfg)

    # Map component display names to their ids for resolving connections
    name_to_id = {n.name: n.id for n in g.nodes.values()}

    # Create edges, resolving names to ids when necessary.  Preserve the Talend
    # connector name (e.g. FLOW, FILTER, REJECT) so downstream code can
    # differentiate between normal data flow and control links such as
    # COMPONENT_OK or RUN_IF.
    for conn in root.findall(".//connection"):
        src = conn.get("source")
        tgt = conn.get("target")
        connector = conn.get("connectorName", "FLOW")
        src_id = src if src in g.nodes else name_to_id.get(src)
        tgt_id = tgt if tgt in g.nodes else name_to_id.get(tgt)
        if src_id is None or tgt_id is None:
            raise ValueError(
                f"Connection references unknown nodes: {src!r} -> {tgt!r}"
            )
        g.edges.append(Edge(source=src_id, target=tgt_id, connector=connector))

    # Collect any declared routines so they can be made available during code
    # generation.  Routines are stored by name only; the actual implementation
    # is expected to be provided separately in Python modules.
    for r in root.findall(".//routinesParameter"):
        name = r.get("name")
        if name:
            g.routines.append(name)

    return g
