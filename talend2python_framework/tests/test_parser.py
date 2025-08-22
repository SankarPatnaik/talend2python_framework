from talend2python.parsers.talend_xml_parser import parse_talend_item
from pathlib import Path

EXAMPLE_ITEM = Path(__file__).resolve().parents[1] / "examples/jobs/sample_talend_job/sample_job.item"

def test_parse_basic_graph():
    g = parse_talend_item(str(EXAMPLE_ITEM))
    assert len(g.nodes) == 5
    assert len(g.edges) == 4
    order = g.topological_order()
    assert order[0].type == "tFileInputDelimited"
