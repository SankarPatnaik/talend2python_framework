from pathlib import Path
from talend2python.parsers.talend_xml_parser import parse_talend_item

EXAMPLE_ITEM = (
    Path(__file__).resolve().parents[1]
    / "examples/jobs/sample_talend_job/ProductSampleJob_0.1.item"
)


def test_connector_types_preserved():
    g = parse_talend_item(str(EXAMPLE_ITEM))
    # Map (source,target) -> connector for easier assertions
    edge_types = {(e.source, e.target): e.connector for e in g.edges}
    assert edge_types[("tFileInputDelimited_1", "tJavaRow_1")] == "FLOW"
    assert edge_types[("tJavaRow_1", "tFilterRow_1")] == "FLOW"
    assert edge_types[("tFilterRow_1", "tJavaRow_2")] == "FILTER"
    assert edge_types[("tFilterRow_1", "tJavaRow_3")] == "REJECT"

