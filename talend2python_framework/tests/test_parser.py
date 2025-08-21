from talend2python.parsers.talend_xml_parser import parse_talend_item

def test_parse_basic_graph():
    g = parse_talend_item("examples/jobs/sample_talend_job/sample_job.item")
    assert len(g.nodes) == 5
    assert len(g.edges) == 4
    order = g.topological_order()
    assert order[0].type == "tFileInputDelimited"
