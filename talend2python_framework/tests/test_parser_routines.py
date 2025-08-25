from pathlib import Path

from talend2python.parsers.talend_xml_parser import parse_talend_item


ROUTINE_ITEM = (
    Path(__file__).resolve().parents[1]
    / "examples/jobs/sample_talend_job/ProductSampleJob_0.1.item"
)


def test_parse_routines():
    g = parse_talend_item(str(ROUTINE_ITEM))
    assert "TalendDate" in g.routines
    assert "StringHandling" in g.routines

