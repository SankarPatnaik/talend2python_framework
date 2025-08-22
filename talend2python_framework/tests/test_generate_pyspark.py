from talend2python.parsers.talend_xml_parser import parse_talend_item
from talend2python.generators.pyspark_generator import generate
from pathlib import Path

EXAMPLE_ITEM = Path(__file__).resolve().parents[1] / "examples/jobs/sample_talend_job/sample_job.item"

def test_codegen(tmp_path):
    g = parse_talend_item(str(EXAMPLE_ITEM))
    out = tmp_path / "code"
    generate(g, out)
    assert (out / "main.py").exists()
