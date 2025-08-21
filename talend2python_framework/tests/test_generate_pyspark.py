from talend2python.parsers.talend_xml_parser import parse_talend_item
from talend2python.generators.pyspark_generator import generate
from pathlib import Path

def test_codegen(tmp_path):
    g = parse_talend_item("examples/jobs/sample_talend_job/sample_job.item")
    out = tmp_path / "code"
    generate(g, out)
    assert (out / "main.py").exists()
