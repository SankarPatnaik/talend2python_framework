import textwrap
from talend2python.parsers.talend_xml_parser import parse_talend_item


def test_parse_node_elements(tmp_path):
    xml = textwrap.dedent(
        """
        <talendfile:ProcessType xmlns:talendfile="http://www.talend.com">
          <node componentName="tFileInputDelimited">
            <elementParameter name="UNIQUE_NAME" value="tFileInputDelimited_1"/>
            <elementParameter name="LABEL" value="InputCsv"/>
            <elementParameter name="FILENAME" value="input.csv"/>
            <elementParameter name="FIELDSEPARATOR" value=";"/>
            <elementParameter name="HEADER" value="1"/>
          </node>
          <node componentName="tLogRow">
            <elementParameter name="UNIQUE_NAME" value="tLogRow_1"/>
            <elementParameter name="LABEL" value="LogRow"/>
          </node>
          <connection source="tFileInputDelimited_1" target="tLogRow_1" />
        </talendfile:ProcessType>
        """
    )
    path = tmp_path / "job.item"
    path.write_text(xml)
    g = parse_talend_item(str(path))

    assert "tFileInputDelimited_1" in g.nodes
    node = g.nodes["tFileInputDelimited_1"]
    assert node.type == "tFileInputDelimited"
    assert node.name == "InputCsv"
    assert node.config["file_path"] == "input.csv"
    assert node.config["separator"] == ";"
    assert node.config["header"] == "1"

    # Connections reference the UNIQUE_NAME values directly
    assert g.edges[0].source == "tFileInputDelimited_1"
    assert g.edges[0].target == "tLogRow_1"
