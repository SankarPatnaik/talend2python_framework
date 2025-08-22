import textwrap
from talend2python.parsers.talend_xml_parser import parse_talend_item


def test_parse_connections_using_component_names(tmp_path):
    xml = textwrap.dedent(
        """
        <talendJob name="Sample">
          <components>
            <component id="n1" type="tMap" name="tMap_1" />
            <component id="n2" type="tFileOutputDelimited" name="tFileOutputDelimited_1" />
          </components>
          <connections>
            <connection source="tMap_1" target="tFileOutputDelimited_1" />
          </connections>
        </talendJob>
        """
    )
    p = tmp_path / "job.item"
    p.write_text(xml)
    g = parse_talend_item(str(p))
    # Edges should resolve to component ids even though connections used names
    assert g.edges[0].source == "n1"
    assert g.edges[0].target == "n2"
    # Topological order should include both nodes without error
    order = [n.id for n in g.topological_order()]
    assert order == ["n1", "n2"]

