import textwrap

from talend2python.parsers.talend_xml_parser import parse_talend_item


def test_parse_components_with_duplicate_names(tmp_path):
    xml = textwrap.dedent(
        """
        <talendJob name="Sample">
          <components>
            <component type="tMap" label="tMap_1" />
            <component type="tMap" label="tMap_2" />
          </components>
          <connections>
            <connection source="tMap_1" target="tMap_2" />
          </connections>
        </talendJob>
        """
    )
    path = tmp_path / "job.item"
    path.write_text(xml)
    graph = parse_talend_item(str(path))

    # Both components should be present using their label as the id
    assert set(graph.nodes) == {"tMap_1", "tMap_2"}

    # Connections using the labels should resolve correctly
    assert graph.edges[0].source == "tMap_1"
    assert graph.edges[0].target == "tMap_2"

