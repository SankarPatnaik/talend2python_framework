import pytest
from talend2python.ir.model import Graph, Node, Edge


def test_topological_order_disconnected():
    g = Graph(
        nodes={
            "a": Node(id="a", type="t", name="A"),
            "b": Node(id="b", type="t", name="B"),
        },
        edges=[],
    )
    with pytest.raises(ValueError, match="Graph is disconnected"):
        g.topological_order()


def test_topological_order_cycle():
    g = Graph(
        nodes={
            "a": Node(id="a", type="t", name="A"),
            "b": Node(id="b", type="t", name="B"),
        },
        edges=[Edge(source="a", target="b"), Edge(source="b", target="a")],
    )
    with pytest.raises(ValueError, match="Graph has cycles"):
        g.topological_order()
