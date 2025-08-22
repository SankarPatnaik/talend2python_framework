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


def test_topological_order_disconnected_allowed():
    g = Graph(
        nodes={
            "a1": Node(id="a1", type="t", name="A1"),
            "a2": Node(id="a2", type="t", name="A2"),
            "b1": Node(id="b1", type="t", name="B1"),
            "b2": Node(id="b2", type="t", name="B2"),
        },
        edges=[Edge(source="a1", target="a2"), Edge(source="b1", target="b2")],
    )
    order = g.topological_order(require_connected=False)
    ids = [n.id for n in order]
    assert set(ids) == {"a1", "a2", "b1", "b2"}
    assert ids.index("a1") < ids.index("a2")
    assert ids.index("b1") < ids.index("b2")


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
