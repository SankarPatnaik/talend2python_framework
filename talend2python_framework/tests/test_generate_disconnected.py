from talend2python.ir.model import Graph, Node
from talend2python.generators.pandas_generator import generate as gen_pandas
from talend2python.generators.pyspark_generator import generate as gen_pyspark


def _build_disconnected_graph():
    return Graph(
        nodes={
            "a": Node(id="a", type="t", name="A"),
            "b": Node(id="b", type="t", name="B"),
        },
        edges=[],
    )


def test_generate_pandas_disconnected(tmp_path):
    g = _build_disconnected_graph()
    out = tmp_path / "pandas"
    gen_pandas(g, out)
    assert (out / "main.py").exists()


def test_generate_pyspark_disconnected(tmp_path):
    g = _build_disconnected_graph()
    out = tmp_path / "pyspark"
    gen_pyspark(g, out)
    assert (out / "main.py").exists()

