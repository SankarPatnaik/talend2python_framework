import pytest
from talend2python.ir.model import Graph, Node, Edge

def test_topological_order_disconnected():
    g = Graph(
        nodes={
            'a': Node(id='a', type='t', name='A'),
            'b': Node(id='b', type='t', name='B')
        },
        edges=[]
    )
    with pytest.raises(ValueError):
        g.topological_order()
