"""
Generator to produce a pandas implementation of a Talend job.

This module walks the intermediate representation (IR) graph and collects
information about each node: its id, type, name, configuration and the ids of
any predecessor nodes. The information is fed into a Jinja2 template which
emits runnable Python code using the pandas library.

The support for multi‑input components (such as tJoin) relies on the
``inputs`` field: a list of upstream node ids derived from the graph's edges.
"""

from jinja2 import Environment, FileSystemLoader, StrictUndefined
from pathlib import Path


def generate(graph, out_dir):
    """Render a pandas implementation for a given Talend graph.

    Parameters
    ----------
    graph : talend2python.ir.model.Graph
        A directed acyclic graph representing the Talend job. Each node in the
        graph corresponds to a Talend component and edges encode the flow of
        data between components.
    out_dir : str or Path
        Path where the generated ``main.py`` file will be written.

    Returns
    -------
    dict
        Metadata about the generated engine and files.
    """
    # Load templates from the local ``templates`` directory.  Using
    # ``StrictUndefined`` forces the template to raise an error if a variable
    # is missing, helping catch mistakes early.
    templates_path = Path(__file__).resolve().parent.parent / "templates"
    env = Environment(
        loader=FileSystemLoader(str(templates_path)),
        undefined=StrictUndefined,
        trim_blocks=True,
        lstrip_blocks=True,
    )
    main_tpl = env.get_template("main_pandas.py.j2")

    # Collect node information along with input dependencies.  The IR graph
    # exposes a list of edges, where each edge has a ``source`` and ``target``
    # attribute.  For each node we look up all incoming edges to determine
    # which other dataframes should be available when rendering code for that
    # node.  Nodes with no inputs (e.g. file inputs) will have an empty list.
    steps = []
    for node in graph.topological_order():
        cfg = node.config or {}
        inputs = [edge.source for edge in graph.edges if edge.target == node.id]
        steps.append(
            {
                "id": node.id,
                "type": node.type,
                "name": node.name,
                "config": cfg,
                "inputs": inputs,
            }
        )

    # Render the code and write it to the output directory.
    code = main_tpl.render(steps=steps)
    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)
    (out_path / "main.py").write_text(code, encoding="utf-8")
    return {"engine": "pandas", "files": ["main.py"]}