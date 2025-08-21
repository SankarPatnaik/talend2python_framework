"""
Generator to produce a PySpark implementation of a Talend job.

Much like the pandas generator, this module gathers information about each
component in the intermediate representation (IR) graph including the ids
of any upstream nodes.  It then feeds this information into a Jinja2
template which emits runnable PySpark code.  The resulting script reads
inputs, performs filtering, mapping, joining and logging, and writes
outputs using Spark's DataFrame API.
"""

from jinja2 import Environment, FileSystemLoader, StrictUndefined
from pathlib import Path


def generate(graph, out_dir):
    """Render a PySpark implementation for a given Talend graph.

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
    templates_path = Path(__file__).resolve().parent.parent / "templates"
    env = Environment(
        loader=FileSystemLoader(str(templates_path)),
        undefined=StrictUndefined,
        trim_blocks=True,
        lstrip_blocks=True,
    )
    main_tpl = env.get_template("main_pyspark.py.j2")

    # Build the steps list with input dependencies.  This mirrors the
    # implementation in the pandas generator but returns a PySpark specific
    # template.
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

    code = main_tpl.render(steps=steps)
    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)
    (out_path / "main.py").write_text(code, encoding="utf-8")
    return {"engine": "pyspark", "files": ["main.py"]}