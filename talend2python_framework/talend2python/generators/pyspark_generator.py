from jinja2 import Environment, FileSystemLoader, StrictUndefined
from pathlib import Path

def generate(graph, out_dir):
    env = Environment(
        loader=FileSystemLoader(str(Path(__file__).resolve().parent.parent / "templates")),
        undefined=StrictUndefined,
        trim_blocks=True,
        lstrip_blocks=True,
    )
    main_tpl = env.get_template("main_pyspark.py.j2")

    steps = []
    for node in graph.topological_order():
        cfg = node.config or {}
        steps.append({"id": node.id, "type": node.type, "name": node.name, "config": cfg})

    code = main_tpl.render(steps=steps)
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    (Path(out_dir) / "main.py").write_text(code, encoding="utf-8")
    return {"engine": "pyspark", "files": ["main.py"]}
