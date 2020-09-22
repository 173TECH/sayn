from .misc import reverse_dict


def plot_dag(tasks, folder=None, file_name=None):
    """Uses graphviz to plot the dag
    It requires the graphviz python package (pip install graphviz) and an installation of graphviz
    (eg: brew install graphviz)
    """

    task_list = list(tasks.keys())
    tasks = reverse_dict(tasks)

    from graphviz import Digraph

    dot = Digraph(comment="SAYN", graph_attr={"splines": "ortho", "nodesep": "0.8"})
    for n in task_list:
        dot.node(n, style="rounded", shape="box")
    for a, l in tasks.items():
        if a in task_list:
            for b in l:
                if b in task_list:
                    dot.edge(a, b)

    if file_name is not None:
        dot.render(
            directory=folder,
            filename=file_name,
            cleanup=True,
            view=False,
            format="png",
        )
