from .misc import reverse_dict


def plot_dag(dag, tasks, folder=None, file_name=None):
    """Uses mermaid to plot the dag"""

    task_group = [[key, value.group] for key, value in tasks.items()]
    groups = list(set(val.group for val in tasks.values()))
    task_list = list(dag.keys())
    tasks_dag = reverse_dict(dag)

    text = """graph TB\n"""

    for a, l in tasks_dag.items():
        if a in task_list:
            for b in l:
                if b in task_list:
                    text += f"{a} --> {b}\n"

    for g in groups:
        text += f"subgraph {g}\n"
        for t in task_group:
            if t[1] == g:
                text += f"{t[0]}\n"
        text += "end\n"

    # text += """</div>\n<script>mermaid.initialize({startOnLoad:true});</script>\n<script src="https://cdn.jsdelivr.net/npm/mermaid/dist/mermaid.min.js"></script>"""

    file = open("./ui/src/Dag.svelte", "w")
    file.write(text)
    file.close()
