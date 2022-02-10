from .misc import reverse_dict


def plot_dag(tasks, folder=None, file_name=None):
    """Uses mermaid to plot the dag"""
    alphab = "ABCDEFGHIJK"
    task_list = list(tasks.keys())
    task_dict = {}
    for i, task in enumerate(task_list):
        task_dict[task] = alphab[i]

    tasks = reverse_dict(tasks)

    text = """<div class="mermaid">\ngraph TB\n"""

    for a, l in tasks.items():
        if a in task_list:
            for b in l:
                if b in task_list:
                    text += f"{task_dict[a]}[{a}] --> {task_dict[b]}[{b}]\n"

    text += """</div>\n<script>mermaid.initialize({startOnLoad:true});</script>\n<script src="https://cdn.jsdelivr.net/npm/mermaid/dist/mermaid.min.js"></script>"""

    file = open("dag.html", "w")
    file.write(text)
    file.close()
