from copy import copy, deepcopy
from collections import deque, OrderedDict
from datetime import datetime
from itertools import groupby
import logging
import sys

from .config import Config
from .utils.singleton import singleton
from .utils.logger import Logger
from .tasks import create_task, TaskStatus, IgnoreTask


class DagValidationError(Exception):
    pass


@singleton
class Dag:
    """Directed acyclic graph implementation."""

    graph = OrderedDict()
    tasks = dict()

    def __init__(self, tasks_query=(), exclude_query=()):
        Logger().set_config(stage="DAG")
        logging.info("----------")
        logging.info(f"Setting up. Run ID: {Config().run_id}")
        logging.info("----------")

        task_definitions = Config()._task_definitions

        # Setup DAG structure using just names and parents
        self._from_dict_reversed(
            {k: v.get("parents", list()) for k, v in task_definitions.items()}
        )

        # Calculate list of tasks in the correct order of execution based on parentage
        self.tasks = {name: None for name in self.topological_sort()}

        # Get the list of tasks that need to run in this execution

        # 1. Get a dict of tags > list of tasks with that tag (used by _task_query to get the list of relevant tasks)
        tags = {
            tag: [i[1] for i in tag_tasks]
            for tag, tag_tasks in groupby(
                sorted(
                    [
                        (tag, task_name)
                        for task_name, task_def in task_definitions.items()
                        for tag in task_def.get("tags", list())
                    ],
                    key=lambda x: x[0],
                ),
                lambda x: x[0],
            )
        }

        # 2. Get a dict of dags > task list with that model (used by _task_query to get the list of relevant tasks)
        dags = {
            dag: [i[1] for i in dag_tasks]
            for dag, dag_tasks in groupby(
                sorted(
                    [
                        (task_def["dag"], task_name)
                        for task_name, task_def in task_definitions.items()
                    ],
                    key=lambda x: x[0],
                ),
                lambda x: x[0],
            )
        }

        # 3. We generate tasks_to_process containing all tasks to be run
        if len(tasks_query) == 0:
            # If there no task filter, we'll run all tasks
            tasks_to_process = set(self.tasks.keys())
        else:
            # Otherwise we get the list of tasks corresponding to what's specified in --tasks
            tasks_to_process = set(
                t for q in tasks_query for t in self._task_query(tags, dags, q)
            )

        # We always apply the exclude filter query
        tasks_to_process = tasks_to_process - set(
            t for q in exclude_query for t in self._task_query(tags, dags, q)
        )

        # Create task objects and set them up
        self.tasks = {
            name: create_task(
                name,
                task_definitions[name]["type"],
                task_definitions[name],
                name not in tasks_to_process,
            )
            for name in self.tasks.keys()
        }

        # Once all objects are created, we can add references to those in each task
        for _, task in self.tasks.items():
            task.set_parents(self.tasks)

        # Run the setup for each task
        for _, task in self.tasks.items():
            Logger().set_config(task=task.name)
            if task.status == TaskStatus.FAILED:
                continue
            task.setup()

        # delete the task_definitions attribute from the config so it we do not have tasks on both APIs
        # delattr(Config(), "_task_definitions")
        Logger().set_config(task=None)
        logging.info("DAG Setup: done.")

    def _task_query(self, tags, dags, query):
        """Returns a list of tasks from the dag matching the query"""

        if query[0] == "+":
            # A "+" before a task name means the specified task and all its parents
            task = query[1:]
            if task not in self.tasks:
                raise KeyError(f'Tag "{task}" not in dag')
            return [task] + self.all_upstreams(task)

        elif query[-1] == "+":
            # A "+" after a task name means the specified task and all its children
            task = query[:-1]
            if task not in self.tasks:
                raise KeyError(f'Task "{task}" not in dag')
            return [task] + self.all_downstreams(task)

        elif query[:4] == "tag:":
            # A tag name will be specified as `tag:tag_name`
            tag = query[4:]
            if tag not in tags:
                raise KeyError(f'Tag "{tag}" not in dag')
            return tags[tag]

        elif query[:6] == "dag:":
            # A dag name will be specified as `dag:dag_name`
            dag = query[6:]
            if dag not in dags:
                raise KeyError(f'DAG "{dag}" does not exists')
            return dags[dag]

        elif query in self.tasks:
            # Otherwise it should be a single task name
            return [query]

        else:
            # ... or the task doesn't exists in the dag
            raise KeyError(f'Task "{query}" not in dag')

    def _run_task(self, command, task):
        Logger().set_config(task=task.name)
        task_start_ts = datetime.now()
        if task.status != TaskStatus.READY:
            task.failed("Task failed during setup. Skipping...")
        elif not task.can_run():
            logging.warn("SKIPPING")
            task.skipped()
        else:
            logging.debug("Starting")
            task.executing()
            try:
                if command == "compile":
                    status = task.compile()
                elif command == "run":
                    status = task.run()
                else:
                    status = None
            except Exception as e:
                logging.exception(e)

                status = None

            if status is None:
                task.status = TaskStatus.UNKNOWN
                logging.error(
                    f"Finished in an unknown state ({datetime.now() - task_start_ts})"
                )
            elif status != TaskStatus.SUCCESS:
                logging.error(f"Failed status ({datetime.now() - task_start_ts})")
            else:
                logging.info(
                    f"\u001b[32mSuccess ({datetime.now() - task_start_ts})\u001b[0m"
                )

        return task.status

    def _run_command(self, command):
        Logger().set_config(stage="Run", task=None, progress="0")
        logging.info("----------")
        logging.info(f"Running. Run ID: {Config().run_id}")
        logging.info("----------")

        failed = list()
        success = list()
        skipped = list()

        tasks_to_run = [
            task for task in self.tasks.values() if not isinstance(task, IgnoreTask)
        ]
        ntasks = len(tasks_to_run)

        tcounter = 0
        for task in self.tasks.values():
            if not task.should_run():
                # For IgnoreTasks
                task.success()
                continue

            status = self._run_task(command, task)
            if status == TaskStatus.SKIPPED:
                skipped.append(task.name)
            elif status in (TaskStatus.FAILED, TaskStatus.UNKNOWN) or status != TaskStatus.SUCCESS:
                failed.append(task.name)
            else:
                success.append(task.name)

            tcounter += 1
            run_progress = str(round((tcounter) / ntasks * 100))
            Logger().set_config(progress=run_progress)

        Logger().set_config(stage="Summary", task=None, progress=None)
        logging.info("----------")
        logging.info(f"Stats for Run ID: {Config().run_id}")
        logging.info("----------")

        recap_str = list()
        recap_str.append(
            f"Process finished. Total tasks: {ntasks}. Success: {len(success)}. Failed {len(failed)}. Skipped {len(skipped)}."
        )
        if len(success) > 0:
            recap_str.append(f"The following tasks succeded: {', '.join(success)}")
        if len(failed) > 0:
            recap_str.append(f"The following tasks failed: {', '.join(failed)}")
        if len(skipped) > 0:
            recap_str.append(f"The following tasks were skipped: {', '.join(skipped)}")

        if len(failed) > 0:
            log_func = logging.error
        elif len(skipped) > 0:
            log_func = logging.warning
        else:
            log_func = lambda x: logging.info(f"\u001b[32m{x}\u001b[0m")

        for msg in recap_str:
            log_func(msg)

    def run(self):
        self._run_command("run")

    def compile(self):
        self._run_command("compile")

    def plot(self, folder=None, file_name=None):
        """Uses graphviz to plot the dag
        It requires the graphviz python package (pip install graphviz) and an installation of graphviz
        (eg: brew install graphviz)
        """

        task_list = list(self.tasks.keys())

        try:
            from graphviz import Digraph
        except:
            logging.error(
                "Graphviz is required. To install it `pip install graphviz` and install it in your system (eg: `brew install graphviz"
            )
            sys.exit()
        dot = Digraph(comment="SAYN", graph_attr={"splines": "ortho", "nodesep": "0.8"})
        for n in task_list:
            dot.node(n, style="rounded", shape="box")
        for a, l in self.graph.items():
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

    # Creation

    def _from_dict(self, graph_dict):
        """Reset the graph and build it from the passed dictionary.
        The dictionary takes the form of {node_name: [directed edges]}
        """

        self.reset_graph()

        graph_dict = deepcopy(graph_dict)
        graph_dict.update(
            {v: [] for l in graph_dict.values() for v in l if v not in graph_dict}
        )

        for new_node in graph_dict.keys():
            self.add_node(new_node)
        for ind_node, dep_nodes in graph_dict.items():
            if not isinstance(dep_nodes, list):
                raise TypeError("dict values must be lists")
            for dep_node in dep_nodes:
                self.add_edge(ind_node, dep_node)

    def _from_dict_reversed(self, graph_dict):
        """Reverses the dictionary before calling _from_dict"""
        self._from_dict(self._reverse_dict(graph_dict))

    def _reverse_dict(self, graph_dict):
        """Reverses the dictionary"""
        out = {
            k: list([v[1] for v in g])
            for k, g in groupby(
                sorted([(v, a) for a, l in graph_dict.items() for v in l]),
                key=lambda x: x[0],
            )
        }
        # Make sure all nodes are in
        out.update(
            {
                k: []
                for k in set(
                    [v for a, l in graph_dict.items() for v in l if v not in out]
                    + [k for k in graph_dict.keys() if k not in out]
                )
            }
        )
        return out

    def reset_graph(self):
        """Restore the graph to an empty state."""
        self.graph = OrderedDict()

    def validate(self, graph=None):
        """Returns (Boolean, message) of whether DAG is valid."""
        graph = graph if graph is not None else self.graph
        if len(self.ind_nodes(graph)) == 0:
            return (False, "no independent nodes detected")
        try:
            self.topological_sort(graph)
        except ValueError:
            return (False, "failed topological sort")
        return (True, "valid")

    # Manipulation

    def add_node(self, node_name, graph=None):
        """Add a node if it does not exist yet, or error out."""
        if not graph:
            graph = self.graph
        if node_name in graph:
            raise KeyError("node %s already exists" % node_name)
        graph[node_name] = set()

    def add_node_if_not_exists(self, node_name, graph=None):
        try:
            self.add_node(node_name, graph=graph)
        except KeyError:
            pass

    def delete_node(self, node_name, graph=None):
        """Deletes this node and all edges referencing it."""
        if not graph:
            graph = self.graph
        if node_name not in graph:
            raise KeyError("node %s does not exist" % node_name)
        graph.pop(node_name)

        for node, edges in iter(graph.items()):
            if node_name in edges:
                edges.remove(node_name)

    def delete_node_if_exists(self, node_name, graph=None):
        try:
            self.delete_node(node_name, graph=graph)
        except KeyError:
            pass

    def add_edge(self, ind_node, dep_node, graph=None):
        """Add an edge (dependency) between the specified nodes."""
        if not graph:
            graph = self.graph
        if ind_node not in graph or dep_node not in graph:
            raise KeyError("one or more nodes do not exist in graph")
        test_graph = deepcopy(graph)
        test_graph[ind_node].add(dep_node)
        is_valid, message = self.validate(test_graph)
        if is_valid:
            graph[ind_node].add(dep_node)
        else:
            raise DagValidationError()

    def delete_edge(self, ind_node, dep_node, graph=None):
        """Delete an edge from the graph."""
        if not graph:
            graph = self.graph
        if dep_node not in graph.get(ind_node, []):
            raise KeyError("this edge does not exist in graph")
        graph[ind_node].remove(dep_node)

    def rename_edges(self, old_task_name, new_task_name, graph=None):
        """Change references to a task in existing edges."""
        if not graph:
            graph = self.graph
        for node, edges in graph.items():

            if node == old_task_name:
                graph[new_task_name] = copy(edges)
                del graph[old_task_name]

            else:
                if old_task_name in edges:
                    edges.remove(old_task_name)
                    edges.add(new_task_name)

    # Navigation

    def predecessors(self, node, graph=None):
        """Returns a list of all predecessors of the given node."""
        if graph is None:
            graph = self.graph
        return [key for key in graph if node in graph[key]]

    def upstream(self, node, graph=None):
        """Returns a list of all nodes this node has edges towards."""
        if graph is None:
            graph = self.graph
        if node not in graph:
            raise KeyError("node %s is not in graph" % node)
        return list(self.predecessors(node, graph))

    def all_upstreams(self, node, graph=None):
        """Returns a list of all nodes ultimately downstream
        of the given node in the dependency graph, in
        topological order.
        """
        if graph is None:
            graph = self.graph
        nodes = [node]
        nodes_seen = set()
        i = 0
        while i < len(nodes):
            upstreams = self.upstream(nodes[i], graph)
            for upstream_node in upstreams:
                if upstream_node not in nodes_seen:
                    nodes_seen.add(upstream_node)
                    nodes.append(upstream_node)
            i += 1
        return list(
            filter(lambda node: node in nodes_seen, self.topological_sort(graph=graph))
        )

    def downstream(self, node, graph=None):
        """Returns a list of all nodes this node has edges towards."""
        if graph is None:
            graph = self.graph
        if node not in graph:
            raise KeyError("node %s is not in graph" % node)
        return list(graph[node])

    def all_downstreams(self, node, graph=None):
        """Returns a list of all nodes ultimately downstream
        of the given node in the dependency graph, in
        topological order.
        """
        if graph is None:
            graph = self.graph
        nodes = [node]
        nodes_seen = set()
        i = 0
        while i < len(nodes):
            downstreams = self.downstream(nodes[i], graph)
            for downstream_node in downstreams:
                if downstream_node not in nodes_seen:
                    nodes_seen.add(downstream_node)
                    nodes.append(downstream_node)
            i += 1
        return list(
            filter(lambda node: node in nodes_seen, self.topological_sort(graph=graph))
        )

    def all_leaves(self, graph=None):
        """Return a list of all leaves (nodes with no downstreams)"""
        if graph is None:
            graph = self.graph
        return [key for key in graph if not graph[key]]

    def ind_nodes(self, graph=None):
        """Returns a list of all nodes in the graph with no dependencies."""
        if graph is None:
            graph = self.graph

        dependent_nodes = set(
            node for dependents in iter(graph.values()) for node in dependents
        )
        return [node for node in graph.keys() if node not in dependent_nodes]

    def topological_sort(self, graph=None):
        """Returns a topological ordering of the DAG.
        Raises an error if this is not possible (graph is not valid).
        """
        if graph is None:
            graph = self.graph

        in_degree = {}
        for u in graph:
            in_degree[u] = 0

        for u in graph:
            for v in graph[u]:
                in_degree[v] += 1

        queue = deque()
        for u in in_degree:
            if in_degree[u] == 0:
                queue.appendleft(u)

        l = []
        while queue:
            u = queue.pop()
            l.append(u)
            for v in graph[u]:
                in_degree[v] -= 1
                if in_degree[v] == 0:
                    queue.appendleft(v)

        if len(l) == len(graph):
            return l
        else:
            raise ValueError("graph is not acyclic")

    def size(self):
        return len(self.graph)
