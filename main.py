from multiprocessing import Process
import concurrent.futures
import time
from collections import OrderedDict

from scrap.test_process import Task, TaskProcess

from sayn.database.creator import create as create_db
from sayn.utils.dag import topological_sort_step


N_TASK = 10
N_CPU = 4


def use_regular_class_and_executor(dag, connections):
    """We can limit the number of processes that run concurently by changing the N_CPU
    variable. So we can simulate sequential execution when needed (as default behaviour).
    """
    # Create tasks
    tasks = dict()
    for t in range(1, N_TASK + 1):
        tasks[t] = Task()

    print("Defined Tasks")

    for i, t in tasks.items():
        message = f"I am {i}"
        t.setup(i, message, connections)

    print("Setup Tasks")

    # Get ids of tasks that can be run together
    task_lists = list()
    for task_list in topological_sort_step(dag):
        task_lists.append(task_list)

    with concurrent.futures.ProcessPoolExecutor(N_CPU) as executor:
        for task_list in task_lists:
            futures = {executor.submit(tasks[int(t)].run): t for t in task_list}

            for future in concurrent.futures.as_completed(futures):
                task_id = futures[future]
                try:
                    data = future.result()
                except Exception as exc:
                    print("%r generated an exception: %s" % (task_id, exc))
                else:
                    print("%r page is %d" % (task_id, data))

    return


def use_process_class(dag, connections):
    """We can't limit the number of processes according to a set worker number.
    I thought we would gain advantages in how objects are defined, but that is not the case.
    """

    # Create tasks
    tasks = dict()

    for t in range(1, N_TASK + 1):
        message = f"I am {t}"
        tasks[t] = TaskProcess(t, message, connections)

    # Get ids of tasks that can be run together
    task_lists = list()
    for task_list in topological_sort_step(dag):
        task_lists.append(task_list)

    for task_list in task_lists:
        for t in task_list:
            tasks[int(t)].start()

        for t in task_list:
            tasks[int(t)].join()

    return


def main():
    # Simulate Settings Dictionary
    db = OrderedDict()
    db["type"] = "sqlite"
    db["database"] = ":memory:"
    db_name = "sqlite"

    # Simulate Connections Dictionary
    connections = dict()
    connections["target_db"] = create_db(
        db_name,
        db_name,
        db,
    )

    # Activating Connections and then Passing causes Pickling Error
    # for connection in connections.values():
    #     connection._activate_connection()

    # Define DAG based on tasks
    dag = {
        "1": [],
        "2": ["1"],
        "3": ["1", "2"],
        "4": [],
        "5": ["3", "4"],
        "6": ["4"],
        "7": ["6", "4"],
        "8": ["7"],
        "9": ["8"],
        "10": ["9"],
    }

    use_regular_class_and_executor(dag, connections)

    use_process_class(dag, connections)


if __name__ == "__main__":
    main()
