from multiprocessing import Process
import concurrent.futures
import time
from collections import OrderedDict

from scrap.test_process import Task

from sayn.database.creator import create as create_db
from sayn.utils.dag import topological_sort_step


N_TASK = 10
N_CPU = 4


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

    # Create tasks
    tasks = dict()
    for t in range(1, N_TASK + 1):
        tasks[t] = Task()

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

    print("Defined Tasks")

    for i, t in tasks.items():
        message = f"I am {i}"
        t.setup(i, message, connections)

    print("Setup Tasks")

    with concurrent.futures.ProcessPoolExecutor(N_CPU) as executor:
        for task_list in topological_sort_step(dag):
            # print(task_list)

            futures = {executor.submit(tasks[int(t)].run): t for t in task_list}

            for future in concurrent.futures.as_completed(futures):
                task_id = futures[future]
                try:
                    data = future.result()
                except Exception as exc:
                    print("%r generated an exception: %s" % (task_id, exc))
                else:
                    print("%r page is %d bytes" % (task_id, data))


if __name__ == "__main__":
    main()
