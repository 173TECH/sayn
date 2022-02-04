# Tutorial Part 3: Using Python With SAYN

The [previous section of this tutorial](tutorial_part2.md) showed you how to use SAYN for data modelling purposes. We will now show you how to use Python with SAYN. This will therefore enable you to write end-to-end ELT processes and data science tasks with SAYN. Let's dive in!

## Adding Your Python Task Group

As we did for the `autosql` tasks, we will need to add a task group for our Python tasks. To do so, add the following to your `project.yaml`:

!!! "project.yaml"
  ```yaml
  ...

  groups:
      models:
        ...

      logs:
        type: python
        module: load_data
  ```

This will do the following:

* Create a task group called `logs`.
* All tasks in this group will be of type `python`.
* All functions using the `task` decorator in the file `python/load_data.py` will be transformed into Python tasks. This file should already exist in your `python` folder and defines one task: `load_data`. All `python` tasks should be stored in the `python` folder where an `__init__.py` file must exist.

## Writing Your Python Tasks

### A Simple Python Task

Our tutorial project has two `python` tasks. It starts with simple Python task that interacts with the task's context. This is the `say_hello` task, defined as follows:

```python
from sayn import task

@task()
def say_hello(context):
    context.info('Hello!')
```

Here are the core concepts to know for running Python tasks with SAYN:

* You should import the `task` decorator from `sayn` which you can then use to define your tasks. There is a more advanced way to define `python` tasks with classes using SAYN which you can find in the documentation related to `python` tasks.
* Use the `@task` decorator and then simply define your function. The task name will be the name of the function, in this case `say_hello`.
* We pass the `context` to our function, which can then be used in our code to access task related information and control the logger. In our case, we log `Hello!` as information.

### Creating Data Logs

The second task in the `python/load_data.py` module actually does something more interesting. It creates some random logs, which is the data you initially had in the logs table of `dev.db`. Let's look at whole the code from the `python/load_data.py` file:

```python
import random
from uuid import uuid4

from sayn import task

@task()
def say_hello(context):
    context.info('Hello!')

@task(outputs=[
        'logs_arenas',
        'logs_tournaments',
        'logs_battles',
        'logs_fighters']
     )
def load_data(context, warehouse):
    fighters = ["Son Goku", "John", "Lucy", "Dr. x", "Carlos", "Dr. y?"]
    arenas = ["Earth Canyon", "World Edge", "Namek", "Volcanic Crater", "Underwater"]
    tournaments = ["World Championships", "Tenka-ichi Budokai", "King of the Mountain"]

    context.set_run_steps(
        [
            "Generate Dimensions",
            "Generate Battles",
            "Load fighters",
            "Load arenas",
            "Load tournaments",
            "Load battles",
        ]
    )

    with context.step("Generate Dimensions"):
        # Add ids to the dimensions
        fighters = [
            {"fighter_id": str(uuid4()), "fighter_name": val}
            for id, val in enumerate(fighters)
        ]
        arenas = [
            {"arena_id": str(uuid4()), "arena_name": val}
            for id, val in enumerate(arenas)
        ]
        tournaments = [
            {"tournament_id": str(uuid4()), "tournament_name": val}
            for id, val in enumerate(tournaments)
        ]

    with context.step("Generate Battles"):
        battles = list()
        for tournament in tournaments:
            tournament_id = tournament["tournament_id"]
            # Randomly select a number of battles to generate for each tournament
            n_battles = random.choice([10, 20, 30])

            for _ in range(n_battles):
                battle_id = str(uuid4())

                # Randomly choose fighters and arena
                fighter1_id = random.choice(fighters)["fighter_id"]
                fighter2_id = random.choice(
                    [f for f in fighters if f["fighter_id"] != fighter1_id]
                )["fighter_id"]
                arena_id = random.choice(arenas)["arena_id"]

                # Pick a winner
                winner_id = (
                    fighter1_id if random.uniform(0, 1) <= 0.5 else fighter2_id
                )

                battles.append(
                    {
                        "event_id": str(uuid4()),
                        "tournament_id": tournament_id,
                        "battle_id": battle_id,
                        "arena_id": arena_id,
                        "fighter1_id": fighter1_id,
                        "fighter2_id": fighter2_id,
                        "winner_id": winner_id,
                    }
                )

    data_to_load = {
        "fighters": fighters,
        "arenas": arenas,
        "tournaments": tournaments,
        "battles": battles,
    }

    # Load logs
    for log_type, log_data in data_to_load.items():
        with context.step(f"Load {log_type}"):
            warehouse.load_data(f"logs_{log_type}", log_data, replace=True)
```

The second task defined in this module is the `load_data` one. It uses some further features:

* The `load_data` task produces `outputs` which are defined in the decorator. This will enable you to refer to these outputs with the `src` function in `autosql` tasks and automatically set dependencies to the `load_data` task.
* `warehouse` is also passed as a parameter to the function. This enables you to easily access the `warehouse` connection in your task. You can notably see that at the end of the script with the call to `warehouse.load_data`.

### Setting Dependencies With `load_data`

As mentioned, we now have a `python` task which produces some logs which we want our `autosql` tasks to use for the data modelling process. As a result, we should ensure that the `load_data` task is always executed first. Because our `load_data` task produces `outputs`, we can refer to these with the `src` function in `autosql` tasks and automatically create dependencies. For example, the SQL query of the `dim_arenas` task should be changed from:

```sql
SELECT l.arena_id
     , l.arena_name
  FROM logs_arenas l
```

To:

```sql
SELECT l.arena_id
     , l.arena_name
  FROM {{ src('logs_arenas') }} l
```

This will now mention that the `dim_arenas` task sources the `logs_arenas` table which is an `output` of the `load_data` task. SAYN will automatically make `load_data` a parent of the `dim_arenas` task and therefore always execute it before. You can do the same for all the other logs tables used in the other `autosql` tasks.

## What Next?

This is it for our tutorial. You should now have a good understanding of the true power of SAYN! Our documentation has more extensive details about all the concepts we covered:

* ADD

Enjoy SAYN and happy ELT-ing! :)
