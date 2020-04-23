# `dummy` Task

## About

The `dummy` is a task that does not do anything. It is mostly used as a connector between tasks.

## Defining `dummy` Tasks In `models.yaml`

A `dummy` task is defined as follows:

```yaml
task_dummy:
  type: dummy
```

This task does not require any other setting than its `type`.

## Usage

`dummy` tasks come in useful when you have multiple tasks that depend upon a long list of similar parents. Let's consider the following setup in `models.yaml`:

```yaml
#your models code

tasks:
  #definition of task_1, task_2, task_3, task_4 ...

  task_mlt_parents_1:
    #task definition
    parents:
      - task_1
      - task_2
      - task_3
      - task_4

  task_mlt_parents_2:
    #task definition
    parents:
      - task_1
      - task_2
      - task_3
      - task_4

  task_mlt_parents_3:
    #task definition
    parents:
      - task_1
      - task_2
      - task_3
      - task_4
```

You can avoid repeting the `parents` across those multiple tasks using a `dummy` task to create a connector. This is how it would look like with a dummy task.

```yaml
#your models code

tasks:
  #some tasks

  dummy_task:
    type: dummy
    parents:
      - task_1
      - task_2
      - task_3
      - task_4

  task_mlt_parents_1:
    #task definition
    parents:
      - dummy_task

  task_mlt_parents_2:
    #task definition
    parents:
      - dummy_task

  task_mlt_parents_3:
    #task definition
    parents:
      - dummy_task
```
