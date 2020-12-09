# `dummy` Task

## About

The `dummy` is a task that does not do anything. It is mostly used as a handy connector between tasks when a large number of parents is common to several tasks. Using `dummy` as the parent of those reduces the length of the code and leads to cleaner task groups.

## Defining `dummy` tasks

A `dummy` task has no additional properties other than the properties shared by all task types.

!!! example
    ```yaml
    task_dummy:
      type: dummy
    ```

## Usage

`dummy` tasks come in useful when you have multiple tasks that depend upon a long list of parents. Let's consider the following setup in your task group `task_group.yaml`:

!!! example
    ```yaml
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

You can avoid repeating the `parents` across those multiple tasks using a `dummy` task to create a connector. This is how it would look like with a dummy task.

!!! example
    ```yaml
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
