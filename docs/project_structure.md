# SAYN Project Structure

SAYN projects are structured as follows:

```
  project_name   
    compile/ #only appears after first run     
    dags/
        dag.yaml
    logs/ #only appears after first run
        sayn.log
    python/
        __init__.py
        task_1.py
    sql/
        task_2.sql
        task_3.sql
        task_4.sql
    .gitignore
    project.yaml
    readme.md
    settings.yaml
```

Please see below the role of each component:

* `project.yaml`: defines the core components of the SAYN project. It is **shared across all collaborators**.
* `settings.yaml`: defines the individual user's settings. It is **unique for each collaborator and should never be pushed to git** as it contains credentials.
* `dags`: folder where `dag` files are stored. SAYN tasks are defined in those files.
* `python`: folder where `python` tasks are stored.
* `sql`: folder where `sql` and `autosql` tasks are stored.
* `logs`: folder where SAYN logs are written.
* `compile`: folder where SQL queries are compiled before execution.
