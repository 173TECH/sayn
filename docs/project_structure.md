# SAYN Project Structure

SAYN projects are structured as follows:

```
tutorial
├── project.yaml
├── settings.yaml
├── dags
│   └── base.yaml
├── python
│   ├── __init__.py
│   ├── load_data.py
│   └── utils
│       ├── __init__.py
│       └── log_creator.py
├── sql
│   ├── dim_arenas.sql
│   ├── dim_fighters.sql
│   ├── dim_tournaments.sql
│   ├── f_battles.sql
│   ├── f_fighter_results.sql
│   └── f_rankings.sql
├── compile
├── .gitignore
├── readme.md
└── requirements.txt
```

* `project.yaml`: defines the core components of the SAYN project. It is **shared across all collaborators**.
* `settings.yaml`: defines the individual user's settings. It is **unique for each collaborator and should never be pushed to git** as it contains credentials.
* `dags`: folder where `dag` files are stored. SAYN tasks are defined in those files.
* `python`: folder where `python` tasks are stored.
* `sql`: folder where `sql` and `autosql` tasks are stored.
* `logs`: folder where SAYN logs are written.
* `compile`: folder where SQL queries are compiled before execution.
