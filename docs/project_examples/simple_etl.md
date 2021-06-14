# SAYN Project Example: A Simple ETL

## Project Description

#### Overview

This is an example SAYN project which shows how to implement a simple ETL with the framework. You can find the GitHub repository [here](https://github.com/173TECH/sayn_project_example_simple_etl){target="\_blank"}.

This ETL extracts jokes from an API, translates them into Yodish (the language of Yoda, this is) with another API and then runs some SQL transformations on the extracted data. Both APIs are public and do not require an API key. However, they both have limited quotas (especially the Yodish translation API) so you should avoid re-running the extraction part of the project multiple times in a row (you can use the command `sayn run -x tag:extract` after the first `sayn run`).

#### Features Used

* Python task to extract data with APIs.
* Autosql tasks to automate SQL transformations.
* Usage of parameters to make the code dynamic.
* Usage of presets to define tasks.

By default, the project uses SQLite as a database. You can use [DB Browser for SQLite](https://sqlitebrowser.org/dl/){target="\_blank"} to navigate the data easily.

#### Running The Project

* Clone the repository with the command `git clone https://github.com/173TECH/sayn_project_example_simple_etl.git`.
* Rename the `settings_sample.yaml` file to `settings.yaml`.
* Install the project dependencies by running the `pip install -r requirements.txt` command from the root of the project folder.
* Run all SAYN commands from the root of the project folder.

#### Running The Project With PostgreSQL

If desired, you can also run the project using a PostgreSQL database. For this, you simply need to:

* Change the `warehouse` credential to use a PostgreSQL database connection.
* Install `psycopg2` as a package.
