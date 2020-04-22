# SAYN Project Structure

## Initial Project Structure

A usual SAYN project structure will be as follows:

      project-name    # The name you chose.
        logs/         # The folder where SAYN logs are stored.
        python/       # The folder where Python tasks modules should be stored.
        sql/          # The folder where SQL tasks queries should be stored.
        models.yaml   # The backbone of a SAYN project enabling to define and orchestrate tasks.
        settings.yaml # The settings of an individual SAYN user.
        .gitignore    # To ignore settings.yaml and other files.

The two core files of a SAYN project are `settings.yaml` (unique settings for each individual) and `models.yaml` where the tasks of the project are defined (this is shared by all users on the project).

Please note that `sayn init [project-name]` initialises your SAYN project with an example and and a sample SQLite database. This is only so you can have an overview of SAYN and go through the [Tutorial](tutorial.md) and can be deleted once you start working on your own project.

We will now cover the detail of both files, what they contain and what they are used for.
