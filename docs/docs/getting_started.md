# Getting Started With SAYN

## Installing SAYN

SAYN can be installed through Python's `pip` command. In order to install SAYN, please run `pip install sayn`.

## Creating your first project

In order to initialise your SAYN project, open your command line and then navigate to the directory in which you want to create your SAYN project. Then, run the command `sayn init [project-name]` where `[project-name]` is a name of your choice. This will create the SAYN repository which will have the following structure:

      project-name    # The name you chose.
        logs/         # The folder where SAYN logs are stored.
        python/       # The folder where Python tasks modules should be stored.
        sql/          # The folder where SQL tasks queries should be stored.
        models.yaml   # The backbone of a SAYN project enabling to define and orchestrate tasks.
        settings.yaml # The settings of an individual SAYN user.
        .gitignore    # To ignore settings.yaml and other files.
        readme.md     # Some instructions to get going.

Have a look at the folder structure, then you can proceed with your first SAYN run!

## Your first SAYN run

The SAYN folder initialised with `sayn init` comes with an example project which you can use to get familiar with SAYN. In order to run the project, go to the root of the project folder (where `models.yaml` is) and run the command `sayn run`. You should see the detail of tasks being executed. That's it, you have run your first SAYN project :) You can now continue with the [Tutorial](tutorial.md) which goes through the project example you got with `sayn init` and introduces some core SAYN concepts. Enjoy!
