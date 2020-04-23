# SAYN

SAYN is a data-modelling and processing framework for automating Python and SQL tasks. It enables analytics teams to build robust data infrastructures in minutes.

## Use Cases

SAYN can be used for multiple purposes across the analytics workflow:

* Data extraction: complement tools such as Stitch or Fivetran with customised extraction processes.
* Date modelling: transform raw data in your data warehouse.
* Date science: integrate and execute data science models.

Key Features

* [SQL SELECT statements](tasks/core/autosql.md): turn your queries into managed tables and views automatically.
* [Jinja parameters](parameters.md): switch easily between development and product environment and other tricks with Jinja templating.
* [Python tasks](tasks/core/python.md): use Python scripts to complement your extraction and loading layer and build data science models.
* Create a Direct Acyclic Graph by simply declaring task dependencies.
* Multiple [databases](databases.md) supported.

## Philosophy

SAYN is designed around three core principles:

* **Simplicity**: data models and processes should be easy to create, scale and maintain. So your team can focus on data transformation instead of writing processes. SAYN orchestrates all your tasks systematically and provides a lot of automation features.
* **Flexibility**: data can sometimes be muddy. Analysts need to be able to use the most optimal solution for each process. SAYN currently supports both SQL and Python.
* **Centralisation**: all analytics code should live in one place, making your life easier and allowing dependencies throughout the whole analytics process.

## Quick Start

You can start running SAYN in minutes:

1. `pip install https://github.com/173TECH/sayn.git`
2. `cd` to the directory where you want to create your SAYN project
3. `sayn init [project_name]`
4. `cd [project_name]`
5. `sayn run`

This is it, you made your first SAYN run based on the example project!

## Next Steps

Continue with the SAYN [Tutorial](tutorial.md) which will give you a good overview of SAYN's true power!

## Support

If you need any help with SAYN, or simply want to know more, please contact the team at <sayn@173tech.com>.

---

Made with :heart: by [173tech](https://www.173tech.com).

 *Status: SAYN is under active development so some changes can be expected.*
