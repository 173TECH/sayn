<img
  src="https://173-static-files.s3.eu-west-2.amazonaws.com/sayn_docs/logos/sayn_logo.png"
  alt="SAYN logo"
  style="width: 50%; height: 50%;"
/>

#

SAYN is a modern data processing and modelling framework. Users define tasks (incl. Python, automated SQL transformations and more) and their relationships, SAYN takes care of the rest. It is designed for simplicity, flexibility and centralisation in order to bring significant efficiency gains to the data engineering workflow.

## Use Cases

SAYN can be used for multiple purposes across the data engineering and analytics workflows:

* Data extraction: complement tools such as Fivetran or Stitch with customised extraction processes.
* Data modelling: transform raw data in your data warehouse (e.g. aggregate activity or sessions, calculate marketing campaign ROI, etc.).
* Data science: integrate and execute data science models.

## Key Features

SAYN has the following key features:

* YAML based DAG (Direct Acyclic Graph) creation. This means all analysts, including non Python proficient ones, can easily add tasks to ETL processes with SAYN.
* [Automated SQL transformations](https://173tech.github.io/sayn/tasks/autosql/): write your SELECT statement. SAYN turns it into a table/view and manages everything for you.
* [Jinja parameters](https://173tech.github.io/sayn/parameters/): switch easily between development and product environment and other tricks with Jinja templating.
* [Python tasks](https://173tech.github.io/sayn/tasks/python/): use Python scripts to complement your extraction and loading layer and build data science models.
* Multiple [databases](https://173tech.github.io/sayn/databases/overview/) supported.
* and much more... See the [Documentation](https://173tech.github.io/sayn/).

## Design Principles

SAYN aims to empower data engineers and analysts through its  three core design principles:

* **Simplicity**: data processes should be easy to create, scale and maintain. So your team can focus on data transformation instead of writing processes. SAYN orchestrates all your tasks systematically and provides a lot of automation features.
* **Flexibility**: the power of data is unlimited and so should your tooling. SAYN supports both SQL and Python so your analysts can choose the most optimal solution for each process.
* **Centralisation**: all analytics code should live in one place, making your life easier and allowing dependencies throughout the whole analytics process.

## Quick Start

SAYN supports Python 3.7 to 3.10.

```bash
$ pip install sayn
$ sayn init test_sayn
$ cd test_sayn
$ sayn run
```

This is it! You completed your first SAYN run on the example project. Continue with the [Tutorial: Part 1](https://173tech.github.io/sayn/tutorials/tutorial_part1/) which will give you a good overview of SAYN's true power!

## Release Updates

If you want to receive update emails about SAYN releases, you can sign up [here](http://eepurl.com/hnfJIr).

## Support

If you need any help with SAYN, or simply want to know more, please contact the team at <sayn@173tech.com>.

## License

SAYN is open source under the [Apache 2.0](https://www.apache.org/licenses/LICENSE-2.0) license.

---

Made with :heart: by [173tech](https://www.173tech.com).
