![](https://github.com/173TECH/sayn/workflows/CI/badge.svg)

<img
  src="https://173-static-files.s3.eu-west-2.amazonaws.com/sayn_docs/logos/sayn_logo.png"
  alt="SAYN logo"
  style="width: 50%; height: 50%;"
/>

#

SAYN is a data-modelling and processing framework for automating Python and SQL tasks. It enables analytics teams to build robust data infrastructures in minutes.

 *Status: SAYN is under active development so some changes can be expected.*

## Use Cases

SAYN can be used for multiple purposes across the analytics workflow:

* Data extraction: complement tools such as Fivetran or Stitch with customised extraction processes.
* Data modelling: transform raw data in your data warehouse.
* Data science: integrate and execute data science models.

## Key Features

SAYN has the following key features:

* YAML based creation of [DAGs](https://173tech.github.io/sayn/dags/) (Direct Acyclic Graph). This means all analysts, including non Python proficient ones, can contribute to building ETL processes.
* [SQL SELECT statements](https://173tech.github.io/sayn/tasks/autosql/): turn your queries into managed tables and views automatically.
* [Jinja parameters](https://173tech.github.io/sayn/parameters/): switch easily between development and product environment and other tricks with Jinja templating.
* [Python tasks](https://173tech.github.io/sayn/tasks/python/): use Python scripts to complement your extraction and loading layer and build data science models.
* Multiple [databases](https://173tech.github.io/sayn/databases/overview/) supported.
* and much more... See the [Documentation](https://173tech.github.io/sayn/).

## Design Principles

SAYN is designed around three core principles:

* **Simplicity**: data models and processes should be easy to create, scale and maintain. So your team can focus on data transformation instead of writing processes. SAYN orchestrates all your tasks systematically and provides a lot of automation features.
* **Flexibility**: the power of data is unlimited and so should your tooling. SAYN currently supports both SQL and Python so your analysts can choose the most optimal solution for each process.
* **Centralisation**: all analytics code should live in one place, making your life easier and allowing dependencies throughout the whole analytics process.

## Quick Start

```bash
$ pip install sayn
$ sayn init test_sayn
$ cd test_sayn
$ sayn run
```

This is it! You completed your first SAYN run on the example project. Continue with the [Tutorial: Part 1](https://173tech.github.io/sayn/tutorials/tutorial_part1/) which will give you a good overview of SAYN's true power!

## Support

If you need any help with SAYN, or simply want to know more, please contact the team at <sayn@173tech.com>.

## License

SAYN is open source under the [Apache 2.0](https://www.apache.org/licenses/LICENSE-2.0) license.

---

Made with :heart: by [173tech](https://www.173tech.com).
