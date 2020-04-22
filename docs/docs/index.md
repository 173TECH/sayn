# About SAYN

## Introduction

**SAYN is a command line tool that enables analytics teams to build a robust, scalable and efficient data modelling & processing infrastructure.** It enables data analysts and engineers to create and orchestrate tasks (including SQL and Python) and to build data processes in minutes. SAYN is maintained by [173tech](https://www.173tech.com){target="\_blank"} with :heart:.

## Usage

SAYN can be used for multiple purposes across the analytics workflow:

- data extraction: SAYN can be used in traditional ETL models to extract the data. For ELT models, SAYN can complement tools such as Stitch or Fivetran to orchestrate extraction of missing or suboptimal data extractors.
- data modelling: SAYN can be used for transforming data in your warehouse.
- data science: SAYN can be used to orchestrate the run of your data science algorithms.

Through its key features, SAYN allows the following:

- ability to use both Python and sql
- creation of a DAG with task dependency
- separation of development and production environments
- automation of data processes

## Philosophy

**SAYN is your perfect army knife for data modelling and processing.** It has been built to provide data analysts and engineers with a solution that enables quick and efficient implementation of data processes, whilst ensuring long term scalability. **SAYN is designed around four core principles:**

- **simplicity:** analytics infrastructures should be simple to orchestrate and maintain. With SAYN, data analysts and engineers simply define tasks and SAYN orchestrates everything in the background.
- **automation:** data analysts and engineers should focus on writing data transformation, not on writing processes. SAYN automates the heavy lifting through its many tasks. For example, the `autosql` task enables to write SQL `SELECT` statements, SAYN takes care of take table / view creation and maintenance in the background.
- **flexibity:** data is versatile. Data analysts and engineers should have the flexibility to use the right approach for the right process. SAYN currently supports both SQL and Python.
- **centralisation:** analytics infrastructures should be centralised (no need for many tools!). Because SAYN supports SQL and Python, it can be used to orchestrate the whole analytics workflow: covering data extraction (where necessary), data modelling and data science models.

## Community

For SAYN users to have the best experience, we maintain a [public Slack channel](link_to_be_added). Anyone use this channel in order to ask questions to the core developers and maintainers of SAYN (or to other fellow SAYNs!).

## Getting started

If you have been looking for a simple tool that can enable you to orchestrate and automate data modelling and processes then SAYN is for you! Continue with the [Getting Started](getting_started.md) section and get your data modelling and processes up and running in a few minutes.
