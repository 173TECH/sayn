# Installation

SAYN is available for installation using `pip` and is regularly tested in Python 3.6+ on both
MacOS and Linux environments.

It is recommended to separate the python environment from project to project, so you might want to
create a virtual environment first by running the following in a terminal:

```bash
python -m venv sayn_venv
source sayn_venv/bin/activate
```

From here, instaling SAYN is just one pip command away:

```bash
pip install sayn
```

This default installation will not install any extra [database drivers](databases/overview.md), so
only support for sqlite will be available. Extra drivers can be installed using pip's optional
packages specification:

```bash
pip install "sayn[postgresql]"
```

Check the [database](databases/overview.md) section for a full list of supported databases.

By default the tutorials use sqlite, so with the setup above you're already setup to follow the
[tutorial](tutorials/tutorial_part1.md).
