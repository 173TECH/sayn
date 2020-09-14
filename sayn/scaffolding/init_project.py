import os
from pathlib import Path
import shutil


def sayn_init(sayn_project_name):
    src = Path(
        os.path.dirname(os.path.abspath(__file__)), "data", "init_project"
    ).absolute()
    dst = Path(sayn_project_name).absolute()

    print(
        "Creating SAYN base project at the following location: {dst}...".format(dst=dst)
    )
    try:
        shutil.copytree(src, dst)

        # rename the sample_settings.yaml to settings.yaml
        os.rename(
            Path(dst, "sample_settings.yaml").absolute(),
            Path(dst, "settings.yaml").absolute(),
        )

        print("SAYN base project created.")
        print("For more information about SAYN visit our documentation:")
        print("https://173tech.github.io/sayn/")
    except OSError as e:
        print(e)
