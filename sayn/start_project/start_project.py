import logging
import os
from pathlib import Path
import shutil


def sayn_init(sayn_project_name):
    src = Path(os.path.dirname(os.path.abspath(__file__)), 'sayn_project_base').absolute()
    dst = Path(sayn_project_name).absolute()

    logging.info('Creating SAYN base project at the following location: {dst}...'.format(dst=dst))
    try:
        shutil.copytree(src, dst)

        # rename the sample_settings.yaml to settings.yaml
        os.rename(Path(dst, 'sample_settings.yaml').absolute(), Path(dst, 'settings.yaml').absolute())

        logging.info('SAYN base project created.')
        logging.info('For more information about SAYN visit our documentation: https://173tech.github.io/sayn/')
    except OSError as e:
        print(e)
