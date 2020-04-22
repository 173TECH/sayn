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
        logging.info('SAYN base project created.')
    except OSError as e:
        print(e)
