from pathlib import Path

from sayn.utils.python_loader import PythonLoader
from . import inside_dir

# utils


def initiate_python_setup(module=None, module_content=None):
    fpath = Path("python", "__init__.py")
    fpath.parent.mkdir(parents=True, exist_ok=True)
    fpath.write_text("")

    if module is not None:
        fpath = Path("python", f"{module}.py")
        fpath.write_text(module_content)


# tests


def test_python(tmp_path):
    python_loader = PythonLoader()
    # check ok
    module = "test_python"
    module_content = "from sayn import PythonTask\nclass TestPython(PythonTask):\n\tdef setup(self):\n\t\treturn self.success()\n\tdef run(self):\n\t\treturn self.success()"
    with inside_dir(tmp_path):
        initiate_python_setup(module=module, module_content=module_content)
        assert python_loader.register_module("python_tasks", "python").is_ok
        assert python_loader.get_class("python_tasks", "test_python.TestPython").is_ok


def test_python_task_err1(tmp_path):
    python_loader = PythonLoader()
    # check error missing module
    with inside_dir(tmp_path):
        initiate_python_setup()
        assert python_loader.register_module("python_tasks", "python").is_ok
        assert python_loader.get_class("python_tasks", "fake_module.fake_class").is_err


def test_python_task_err2(tmp_path):
    python_loader = PythonLoader()
    # check error module exists missing class
    module = "test_python"
    module_content = ""
    with inside_dir(tmp_path):
        initiate_python_setup(module=module, module_content=module_content)
        assert python_loader.register_module("python_tasks", "python").is_ok
        assert python_loader.get_class(
            "python_tasks", "test_python.TestPythonErr"
        ).is_err
