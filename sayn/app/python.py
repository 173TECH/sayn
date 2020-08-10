from pathlib import Path
import importlib
import sys


class PythonLoaderError(Exception):
    pass


class PythonLoader:
    modules = list()

    def register_module(self, key, folder):
        if f"sayn_{key}" in self.modules:
            raise PythonLoaderError(f"{key.capitalize()} module already registered")

        path = Path(folder, "__init__.py")
        if not path.is_file():
            raise PythonLoaderError(f"Missing file: {path.fullname}")

        loader = importlib.machinery.SourceFileLoader(
            key, str(Path(folder, "__init__.py"))
        )
        spec = importlib.util.spec_from_loader(key, loader)
        m = importlib.util.module_from_spec(spec)

        sys.modules[f"sayn_{key}"] = m

        try:
            loader.exec_module(m)
        except Exception as e:
            raise PythonLoaderError(
                f"Error importing python module {self.python_path.absolute}, {e}"
            )

        self.modules.append(f"sayn_{key}")

    def get_class(self, module_key, class_path):
        if f"sayn_{module_key}" not in self.modules:
            raise PythonLoaderError(
                f"{module_key.capitalize()} module  not registered registered"
            )

        module_str = ".".join(class_path.split(".")[:-1])
        class_name = class_path.split(".")[-1]

        if len(module_str) > 0:
            try:
                task_module = importlib.import_module(f"sayn_{module_key}.{module_str}")
            except Exception as e:
                raise PythonLoaderError(f'Error loading module "{module_str}", {e}')
        else:
            task_module = importlib.import_module(f"sayn_{module_key}")

        try:
            klass = getattr(task_module, class_name)
        except Exception as e:
            module_file_name = (
                module_str.replace(".", "/") if len(module_str) > 0 else "__init__"
            )
            raise PythonLoaderError(
                f'Error importing class "{class_path}" found in "python/{module_file_name}.py", {e}'
            )

        return klass
