from pathlib import Path
import importlib
import sys

from ..core.errors import Err, Ok


class PythonLoader:
    modules = list()

    def has_module(self, key):
        return key in self.modules

    def register_module(self, key, folder):
        path = Path(folder, "__init__.py")
        if not path.is_file():
            return Err("python_loader", "missing_init_py", path=path)

        loader = importlib.machinery.SourceFileLoader(
            key, str(Path(folder, "__init__.py"))
        )
        spec = importlib.util.spec_from_loader(key, loader)
        m = importlib.util.module_from_spec(spec)

        sys.modules[f"sayn_{key}"] = m

        try:
            loader.exec_module(m)
        except Exception as e:
            return Err(
                "python_loader", "exec_module_exception", module_key=key, exception=e
            )

        self.modules.append(f"sayn_{key}")

        return Ok()

    def get_class(self, module_key, class_path):
        if f"sayn_{module_key}" not in self.modules:
            return Err("python_loader", "module_not_registered", module_key=module_key)
        try:
            module_str = ".".join(class_path.split(".")[:-1])
            class_name = class_path.split(".")[-1]
        except Exception as e:
            return Err("python_loader", "load_class_exception", exception=e)

        if len(module_str) > 0:
            try:
                task_module = importlib.import_module(f"sayn_{module_key}.{module_str}")
            except Exception as e:
                return Err(
                    "python_loader",
                    "load_class_exception",
                    module_key=module_key,
                    exception=e,
                )
        else:
            task_module = importlib.import_module(f"sayn_{module_key}")

        try:
            klass = getattr(task_module, class_name)
        except:
            # module_file_name = (
            #     module_str.replace(".", "/") if len(module_str) > 0 else "__init__"
            # )
            return Err(
                "python_loader",
                "missing_class",
                module_key=module_key,
                module_path=module_str,
                pyclass=class_name,
            )

        return Ok(klass)
