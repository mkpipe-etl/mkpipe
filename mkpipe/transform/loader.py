import importlib.util
import os
from typing import Callable


def load_transform_fn(ref: str, base_dir: str = None) -> Callable:
    if '::' not in ref:
        raise ValueError(
            f"Invalid transform reference: '{ref}'. "
            f"Expected format: 'path/to/file.py::function_name'"
        )

    file_path, fn_name = ref.rsplit('::', 1)

    if base_dir and not os.path.isabs(file_path):
        file_path = os.path.join(base_dir, file_path)

    file_path = os.path.abspath(file_path)

    if not os.path.exists(file_path):
        raise FileNotFoundError(
            f"Transform file not found: '{file_path}'"
        )

    spec = importlib.util.spec_from_file_location('_mkpipe_transform', file_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    fn = getattr(module, fn_name, None)
    if fn is None:
        raise AttributeError(
            f"Function '{fn_name}' not found in '{file_path}'"
        )

    if not callable(fn):
        raise TypeError(
            f"'{fn_name}' in '{file_path}' is not callable"
        )

    return fn
