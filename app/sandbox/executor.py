from RestrictedPython import compile_restricted, safe_globals
from RestrictedPython import compile_restricted_eval
from RestrictedPython.Guards import full_write_guard

import pandas as pd
import numpy as np
import datetime

from app.sandbox.validators import validate_code, SecurityViolationError


class ExecutionError(Exception):
    pass


def _write_wrapper(obj):
    return full_write_guard(obj)


def _getattr_wrapper(obj, name, default=None):
    if name.startswith("_"):
        raise AttributeError(f"Attribute '{name}' is not allowed")
    return getattr(obj, name, default)


def _import_wrapper(name):
    allowed = {"pandas", "numpy", "datetime", "np", "pd"}
    if name not in allowed and name not in {"pd", "np"}:
        raise ImportError(f"Module '{name}' is not allowed")

    if name == "pd":
        return pd
    elif name == "np":
        return np
    elif name == "pandas":
        return pd
    elif name == "numpy":
        return np
    elif name == "datetime":
        return datetime

    raise ImportError(f"Module '{name}' is not allowed")


SAFE_GLOBALS = safe_globals.copy()
SAFE_GLOBALS["_write_"] = _write_wrapper
SAFE_GLOBALS["_getattr_"] = _getattr_wrapper
SAFE_GLOBALS["_import_"] = _import_wrapper
SAFE_GLOBALS["__builtins__"] = {}
SAFE_GLOBALS["pd"] = pd
SAFE_GLOBALS["np"] = np
SAFE_GLOBALS["datetime"] = datetime


def execute_pandas_code(code: str, df: pd.DataFrame) -> any:
    validate_code(code)

    try:
        compiled = compile_restricted(code, filename="<inline>", mode="exec")
    except Exception as e:
        raise ExecutionError(f"Failed to compile code: {e}")

    local_scope = {
        "df": df.copy(),
    }

    try:
        exec(compiled, SAFE_GLOBALS, local_scope)
    except SecurityViolationError:
        raise
    except Exception as e:
        raise ExecutionError(f"Execution failed: {e}")

    if "result" in local_scope:
        return local_scope["result"]
    elif "df" in local_scope:
        return local_scope["df"]
    else:
        return None
