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
    if isinstance(obj, (pd.DataFrame, pd.Series, dict, list)):
        return obj
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


def _getitem_wrapper(obj, key):
    return obj[key]


def _getiter_wrapper(obj):
    return iter(obj)


def _inplacevar_wrapper(op, x, y):
    if op == "+=":
        return x + y
    elif op == "-=":
        return x - y
    elif op == "*=":
        return x * y
    elif op == "/=":
        return x / y
    raise ValueError(f"Unsupported in-place operator: {op}")


SAFE_GLOBALS = safe_globals.copy()
SAFE_GLOBALS["_write_"] = _write_wrapper
SAFE_GLOBALS["_getattr_"] = _getattr_wrapper
SAFE_GLOBALS["_getitem_"] = _getitem_wrapper
SAFE_GLOBALS["_getiter_"] = _getiter_wrapper
SAFE_GLOBALS["_inplacevar_"] = _inplacevar_wrapper
SAFE_GLOBALS["_import_"] = _import_wrapper
SAFE_GLOBALS["__builtins__"] = {}
SAFE_GLOBALS["pd"] = pd
SAFE_GLOBALS["np"] = np
SAFE_GLOBALS["datetime"] = datetime
SAFE_GLOBALS["len"] = len
SAFE_GLOBALS["range"] = range
SAFE_GLOBALS["list"] = list
SAFE_GLOBALS["dict"] = dict
SAFE_GLOBALS["str"] = str
SAFE_GLOBALS["int"] = int
SAFE_GLOBALS["float"] = float
SAFE_GLOBALS["bool"] = bool
SAFE_GLOBALS["tuple"] = tuple
SAFE_GLOBALS["sorted"] = sorted
SAFE_GLOBALS["sum"] = sum
SAFE_GLOBALS["min"] = min
SAFE_GLOBALS["max"] = max
SAFE_GLOBALS["abs"] = abs
SAFE_GLOBALS["round"] = round
SAFE_GLOBALS["enumerate"] = enumerate
SAFE_GLOBALS["zip"] = zip
SAFE_GLOBALS["print"] = lambda *a, **k: None


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
