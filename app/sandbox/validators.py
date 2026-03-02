import ast
import re


class SecurityViolationError(Exception):
    pass


FORBIDDEN_PATTERNS = [
    r"\bos\.",
    r"\bsubprocess",
    r"\bopen\s*\(",
    r"\bfile\s*\(",
    r"\brequests\.",
    r"\bhttp",
    r"\bsocket\.",
    r"\beval\s*\(",
    r"\bexec\s*\(",
    r"\bcompile\s*\(",
    r"\b__import__",
    r"\bgetattr\s*\(",
    r"\bsetattr\s*\(",
    r"\bdelattr\s*\(",
    r"\binput\s*\(",
    r"\bprint\s*\(",
    r"\bsys\.",
    r"\bos\.path\.",
    r"\bshutil\.",
    r"\bpathlib\.",
    r"\bglob\.",
    r"\bsignal\.",
    r"\bthreading\.",
    r"\bmultiprocessing\.",
    r"\bsubprocess\.",
    r"\bpopen",
    r"\bspawn",
    r"\bfork",
    r"\bpickle\.",
    r"\bmarshal\.",
    r"\byaml\.",
    r"\bjson\.load",
    r"\bjson\.loads",
]

ALLOWED_FUNCTIONS = {
    "pandas": {
        "DataFrame",
        "Series",
        "read_csv",
        "read_excel",
        "read_json",
        "read_html",
        "read_sql",
        "read_parquet",
        "merge",
        "concat",
        "groupby",
        "agg",
        "sum",
        "mean",
        "median",
        "std",
        "var",
        "min",
        "max",
        "count",
        "describe",
        "head",
        "tail",
        "sort_values",
        "filter",
        "loc",
        "iloc",
        "drop",
        "fillna",
        "isnull",
        "notnull",
        "dropna",
        "rename",
        "assign",
        "apply",
        "map",
        "astype",
        "copy",
        "to_dict",
        "to_list",
        "to_numpy",
        "values",
        "shape",
        "columns",
        "dtypes",
        "index",
        "reset_index",
        "set_index",
        "unique",
        "nunique",
        "value_counts",
        "cumsum",
        "cumprod",
        "cummin",
        "cummax",
        "rolling",
        "expanding",
        "ewm",
        "pivot",
        "pivot_table",
        "crosstab",
        "melt",
        "query",
        "abs",
        "round",
        "ceil",
        "floor",
        "clip",
        "replace",
        "str",
        "dt",
        "cat",
        "plot",
    },
    "numpy": {
        "array",
        "nan",
        "inf",
        "pi",
        "e",
        "linspace",
        "arange",
        "zeros",
        "ones",
        "empty",
        "random",
        "mean",
        "std",
        "var",
        "min",
        "max",
        "sum",
        "prod",
        "cumsum",
        "cumprod",
        "abs",
        "ceil",
        "floor",
        "round",
        "where",
        "nanmean",
        "nanstd",
        "nanmin",
        "nanmax",
        "nansum",
        "nanprod",
    },
    "datetime": {
        "datetime",
        "date",
        "time",
        "timedelta",
        "timezone",
    },
}


def validate_code(code: str) -> None:
    for pattern in FORBIDDEN_PATTERNS:
        if re.search(pattern, code):
            raise SecurityViolationError(f"Forbidden pattern detected: {pattern}")

    try:
        tree = ast.parse(code)
    except SyntaxError as e:
        raise SecurityViolationError(f"Invalid Python syntax: {e}")

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name not in ALLOWED_FUNCTIONS:
                    raise SecurityViolationError(f"Import not allowed: {alias.name}")

        elif isinstance(node, ast.ImportFrom):
            if node.module not in ALLOWED_FUNCTIONS:
                raise SecurityViolationError(f"Import from not allowed: {node.module}")

        elif isinstance(node, ast.Call):
            if isinstance(node.func, ast.Name):
                if node.func.id == "eval" or node.func.id == "exec":
                    raise SecurityViolationError(f"Call not allowed: {node.func.id}")
            elif isinstance(node.func, ast.Attribute):
                if node.func.attr in ("eval", "exec", "compile"):
                    raise SecurityViolationError(
                        f"Attribute call not allowed: {node.func.attr}"
                    )
