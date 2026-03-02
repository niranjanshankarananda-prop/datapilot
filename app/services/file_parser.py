import io
from typing import Literal

import pandas as pd


FileType = Literal["csv", "tsv", "xlsx", "xls"]


def detect_file_type(filename: str) -> FileType:
    """Detect file type from filename extension."""
    ext = filename.lower().split(".")[-1]
    if ext in ("csv", "tsv", "xlsx", "xls"):
        return ext
    raise ValueError(f"Unsupported file type: {ext}")


def parse_file(
    content: bytes,
    filename: str,
) -> pd.DataFrame:
    """Parse CSV, TSV, or Excel files into a Pandas DataFrame."""
    file_type = detect_file_type(filename)

    if file_type == "csv":
        df = pd.read_csv(io.BytesIO(content))
    elif file_type == "tsv":
        df = pd.read_csv(io.BytesIO(content), sep="\t")
    elif file_type in ("xlsx", "xls"):
        df = pd.read_excel(io.BytesIO(content))
    else:
        raise ValueError(f"Unsupported file type: {file_type}")

    return df
