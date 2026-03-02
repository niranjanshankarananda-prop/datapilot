from enum import Enum
from typing import Any

import pandas as pd
import numpy as np


class ResultType(str, Enum):
    TABLE = "table"
    NUMBER = "number"
    TEXT = "text"


def format_result(result: Any) -> dict:
    if result is None:
        return {"type": ResultType.TEXT, "value": "No result returned"}

    if isinstance(result, pd.DataFrame):
        return {
            "type": ResultType.TABLE,
            "value": result.to_dict(orient="records"),
            "columns": list(result.columns),
            "shape": list(result.shape),
        }

    if isinstance(result, pd.Series):
        return {
            "type": ResultType.TABLE,
            "value": result.to_dict(),
            "name": result.name,
        }

    if isinstance(result, (int, float, np.integer, np.floating)):
        return {
            "type": ResultType.NUMBER,
            "value": float(result)
            if isinstance(result, (np.integer, np.floating))
            else result,
        }

    if isinstance(result, (list, tuple)):
        result_list = list(result)
        # If list of dicts, extract column names for proper table rendering
        if result_list and isinstance(result_list[0], dict):
            columns = list(result_list[0].keys())
            return {
                "type": ResultType.TABLE,
                "value": result_list,
                "columns": columns,
                "shape": [len(result_list), len(columns)],
            }
        return {"type": ResultType.TABLE, "value": result_list}

    if isinstance(result, dict):
        return {"type": ResultType.TABLE, "value": result}

    return {"type": ResultType.TEXT, "value": str(result)}
