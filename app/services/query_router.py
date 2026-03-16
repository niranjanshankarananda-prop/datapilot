from enum import Enum


class QueryRoute(str, Enum):
    PANDAS = "pandas"
    SQL = "sql"


def route_query(dataset_type: str) -> QueryRoute:
    """Route a query to the appropriate execution path based on dataset type."""
    if dataset_type == "database":
        return QueryRoute.SQL
    return QueryRoute.PANDAS
