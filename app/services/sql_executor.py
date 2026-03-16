from typing import Any

from sqlalchemy import create_engine, text


class SQLSecurityError(Exception):
    pass


def execute_sql_query(sql: str, connection_string: str) -> list[dict[str, Any]]:
    """Execute a read-only SQL SELECT query and return results as a list of dicts.

    Raises:
        SQLSecurityError: if the query is not a SELECT statement.
    """
    normalized = sql.strip().upper().lstrip("(")
    if not normalized.startswith("SELECT") and not normalized.startswith("WITH"):
        raise SQLSecurityError(
            f"Only SELECT and WITH queries are allowed. Got: {sql[:50]}"
        )

    engine = create_engine(connection_string)
    with engine.connect() as conn:
        result = conn.execute(text(sql))
        columns = list(result.keys())
        rows = result.fetchall()
        return [dict(zip(columns, row)) for row in rows]
