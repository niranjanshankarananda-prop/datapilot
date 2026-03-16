import pytest
import sqlite3
import tempfile
import os

from app.services.sql_executor import execute_sql_query, SQLSecurityError


def _make_test_db() -> tuple[str, str]:
    """Create a temp SQLite DB with test data. Returns (path, connection_string)."""
    f = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    f.close()
    conn = sqlite3.connect(f.name)
    conn.execute("CREATE TABLE sales (region TEXT, revenue REAL)")
    conn.execute("INSERT INTO sales VALUES ('North', 100.0)")
    conn.execute("INSERT INTO sales VALUES ('South', 200.0)")
    conn.commit()
    conn.close()
    return f.name, f"sqlite:///{f.name}"


def test_executes_select_and_returns_dicts():
    db_path, conn_str = _make_test_db()
    try:
        rows = execute_sql_query("SELECT region, revenue FROM sales ORDER BY revenue", conn_str)
        assert len(rows) == 2
        assert rows[0]["region"] == "North"
        assert rows[1]["revenue"] == 200.0
    finally:
        os.unlink(db_path)


def test_aggregation_query():
    db_path, conn_str = _make_test_db()
    try:
        rows = execute_sql_query(
            "SELECT region, SUM(revenue) AS total FROM sales GROUP BY region ORDER BY region",
            conn_str,
        )
        assert len(rows) == 2
        assert rows[0]["total"] == 100.0
    finally:
        os.unlink(db_path)


def test_blocks_non_select():
    _, conn_str = _make_test_db()
    with pytest.raises(SQLSecurityError):
        execute_sql_query("DROP TABLE sales", conn_str)
    with pytest.raises(SQLSecurityError):
        execute_sql_query("DELETE FROM sales WHERE 1=1", conn_str)
    with pytest.raises(SQLSecurityError):
        execute_sql_query("INSERT INTO sales VALUES ('East', 50)", conn_str)
