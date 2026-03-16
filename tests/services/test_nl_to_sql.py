import pytest
from unittest.mock import patch
from app.services.nl_to_sql import nl_to_sql


SCHEMA = [
    {"name": "revenue", "dtype": "REAL"},
    {"name": "region", "dtype": "TEXT"},
    {"name": "product", "dtype": "TEXT"},
]


def test_nl_to_sql_returns_select_query(monkeypatch):
    monkeypatch.setattr(
        "app.services.nl_to_sql._call_llm",
        lambda prompt, api_key: "SELECT region, SUM(revenue) AS total_revenue FROM sales GROUP BY region",
    )
    result = nl_to_sql("total revenue by region", SCHEMA, table_name="sales", api_key="gsk_fake")
    assert result.upper().startswith("SELECT")
    assert "sales" in result


def test_nl_to_sql_strips_markdown_fences(monkeypatch):
    monkeypatch.setattr(
        "app.services.nl_to_sql._call_llm",
        lambda prompt, api_key: "```sql\nSELECT * FROM orders\n```",
    )
    result = nl_to_sql("show all orders", SCHEMA, table_name="orders", api_key="gsk_fake")
    assert not result.startswith("```")
    assert "SELECT" in result.upper()


def test_nl_to_sql_raises_without_api_key():
    with patch("app.services.nl_to_sql.settings") as mock_settings:
        mock_settings.GROQ_API_KEY = None
        with pytest.raises(ValueError, match="No API key"):
            nl_to_sql("count rows", SCHEMA, table_name="t")
