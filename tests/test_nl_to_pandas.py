import pytest
from unittest.mock import patch, MagicMock
import pandas as pd

from app.services.nl_to_pandas import nl_to_pandas, generate_schema_info
from app.sandbox.executor import execute_pandas_code, ExecutionError


class TestNlToPandas:
    def test_generate_schema_info(self):
        schema = [
            {"name": "product_name", "dtype": "object", "sample": "Laptop"},
            {"name": "revenue", "dtype": "float64", "sample": "1299.99"},
        ]
        result = generate_schema_info(schema)
        assert "product_name: object (sample: Laptop)" in result
        assert "revenue: float64 (sample: 1299.99)" in result

    @patch("app.services.nl_to_pandas.Groq")
    def test_nl_to_pandas_total_revenue(self, mock_groq_class, sales_schema):
        mock_client = MagicMock()
        mock_groq_class.return_value = mock_client
        mock_response = MagicMock()
        mock_response.choices = [
            MagicMock(message=MagicMock(content="result = df['revenue'].sum()"))
        ]
        mock_client.chat.completions.create.return_value = mock_response

        code = nl_to_pandas("What is the total revenue?", sales_schema)

        assert "result" in code
        assert "revenue" in code.lower() or "sum" in code.lower()

    @patch("app.services.nl_to_pandas.Groq")
    def test_nl_to_pandas_top_products(self, mock_groq_class, sales_schema):
        mock_client = MagicMock()
        mock_groq_class.return_value = mock_client
        mock_response = MagicMock()
        mock_response.choices = [
            MagicMock(
                message=MagicMock(
                    content="result = df.sort_values('revenue', ascending=False).head(10).to_dict(orient='records')"
                )
            )
        ]
        mock_client.chat.completions.create.return_value = mock_response

        code = nl_to_pandas("Show top 10 products by sales", sales_schema)

        assert "result" in code
        assert "head" in code or "sort_values" in code

    @patch("app.services.nl_to_pandas.Groq")
    def test_nl_to_pandas_handles_non_existent_column(
        self, mock_groq_class, sales_schema
    ):
        mock_client = MagicMock()
        mock_groq_class.return_value = mock_client
        mock_response = MagicMock()
        mock_response.choices = [
            MagicMock(message=MagicMock(content="result = df['nonexistent'].sum()"))
        ]
        mock_client.chat.completions.create.return_value = mock_response

        code = nl_to_pandas("What is the total of non_existent_column?", sales_schema)

        assert "result" in code

    @patch("app.services.nl_to_pandas.Groq")
    def test_nl_to_pandas_strips_code_fences(self, mock_groq_class, sales_schema):
        mock_client = MagicMock()
        mock_groq_class.return_value = mock_client
        mock_response = MagicMock()
        mock_response.choices = [
            MagicMock(
                message=MagicMock(
                    content="```python\nresult = df['revenue'].sum()\n```"
                )
            )
        ]
        mock_client.chat.completions.create.return_value = mock_response

        code = nl_to_pandas("What is the total revenue?", sales_schema)

        assert not code.startswith("```")
        assert not code.endswith("```")

    @patch("app.services.nl_to_pandas.Groq")
    def test_nl_to_pandas_empty_response_raises(self, mock_groq_class, sales_schema):
        mock_client = MagicMock()
        mock_groq_class.return_value = mock_client
        mock_response = MagicMock()
        mock_response.choices = [MagicMock(message=MagicMock(content=""))]
        mock_client.chat.completions.create.return_value = mock_response

        with pytest.raises(ValueError, match="Empty response"):
            nl_to_pandas("What is the total revenue?", sales_schema)


class TestNlToPandasExecution:
    def test_total_revenue_execution(self, sales_df):
        code = "result = df['revenue'].sum()"
        result = execute_pandas_code(code, sales_df)
        assert isinstance(result, (int, float))

    def test_top_10_products_execution(self, sales_df):
        code = "result = df.sort_values('revenue', ascending=False).head(10)"
        result = execute_pandas_code(code, sales_df)
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 10

    def test_category_groupby(self, sales_df):
        code = "result = df.groupby('category')['revenue'].sum().reset_index()"
        result = execute_pandas_code(code, sales_df)
        assert isinstance(result, pd.DataFrame)
        assert "category" in result.columns
        assert "revenue" in result.columns
