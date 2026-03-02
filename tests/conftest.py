import pandas as pd
import pytest
from pathlib import Path

FIXTURES_DIR = Path(__file__).parent / "fixtures" / "sample_csvs"


@pytest.fixture
def sales_df() -> pd.DataFrame:
    return pd.read_csv(FIXTURES_DIR / "sales_data.csv")


@pytest.fixture
def customer_df() -> pd.DataFrame:
    return pd.read_csv(FIXTURES_DIR / "customer_data.csv")


@pytest.fixture
def web_analytics_df() -> pd.DataFrame:
    return pd.read_csv(FIXTURES_DIR / "web_analytics.csv")


@pytest.fixture
def financial_df() -> pd.DataFrame:
    return pd.read_csv(FIXTURES_DIR / "financial_data.csv")


@pytest.fixture
def survey_df() -> pd.DataFrame:
    return pd.read_csv(FIXTURES_DIR / "survey_results.csv")


@pytest.fixture
def sales_schema() -> list[dict]:
    return [
        {"name": "product_id", "dtype": "object", "sample": "P001"},
        {"name": "product_name", "dtype": "object", "sample": "Laptop Pro"},
        {"name": "category", "dtype": "object", "sample": "Electronics"},
        {"name": "revenue", "dtype": "float64", "sample": "1299.99"},
        {"name": "units_sold", "dtype": "int64", "sample": "150"},
        {"name": "date", "dtype": "object", "sample": "2024-01-15"},
    ]


@pytest.fixture
def customer_schema() -> list[dict]:
    return [
        {"name": "customer_id", "dtype": "object", "sample": "C001"},
        {"name": "first_name", "dtype": "object", "sample": "John"},
        {"name": "last_name", "dtype": "object", "sample": "Smith"},
        {"name": "email", "dtype": "object", "sample": "john.smith@email.com"},
        {"name": "segment", "dtype": "object", "sample": "Enterprise"},
        {"name": "signup_date", "dtype": "object", "sample": "2023-01-15"},
        {"name": "total_purchases", "dtype": "int64", "sample": "45"},
        {"name": "lifetime_value", "dtype": "float64", "sample": "12500.00"},
    ]


@pytest.fixture
def financial_schema() -> list[dict]:
    return [
        {"name": "transaction_id", "dtype": "object", "sample": "TXN001"},
        {"name": "transaction_date", "dtype": "object", "sample": "2024-01-15"},
        {"name": "amount", "dtype": "float64", "sample": "150.25"},
        {"name": "category", "dtype": "object", "sample": "Groceries"},
        {"name": "payment_method", "dtype": "object", "sample": "Credit Card"},
        {"name": "status", "dtype": "object", "sample": "Completed"},
        {"name": "customer_id", "dtype": "object", "sample": "C001"},
    ]
