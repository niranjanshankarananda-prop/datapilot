import pytest
import pandas as pd

from app.sandbox.executor import execute_pandas_code, ExecutionError
from app.sandbox.validators import SecurityViolationError


class TestCodeExecutorSandbox:
    def test_allows_pandas_operations(self, sales_df):
        code = "result = df['revenue'].sum()"
        result = execute_pandas_code(code, sales_df)
        assert result is not None

    def test_allows_numpy_operations(self, sales_df):
        code = "result = np.mean(df['revenue'])"
        result = execute_pandas_code(code, sales_df)
        assert result is not None

    def test_allows_groupby_operations(self, sales_df):
        code = "result = df.groupby('category')['revenue'].sum()"
        result = execute_pandas_code(code, sales_df)
        assert result is not None

    def test_blocks_os_module(self, sales_df):
        code = "import os\nresult = os.getcwd()"
        with pytest.raises((SecurityViolationError, ExecutionError)):
            execute_pandas_code(code, sales_df)

    def test_blocks_subprocess(self, sales_df):
        code = "import subprocess\nresult = subprocess.run(['ls'])"
        with pytest.raises((SecurityViolationError, ExecutionError)):
            execute_pandas_code(code, sales_df)

    def test_blocks_open_function(self, sales_df):
        code = "result = open('test.txt', 'r')"
        with pytest.raises((SecurityViolationError, ExecutionError)):
            execute_pandas_code(code, sales_df)

    def test_blocks_eval(self, sales_df):
        code = "result = eval('1 + 1')"
        with pytest.raises((SecurityViolationError, ExecutionError)):
            execute_pandas_code(code, sales_df)

    def test_blocks_exec(self, sales_df):
        code = "exec('print(1)')"
        with pytest.raises((SecurityViolationError, ExecutionError)):
            execute_pandas_code(code, sales_df)

    def test_blocks_sys_module(self, sales_df):
        code = "import sys\nresult = sys.exit()"
        with pytest.raises((SecurityViolationError, ExecutionError)):
            execute_pandas_code(code, sales_df)

    def test_blocks_requests(self, sales_df):
        code = "import requests\nresult = requests.get('http://example.com')"
        with pytest.raises((SecurityViolationError, ExecutionError)):
            execute_pandas_code(code, sales_df)

    def test_blocks_shutil(self, sales_df):
        code = "import shutil\nresult = shutil.rmtree('/')"
        with pytest.raises((SecurityViolationError, ExecutionError)):
            execute_pandas_code(code, sales_df)

    def test_blocks_pathlib(self, sales_df):
        code = "from pathlib import Path\nresult = Path('/etc/passwd')"
        with pytest.raises((SecurityViolationError, ExecutionError)):
            execute_pandas_code(code, sales_df)

    def test_blocks_marshal(self, sales_df):
        code = "import marshal\nresult = marshal.dumps({})"
        with pytest.raises((SecurityViolationError, ExecutionError)):
            execute_pandas_code(code, sales_df)

    def test_returns_none_when_no_result(self, sales_df):
        code = "df['revenue'].sum()"
        result = execute_pandas_code(code, sales_df)
        assert result is None

    def test_handles_syntax_errors(self, sales_df):
        code = "result = df["
        with pytest.raises((SecurityViolationError, ExecutionError)):
            execute_pandas_code(code, sales_df)

    def test_preserves_original_dataframe(self, sales_df):
        original_sum = sales_df["revenue"].sum()
        code = "df['revenue'] = df['revenue'] * 2\nresult = df['revenue'].sum()"
        execute_pandas_code(code, sales_df)
        assert sales_df["revenue"].sum() == original_sum


class TestCodeExecutorResults:
    def test_returns_dict_from_dataframe(self, sales_df):
        code = "result = df.head(5).to_dict(orient='records')"
        result = execute_pandas_code(code, sales_df)
        assert isinstance(result, list)
        assert len(result) == 5

    def test_returns_series_as_dict(self, sales_df):
        code = "result = df['category'].value_counts().to_dict()"
        result = execute_pandas_code(code, sales_df)
        assert isinstance(result, dict)

    def test_returns_dataframe_when_no_result_var(self, sales_df):
        code = "df_sorted = df.sort_values('revenue', ascending=False)"
        result = execute_pandas_code(code, sales_df)
        assert isinstance(result, pd.DataFrame)

    def test_complex_aggregation(self, financial_df):
        code = "result = df.groupby('category')['amount'].agg(['sum', 'mean', 'count']).reset_index()"
        result = execute_pandas_code(code, financial_df)
        assert isinstance(result, pd.DataFrame)
        assert "sum" in result.columns
        assert "mean" in result.columns
        assert "count" in result.columns
