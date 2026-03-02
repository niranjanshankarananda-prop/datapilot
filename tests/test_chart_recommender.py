import pytest
from unittest.mock import patch, MagicMock
import pandas as pd

from app.services.chart_recommender import (
    recommend_chart_type,
    _rule_based_recommendation,
)
from app.schemas.chart import ChartType


class TestChartRecommender:
    def test_recommends_bar_for_top_n(self, sales_df):
        result = recommend_chart_type(
            question="Show top 10 products by sales",
            result_df=sales_df,
        )
        assert result.chart_type == ChartType.BAR
        assert result.confidence >= 0.85

    def test_recommends_line_for_trend_over_time(self, sales_df):
        result = recommend_chart_type(
            question="What is the revenue trend over time?",
            result_df=sales_df,
        )
        assert result.chart_type == ChartType.LINE

    def test_recommends_bar_for_ranking(self, customer_df):
        result = recommend_chart_type(
            question="Which segment has the most customers?",
            result_df=customer_df,
        )
        assert result.chart_type == ChartType.BAR

    def test_recommends_pie_for_proportion(self, financial_df):
        result = recommend_chart_type(
            question="What is the proportion of spending by category?",
            result_df=financial_df,
        )
        assert result.chart_type == ChartType.PIE

    def test_recommends_histogram_for_distribution(self, sales_df):
        result = recommend_chart_type(
            question="What is the distribution of revenue?",
            result_df=sales_df,
        )
        assert result.chart_type == ChartType.HISTOGRAM

    def test_recommends_scatter_for_correlation(self, survey_df):
        result = recommend_chart_type(
            question="Is there a relationship between satisfaction and NPS?",
            result_df=survey_df,
        )
        assert result.chart_type == ChartType.SCATTER

    def test_ambiguous_question_produces_reasonable_result(self, sales_df):
        result = recommend_chart_type(
            question="Show me the data",
            result_df=sales_df,
        )
        assert result.chart_type in [ChartType.BAR, ChartType.LINE, ChartType.PIE]
        assert result.confidence >= 0.5

    def test_requires_either_result_df_or_columns(self):
        with pytest.raises(
            ValueError, match="Either result_df or columns must be provided"
        ):
            recommend_chart_type(question="Show data")

    def test_uses_columns_when_result_df_not_provided(self):
        columns = [
            {"name": "revenue", "type": "numeric"},
            {"name": "category", "type": "categorical"},
        ]
        result = recommend_chart_type(
            question="Show top categories by revenue",
            columns=columns,
        )
        assert result.chart_type in [ChartType.BAR, ChartType.LINE]

    def test_non_existent_column_returns_reasonable_result(self, sales_df):
        result = recommend_chart_type(
            question="What is the total of nonexistent_column?",
            result_df=sales_df,
        )
        assert result.chart_type in [
            ChartType.BAR,
            ChartType.LINE,
            ChartType.PIE,
            ChartType.HISTOGRAM,
        ]
        assert result.confidence >= 0.5

    @patch("app.services.chart_recommender.Groq")
    def test_ai_fallback_for_ambiguous(self, mock_groq_class, sales_df):
        mock_client = MagicMock()
        mock_groq_class.return_value = mock_client
        mock_response = MagicMock()
        mock_response.choices = [
            MagicMock(message=MagicMock(content="chart_type: bar\nreasoning: default"))
        ]
        mock_client.chat.completions.create.return_value = mock_response

        result = recommend_chart_type(
            question="Analyze this data",
            result_df=sales_df,
        )
        assert result.chart_type in ChartType


class TestRuleBasedRecommendation:
    def test_trend_keywords_triggers_line(self):
        columns = [
            {"name": "date", "type": "datetime"},
            {"name": "revenue", "type": "numeric"},
        ]
        result = _rule_based_recommendation("Show trend over time", columns)
        assert result is not None
        assert result.chart_type == ChartType.LINE

    def test_top_keywords_triggers_bar(self):
        columns = [
            {"name": "category", "type": "categorical"},
            {"name": "revenue", "type": "numeric"},
        ]
        result = _rule_based_recommendation("Show top categories", columns)
        assert result is not None
        assert result.chart_type == ChartType.BAR

    def test_proportion_keywords_triggers_pie(self):
        columns = [
            {"name": "category", "type": "categorical"},
            {"name": "revenue", "type": "numeric"},
        ]
        result = _rule_based_recommendation("What proportion", columns)
        assert result is not None
        assert result.chart_type == ChartType.PIE

    def test_correlation_keywords_triggers_scatter(self):
        columns = [
            {"name": "satisfaction", "type": "numeric"},
            {"name": "nps_score", "type": "numeric"},
        ]
        result = _rule_based_recommendation(
            "What is the correlation between x and y", columns
        )
        assert result is not None
        assert result.chart_type == ChartType.SCATTER

    def test_distribution_keywords_triggers_histogram(self):
        columns = [
            {"name": "revenue", "type": "numeric"},
        ]
        result = _rule_based_recommendation("Show distribution", columns)
        assert result is not None
        assert result.chart_type == ChartType.HISTOGRAM

    def test_returns_none_for_unclear_case(self):
        columns = [
            {"name": "value", "type": "numeric"},
        ]
        result = _rule_based_recommendation("Hello world", columns)
        assert result is None
