from app.services.query_router import route_query, QueryRoute


def test_routes_file_dataset_to_pandas():
    assert route_query("file") == QueryRoute.PANDAS


def test_routes_database_dataset_to_sql():
    assert route_query("database") == QueryRoute.SQL


def test_unknown_type_defaults_to_pandas():
    assert route_query("csv") == QueryRoute.PANDAS
    assert route_query("") == QueryRoute.PANDAS
    assert route_query("unknown") == QueryRoute.PANDAS
