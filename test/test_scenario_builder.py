import pytest

from utils.ScenarioBuilder import managed_scenario_builder
from utils.connections import pg_connect


def test_scenario_builder_does_populate_database(managed_scenario_builder):
    managed_scenario_builder.insert_random_nhs_records(count=10)
    managed_scenario_builder.insert_random_ivr_records(count=10)
    managed_scenario_builder.insert_random_web_records(count=10)
    managed_scenario_builder.insert_random_la_feedback(count=10)


def test_scenario_builder_builds_arbitrary_table(managed_scenario_builder):
    table_schema = {
        'test_col_1': 'TEXT',
        'tes_col_2': 'TEXT',
    }
    managed_scenario_builder.build_arbitrary_table("test_table", table_schema)
    postgres_tables = managed_scenario_builder.list_postgres_tables()

    assert "test_table" in postgres_tables

