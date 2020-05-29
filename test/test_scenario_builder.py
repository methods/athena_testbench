import pytest

from utils.ScenarioBuilder import ScenarioBuilder

@pytest.fixture()
def scenario_builder():
    scenario_builder = ScenarioBuilder()
    scenario_builder.reset()
    yield scenario_builder
    scenario_builder.reset()

def test_scenario_builder_does_populate_database(scenario_builder):
    scenario_builder.insert_random_nhs_records(count=10)
    scenario_builder.insert_random_ivr_records(count=10)
    scenario_builder.insert_random_web_records(count=10)
    scenario_builder.insert_random_la_feedback(count=10)


def test_scenario_builder_builds_arbitrary_table(scenario_builder):
    table_schema = {
        'test_col_1': 'TEXT',
        'tes_col_2': 'TEXT',
    }
    scenario_builder.build_arbitrary_table("test_table", table_schema)
    postgres_tables = scenario_builder.list_postgres_tables()

    assert "test_table" in postgres_tables

