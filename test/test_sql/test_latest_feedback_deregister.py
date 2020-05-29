import pytest

from utils.ScenarioBuilder import ScenarioBuilder
from utils.connections import presto_connect


@pytest.fixture()
def scenario_builder():
    scenario_builder = ScenarioBuilder()
    scenario_builder.reset()
    yield scenario_builder
    scenario_builder.reset()


def test_latest_feedback_register_selects_deregistration(scenario_builder):

    table_schema = {
        'nhs_number': 'TEXT',
        'feedback_code': 'TEXT',
        'feedback_time': 'TEXT',
        'feedback_comments': 'TEXT',
    }
    scenario_builder.build_arbitrary_table("all_la_feedback", table_schema)
    expected_row = {
        'nhs_number': '123',
        'feedback_code': 'W003',
        'feedback_time': '2010-11-01 00:00:00',
        'feedback_comments': 'lorem',
    }

    scenario_builder.insert_into_arbitrary_table(
        "all_la_feedback",
        {
            'nhs_number': '123',
            'feedback_code': 'YF19',
            'feedback_time': '2010-11-01 00:00:00',
            'feedback_comments': 'lorem',
        }
    )
    scenario_builder.insert_into_arbitrary_table(
        "all_la_feedback",
        expected_row
    )
    scenario_builder.insert_into_arbitrary_table(
        "all_la_feedback",
        {
            'nhs_number': '123',
            'feedback_code': 'VF22',
            'feedback_time': '2010-11-01 00:00:00',
            'feedback_comments': 'lorem',
        }
    )
    scenario_builder.insert_into_arbitrary_table(
        "all_la_feedback",
        {
            'nhs_number': '123',
            'feedback_code': 'VB6',
            'feedback_time': '2010-11-01 00:00:00',
            'feedback_comments': 'lorem',
        }
    )

    with open('sql_to_build/latest_feedback_deregister.sql', 'r') as f:
        query = f.read()

    con = presto_connect()
    cur = con.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    con.commit()

    assert len(rows) == 1
    assert all((item in list(expected_row.values()) for item in rows[0]))


