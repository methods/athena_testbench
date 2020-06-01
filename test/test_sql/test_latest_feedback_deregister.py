import pytest

from utils.ScenarioBuilder import ScenarioBuilder
from utils.connections import presto_connect


@pytest.fixture()
def scenario_builder():
    scenario_builder = ScenarioBuilder()
    scenario_builder.reset()
    yield scenario_builder
    scenario_builder.reset()


def test_latest_feedback_register_selects_deregistrations(scenario_builder):

    deregistration_codes = ['W003', 'W004', 'D001', 'F002']

    mock_codes = ['YF19', 'VF22', 'VB6']

    table_schema = {
        'nhs_number': 'TEXT',
        'feedback_code': 'TEXT',
        'feedback_time': 'TEXT',
        'feedback_comments': 'TEXT',
    }

    scenario_builder.build_arbitrary_table("all_la_feedback", table_schema)

    for deregistration_code in deregistration_codes:

        expected_row = {
            'nhs_number': '123',
            'feedback_code': deregistration_code,
            'feedback_time': '2010-11-01 00:00:00',
            'feedback_comments': 'lorem',
        }

        mock_rows = [
            {
                'nhs_number': '123',
                'feedback_code': mock_code,
                'feedback_time': '2010-11-01 00:00:00',
                'feedback_comments': 'lorem',
            } for mock_code in mock_codes
        ]

        # insert expected code in the middle of the others...
        all_rows = mock_rows
        all_rows.insert(1, expected_row)


        for row in all_rows:
            scenario_builder.insert_into_arbitrary_table("all_la_feedback",row)


        with open('sql_to_build/latest_feedback_deregister.sql', 'r') as f:
            query = f.read()

        con = presto_connect()
        cur = con.cursor()
        cur.execute(query)
        rows = cur.fetchall()
        con.commit()

        assert len(rows) == 1
        assert all((item in list(expected_row.values()) for item in rows[0]))

        scenario_builder.soft_reset()


