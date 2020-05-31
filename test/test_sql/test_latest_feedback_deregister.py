import datetime

import pytest

from utils.ScenarioBuilder import ScenarioBuilder
from utils.connections import presto_transaction, pg_connect


def test_latest_feedback_register_selects_deregistration():
    with pg_connect() as con:
        scenario_builder = ScenarioBuilder(con)

        table_schema = {
            'nhs_number': 'TEXT',
            'feedback_code': 'TEXT',
            'feedback_time': 'TEXT',
            'feedback_comments': 'TEXT',
        }
        scenario_builder.build_arbitrary_table("all_la_feedback", table_schema)

        feedback_data = [
            {
                'nhs_number': '123',
                'feedback_code': 'YF19',
                'feedback_time': '2010-11-01 00:00:00',
                'feedback_comments': 'lorem',
            },
            {
                'nhs_number': '123',
                'feedback_code': 'W003',
                'feedback_time': '2010-11-01 00:00:00',
                'feedback_comments': 'lorem',
            },
            {
                'nhs_number': '123',
                'feedback_code': 'VF22',
                'feedback_time': '2010-11-01 00:00:00',
                'feedback_comments': 'lorem',
            }, {
                'nhs_number': '123',
                'feedback_code': 'VB6',
                'feedback_time': '2010-11-01 00:00:00',
                'feedback_comments': 'lorem',
            }
        ]
        expected_row = feedback_data[1]

        scenario_builder.insert_multiple_into_arbitrary_table(
            "all_la_feedback", feedback_data
        )

        with open('sql_to_build/latest_feedback_deregister.sql', 'r') as f:
            query = f.read()

        results = presto_transaction(query)

        assert len(results) == 1
        assert all((item in list(expected_row.values()) for item in results[0]))
