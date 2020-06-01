import pytest

from utils.ScenarioBuilder import ScenarioBuilder
from utils.connections import presto_transaction, pg_connect


def test_all_la_feedback_dates():
    # GIVEN A SCENARIO WITH KNOWN LA FEEDBACK
    with pg_connect() as con:
        scenario_builder = ScenarioBuilder(con)
        scenario_builder.reset()

        test_date = '2010-11-01 00:00:00.000'

        scenario_builder.insert_la_feedback('test_nhs_1', 'test_code', test_date, '')
        scenario_builder.insert_la_feedback('test_nhs_2', 'test_code', '2010-11-01 00:00:00.000', '')
        scenario_builder.insert_la_feedback('test_nhs_3', 'test_code', '01-11-2010', '')
        scenario_builder.insert_la_feedback('test_nhs_4', 'test_code', '01/11/2010', '')

        with open('sql_to_build/all_la_feedback.sql', 'r') as f:
            query = f.read()

        results = presto_transaction(query)

        row_dates = [row[2] for row in results]

        assert row_dates == [test_date] * 4
