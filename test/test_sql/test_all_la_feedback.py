import pytest

from utils.ScenarioBuilder import ScenarioBuilder
from utils.connections import presto_connect


def test_all_la_feedback_dates():
    # GIVEN A SCENARIO WITH KNOWN LA FEEDBACK
    test_date = '2010-11-01 00:00:00.000'

    scenario_builder.insert_la_feedback('test_nhs_1', 'test_code', '2010-11-01 00:00:00', '')
    scenario_builder.insert_la_feedback('test_nhs_2', 'test_code', '2010-11-01 00:00:00.000', '')
    scenario_builder.insert_la_feedback('test_nhs_3', 'test_code', '01-11-2010', '')
    scenario_builder.insert_la_feedback('test_nhs_4', 'test_code', '01/11/2010', '')

    with open('sql_to_build/all_la_feedback.sql', 'r') as f:
        query = f.read()

    con = presto_connect()
    cur = con.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    con.commit()

    row_dates = [row[2] for row in rows]

    assert row_dates == ['2010-11-01 00:00:00.000']*4


