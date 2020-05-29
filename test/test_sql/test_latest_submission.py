import pytest

from utils.ScenarioBuilder import ScenarioBuilder
from utils.connections import presto_connect


@pytest.fixture()
def scenario_builder():
    scenario_builder = ScenarioBuilder()
    scenario_builder.reset()
    yield scenario_builder
    scenario_builder.reset()


def test_latest_submission_with_multiple_ivr_entries(scenario_builder):
    # GIVEN A SCENARIO WITH KNOWN LA FEEDBACK
    table_schema = {
        'provenance': 'TEXT',
        'nhs_number': 'TEXT',
        'submission_time': 'TEXT',
        'has_access_to_essential_supplies': 'TEXT',
        'is_able_to_carry_supplies': 'TEXT',
        'email_address': 'TEXT',
        'phone_number_calls': 'TEXT',
        'phone_number_texts': 'TEXT'
    }
    scenario_builder.build_arbitrary_table("all_submissions", table_schema)
    scenario_builder.insert_into_arbitrary_table(
        "all_submissions",
        {
            'provenance': 'IVR',
            'nhs_number': '123',
            'submission_time': '2010-11-01 00:00:00',
            'has_access_to_essential_supplies': 'YES',
            'is_able_to_carry_supplies': 'YES',
            'email_address': 'notreal@notmail.com',
            'phone_number_calls': '12345',
            'phone_number_texts': '12345'
        }
    )
    scenario_builder.insert_into_arbitrary_table(
        "all_submissions",
        {
            'provenance': 'IVR',
            'nhs_number': '123',
            'submission_time': '2010-11-02 00:00:00',
            'has_access_to_essential_supplies': 'YES',
            'is_able_to_carry_supplies': 'YES',
            'email_address': 'notreal@notmail.com',
            'phone_number_calls': '12345',
            'phone_number_texts': '12345'
        }
    )
    scenario_builder.insert_into_arbitrary_table(
        "all_submissions",
        {
            'provenance': 'IVR',
            'nhs_number': '123',
            'submission_time': '2010-11-03 00:00:00',
            'has_access_to_essential_supplies': 'EXPECTED_ROW',
            'is_able_to_carry_supplies': 'YES',
            'email_address': 'notreal@notmail.com',
            'phone_number_calls': '12345',
            'phone_number_texts': '12345'
        }
    )
    scenario_builder.insert_into_arbitrary_table(
        "all_submissions",
        {
            'provenance': 'IVR',
            'nhs_number': '123',
            'submission_time': '2010-10-01 00:00:00',
            'has_access_to_essential_supplies': 'YES',
            'is_able_to_carry_supplies': 'YES',
            'email_address': 'notreal@notmail.com',
            'phone_number_calls': '12345',
            'phone_number_texts': '12345'
        }
    )
    con = presto_connect()

    with open('sql_to_build/latest_submission.sql', 'r') as f:
        query = f.read()

    cur = con.cursor()
    cur.execute(query)
    rows = cur.fetchall()

    assert len(rows) == 1
    assert rows[0][4] == ['EXPECTED_ROW']


