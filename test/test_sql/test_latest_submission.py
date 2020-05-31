import pytest

from utils.ScenarioBuilder import ScenarioBuilder
from utils.connections import presto_connect


def test_latest_submission():


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
    expected_row = {
        'provenance': 'WEB',
        'nhs_number': '123',
        'submission_time': '2010-11-03 00:00:00',
        'has_access_to_essential_supplies': 'NO',
        'is_able_to_carry_supplies': 'no',
        'email_address': 'test_mail@test_mail.com',
        'phone_number_calls': '0891505050',
        'phone_number_texts': '0891505050'
    }

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
        expected_row
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

    with open('sql_to_build/latest_submission.sql', 'r') as f:
        query = f.read()

    con = presto_connect()
    cur = con.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    con.commit()

    assert len(rows) == 1
    assert all((item in list(expected_row.values()) for item in rows[0]))


