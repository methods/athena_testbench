import pytest

from utils.ScenarioBuilder import ScenarioBuilder, managed_scenario_builder
from utils.connections import presto_connect, presto_transaction, pg_connect
from utils.random_utils import n_days_ago


def test_latest_submission_picks_most_recent_submission(managed_scenario_builder):
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
    managed_scenario_builder.build_arbitrary_table("all_submissions", table_schema)

    submission_data = [
        {
            'provenance': 'IVR',
            'nhs_number': '123',
            'submission_time': n_days_ago(n=5),
            'has_access_to_essential_supplies': 'YES',
            'is_able_to_carry_supplies': 'YES',
            'email_address': 'notreal@notmail.com',
            'phone_number_calls': '12345',
            'phone_number_texts': '12345'
        },
        {
            'provenance': 'WEB',
            'nhs_number': '123',
            'submission_time': n_days_ago(n=1),
            'has_access_to_essential_supplies': 'NO',
            'is_able_to_carry_supplies': 'no',
            'email_address': 'test_mail@test_mail.com',
            'phone_number_calls': '0891505050',
            'phone_number_texts': '0891505050'
        },
        {
            'provenance': 'IVR',
            'nhs_number': '123',
            'submission_time': n_days_ago(n=3),
            'has_access_to_essential_supplies': 'YES',
            'is_able_to_carry_supplies': 'YES',
            'email_address': 'notreal@notmail.com',
            'phone_number_calls': '12345',
            'phone_number_texts': '12345'
        }, {
            'provenance': 'IVR',
            'nhs_number': '123',
            'submission_time': n_days_ago(n=4),
            'has_access_to_essential_supplies': 'YES',
            'is_able_to_carry_supplies': 'YES',
            'email_address': 'notreal@notmail.com',
            'phone_number_calls': '12345',
            'phone_number_texts': '12345'
        }
    ]
    expected_row = submission_data[1]

    managed_scenario_builder.insert_multiple_into_arbitrary_table(
        "all_submissions", submission_data
    )

    with open('sql_to_build/latest_submission.sql', 'r') as f:
        query = f.read()

    results = presto_transaction(query)

    managed_scenario_builder.reset()

    assert len(results) == 1
    assert all((item in list(expected_row.values()) for item in results[0]))
