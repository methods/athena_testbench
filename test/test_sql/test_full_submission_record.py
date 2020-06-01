import datetime

import pytest

from build_sql import build_sql
from utils.ScenarioBuilder import ScenarioBuilder
from utils.connections import presto_transaction, pg_connect
from utils.random_utils import n_days_ago, random_nhs_number


def test_full_submission_record_feeds_through_all_latest_submissions(tmp_path: pytest.fixture):
    with pg_connect() as con:
        scenario_builder = ScenarioBuilder(con)

        # GIVEN
        # the tables are all built and reset
        build_and_reset_data_sources(scenario_builder)

        submission_data = [
            latest_submission_row('1', 'NO', n_days_ago(n=1)),
            latest_submission_row('2', 'NO', n_days_ago(n=1)),
            latest_submission_row('3', 'NO', n_days_ago(n=1)),
        ]
        scenario_builder.insert_multiple_into_arbitrary_table(
            "latest_submission", submission_data
        )

        # WHEN
        # we build the latest feedback deregister stack and run it in presto
        query = read_query('sql_to_build/full_submission_record.sql')
        results = presto_transaction(query)

        # THEN
        # the results are 3 lines long and include all the codes
        assert len(results) == 3

        nhs_numbers = [result[1] for result in results]
        assert set(nhs_numbers) == {'1', '2', '3'}


def test_full_submission_record_appends_opt_in_if_present():
    with pg_connect() as con:
        scenario_builder = ScenarioBuilder(con)

        # GIVEN
        # the tables are all built and reset
        build_and_reset_data_sources(scenario_builder)

        submission_data = [
            latest_submission_row('1', 'NO', n_days_ago(n=1))
        ]
        scenario_builder.insert_multiple_into_arbitrary_table(
            "latest_submission", submission_data
        )

        opt_out_data = [
            latest_la_feedback_opt_out_row('1', 'F002', n_days_ago(1), 'expected')
        ]
        scenario_builder.insert_multiple_into_arbitrary_table(
            "latest_la_feedback_to_stop_boxes", opt_out_data
        )

        # WHEN
        # we build the latest feedback deregister stack and run it in presto
        query = read_query('sql_to_build/full_submission_record.sql')
        results = presto_transaction(query)

        # THEN
        # the results are 1 lines long and includes all the codes
        assert len(results) == 1

        assert 'F002' in results[0]
        assert 'expected' in results[0]


def test_full_submission_record_appends_opt_out_if_present():
    with pg_connect() as con:
        scenario_builder = ScenarioBuilder(con)

        # GIVEN
        # the tables are all built and reset
        build_and_reset_data_sources(scenario_builder)

        submission_data = [
            latest_submission_row('1', 'NO', n_days_ago(n=1))
        ]
        scenario_builder.insert_multiple_into_arbitrary_table(
            "latest_submission", submission_data
        )

        opt_in_data = [
            latest_la_feedback_opt_in_row('1', 'F003', n_days_ago(1), 'expect opt out')
        ]
        scenario_builder.insert_multiple_into_arbitrary_table(
            "latest_la_feedback_to_continue_boxes", opt_in_data
        )

        # WHEN
        # we build the latest feedback deregister stack and run it in presto
        query = read_query('sql_to_build/full_submission_record.sql')
        results = presto_transaction(query)

        # THEN
        # the results are 1 lines long and includes all the codes
        assert len(results) == 1

        assert 'F003' in results[0]
        assert 'expect opt out' in results[0]


def test_full_submission_record_appends_both_opt_in_and_opt_out_if_present():
    with pg_connect() as con:
        scenario_builder = ScenarioBuilder(con)

        # GIVEN
        # the tables are all built and reset
        build_and_reset_data_sources(scenario_builder)

        submission_data = [
            latest_submission_row('1', 'NO', n_days_ago(n=1))
        ]
        scenario_builder.insert_multiple_into_arbitrary_table(
            "latest_submission", submission_data
        )

        opt_in_data = [
            latest_la_feedback_opt_in_row('1', 'F003', n_days_ago(1), 'expect opt in')
        ]
        scenario_builder.insert_multiple_into_arbitrary_table(
            "latest_la_feedback_to_continue_boxes", opt_in_data
        )

        opt_out_data = [
            latest_la_feedback_opt_in_row('1', 'F002', n_days_ago(1), 'expect opt out')
        ]
        scenario_builder.insert_multiple_into_arbitrary_table(
            "latest_la_feedback_to_stop_boxes", opt_out_data
        )

        # WHEN
        # we build the latest feedback deregister stack and run it in presto
        query = read_query('sql_to_build/full_submission_record.sql')
        results = presto_transaction(query)

        # THEN
        # the results are 1 lines long and includes all the codes
        assert len(results) == 1

        assert 'F002' in results[0]
        assert 'F003' in results[0]
        assert 'expect opt in' in results[0]
        assert 'expect opt out' in results[0]


def test_resolved_has_access_gives_submission_value():
    pass


def test_resolved_has_access_gives_opt_out_if_latest_feedback():
    pass


def test_resolved_has_access_gives_opt_in_if_latest_feedback():
    pass


def test_resolved_has_access_gives_opt_out_if_opt_out_and_opt_in_on_same_day():
    pass


def test_resolved_has_access_overrides_opt_out_with_later_web_submission():
    pass


def test_resolved_has_access_overrides_opt_out_with_web_submission_on_same_day():
    pass


def latest_la_feedback_opt_in_row(nhs_number, feedback_code, feedback_time, feedback_comments):
    return {
        'nhs_number': nhs_number,
        'feedback_code': feedback_code,
        'feedback_time': feedback_time,
        'feedback_comments': feedback_comments,
    }


def latest_la_feedback_opt_out_row(nhs_number, feedback_code, feedback_time, feedback_comments):
    return {
        'nhs_number': nhs_number,
        'feedback_code': feedback_code,
        'feedback_time': feedback_time,
        'feedback_comments': feedback_comments,
    }


def latest_submission_row(nhs_number, has_access_to_essential_supplies, submission_time,
                          is_able_to_carry_supplies='YES', email_address=None, phone_number_calls=None,
                          phone_number_texts=None, provenance='IVR'):
    return {
        'provenance': provenance,
        'nhs_number': nhs_number,
        'submission_time': submission_time,
        'has_access_to_essential_supplies': has_access_to_essential_supplies,
        'is_able_to_carry_supplies': is_able_to_carry_supplies,
        'email_address': email_address if email_address else f'{nhs_number}@example.com',
        'phone_number_calls': phone_number_calls if phone_number_calls else f'(+44){nhs_number}',
        'phone_number_texts': phone_number_texts if phone_number_texts else f'(+44){nhs_number}'
    }


def build_and_reset_data_sources(scenario_builder):
    build_latest_submissions_as_table(scenario_builder)
    build_latest_la_feedback_opt_in_as_table(scenario_builder)
    build_latest_la_feedback_opt_out_as_table(scenario_builder)


def build_latest_la_feedback_opt_in_as_table(scenario_builder):
    table_schema = {
        'nhs_number': 'TEXT',
        'feedback_code': 'TEXT',
        'feedback_time': 'DATE',
        'feedback_comments': 'TEXT',
    }
    scenario_builder.build_arbitrary_table("latest_la_feedback_to_continue_boxes", table_schema)


def build_latest_la_feedback_opt_out_as_table(scenario_builder):
    table_schema = {
        'nhs_number': 'TEXT',
        'feedback_code': 'TEXT',
        'feedback_time': 'DATE',
        'feedback_comments': 'TEXT',
    }
    scenario_builder.build_arbitrary_table("latest_la_feedback_to_stop_boxes", table_schema)


def build_latest_submissions_as_table(scenario_builder):
    table_schema = {
        'provenance': 'TEXT',
        'nhs_number': 'TEXT',
        'submission_time': 'DATE',
        'has_access_to_essential_supplies': 'TEXT',
        'is_able_to_carry_supplies': 'TEXT',
        'email_address': 'TEXT',
        'phone_number_calls': 'TEXT',
        'phone_number_texts': 'TEXT'
    }
    scenario_builder.build_arbitrary_table("latest_submission", table_schema)


def read_query(sql_file):
    with open(sql_file, 'r') as f:
        query = f.read()
    return query
