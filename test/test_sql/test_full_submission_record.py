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

        # scenario_builder.insert_multiple_into_arbitrary_table(
        #     "all_la_feedback", feedback_data
        # )
        #
        # # WHEN
        # # we build the latest feedback deregister stack and run it in presto
        # query = build_query(tmp_path, 'sql_to_build/latest_la_feedback_opt_in_stack_TEMPLATE.sql')
        # results = presto_transaction(query)
        #
        # # THEN
        # # the results are 3 lines long and include all the codes
        # assert len(results) == 3
        #
        # codes = [result[3] for result in results]
        # assert set(codes) == set(opt_in_codes)


def test_full_submission_record_appends_opt_in_if_present():
    pass


def test_full_submission_record_appends_opt_out_if_present():
    pass


def test_full_submission_record_appends_both_opt_in_and_opt_out_if_present():
    pass


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
        'feedback_time': 'TEXT',
        'feedback_comments': 'TEXT',
    }
    scenario_builder.build_arbitrary_table("latest_la_feedback_to_continue_boxes", table_schema)


def build_latest_la_feedback_opt_out_as_table(scenario_builder):
    table_schema = {
        'nhs_number': 'TEXT',
        'feedback_code': 'TEXT',
        'feedback_time': 'TEXT',
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
