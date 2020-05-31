import datetime

import pytest

from build_sql import build_sql
from utils.ScenarioBuilder import ScenarioBuilder
from utils.connections import presto_transaction, pg_connect
from utils.random_utils import n_days_ago, random_nhs_number


def test_latest_feedback_deregister_template_selects_opt_out_feedback_codes(tmp_path: pytest.fixture):
    with pg_connect() as con:
        scenario_builder = ScenarioBuilder(con)

        # GIVEN
        # the all_la_feedback view will return mixed feedback codes
        build_all_la_feedback_as_table(scenario_builder)

        opt_out_codes = ['W003', 'W004', 'D001']
        feedback_data = [
            all_la_feedback_row('test_1_1', 'YF19', n_days_ago(n=10), ""),
            all_la_feedback_row('test_1_2', 'W003', n_days_ago(n=10), "opt out code"),
            all_la_feedback_row('test_1_3', 'RM19', n_days_ago(n=10), ""),
            all_la_feedback_row('test_1_4', 'D001', n_days_ago(n=10), "opt out code"),
            all_la_feedback_row('test_1_5', 'W004', n_days_ago(n=10), "opt out code"),
            all_la_feedback_row('test_1_6', 'YF19', n_days_ago(n=10), "")
        ]

        scenario_builder.insert_multiple_into_arbitrary_table(
            "all_la_feedback", feedback_data
        )

        # WHEN
        # we build the latest feedback deregister stack and run it in presto
        query = build_query(tmp_path, 'sql_to_build/latest_la_feedback_opt_out_stack_TEMPLATE.sql')
        results = presto_transaction(query)

        # THEN
        # the results are 3 lines long and include all the codes
        assert len(results) == 3

        codes = [result[3] for result in results]
        assert set(codes) == set(opt_out_codes)


def test_latest_feedback_opt_out_gets_latest_opt_out_row(tmp_path: pytest.fixture):
    with pg_connect() as con:
        scenario_builder = ScenarioBuilder(con)

        # GIVEN
        # the all_la_feedback view will return feedback for one person at multiple times
        test_nhs_number = f'{random_nhs_number()}'
        feedback_data = [
            all_la_feedback_row(test_nhs_number, 'YF19', n_days_ago(n=10), ""),
            all_la_feedback_row(test_nhs_number, 'W003', n_days_ago(n=9), ""),
            all_la_feedback_row(test_nhs_number, 'RM19', n_days_ago(n=8), ""),
            all_la_feedback_row(test_nhs_number, 'D001', n_days_ago(n=7), ""),
            all_la_feedback_row(test_nhs_number, 'W004', n_days_ago(n=6), "expected"),
            all_la_feedback_row(test_nhs_number, 'YF19', n_days_ago(n=5), "")
        ]

        scenario_builder.insert_multiple_into_arbitrary_table(
            "all_la_feedback", feedback_data
        )

        # WHEN
        # we build the latest feedback deregister stack and run it in presto
        query = build_query(tmp_path, 'sql_to_build/latest_la_feedback_opt_out_stack_TEMPLATE.sql')
        results = presto_transaction(query)

        # THEN
        # the results for test_number is 1 lines long and has only the feedback from 6 days ago
        results_for_nhs_number = [result for result in results if result[2] == test_nhs_number]
        assert len(results_for_nhs_number) == 1
        assert results_for_nhs_number[0][3] == 'W004'


def test_latest_feedback_opt_out_returns_single_opt_out_row_if_two_on_the_same_day(tmp_path: pytest.fixture):
    with pg_connect() as con:
        scenario_builder = ScenarioBuilder(con)

        # GIVEN
        # the all_la_feedback view will return feedback for one person at multiple times
        # including two lots of opt out feedback on the most recent occasion
        test_nhs_number = f'{random_nhs_number()}'
        feedback_data = [
            all_la_feedback_row(test_nhs_number, 'YF19', n_days_ago(n=10), ""),
            all_la_feedback_row(test_nhs_number, 'D001', n_days_ago(n=9), ""),
            all_la_feedback_row(test_nhs_number, 'RM19', n_days_ago(n=8), ""),
            all_la_feedback_row(test_nhs_number, 'W003', n_days_ago(n=6), "either this one"),
            all_la_feedback_row(test_nhs_number, 'YF19', n_days_ago(n=6), ""),
            all_la_feedback_row(test_nhs_number, 'W004', n_days_ago(n=6), "or this one")
        ]

        scenario_builder.insert_multiple_into_arbitrary_table(
            "all_la_feedback", feedback_data
        )

        # WHEN
        # we build the latest feedback opt out stack and run it in presto
        query = build_query(tmp_path, 'sql_to_build/latest_la_feedback_opt_out_stack_TEMPLATE.sql')
        results = presto_transaction(query)

        # THEN
        # the results for test_number is 1 lines long and has one of the feedback results from 6 days ago
        results_for_nhs_number = [result for result in results if result[2] == test_nhs_number]
        assert len(results_for_nhs_number) == 1
        assert results_for_nhs_number[0][3] == 'W004' or results_for_nhs_number[0][3] == 'W003'


def all_la_feedback_row(nhs_number, feedback_code, feedback_time, feedback_comments):
    return {
        'nhs_number': nhs_number,
        'feedback_code': feedback_code,
        'feedback_time': feedback_time,
        'feedback_comments': feedback_comments,
    }


def build_all_la_feedback_as_table(scenario_builder):
    table_schema = {
        'nhs_number': 'TEXT',
        'feedback_code': 'TEXT',
        'feedback_time': 'TEXT',
        'feedback_comments': 'TEXT',
    }
    scenario_builder.build_arbitrary_table("all_la_feedback", table_schema)


def build_query(tmp_path, template):
    query_file = f"{tmp_path}/built.sql"
    build_sql(template_file=template, output_file=query_file)
    with open(query_file, 'r') as f:
        query = f.read()
    return query
