import datetime

import pytest

from build_sql import build_sql
from utils.ScenarioBuilder import scenario_builder
from utils.connections import presto_transaction, pg_connect
from utils.random_utils import n_days_ago, random_nhs_number


def test_wholesaler_feedback_selects_opt_out_codes_only(tmp_path: pytest.fixture,
                                                        scenario_builder: pytest.fixture):
    scenario_builder.reset()

    # GIVEN
    # the clean_deliveries_outcome_staging table has a mix of outcome entries
    build_clean_deliveries_outcome_as_table(scenario_builder)

    OPT_OUT_CODE = '3'
    ANY_OTHER_CODE = '1'
    feedback_data = [
        clean_deliveries_outcome_row(id='1', deldate=n_days_ago(5), outcome=OPT_OUT_CODE, comment='expected'),
        clean_deliveries_outcome_row(id='2', deldate=n_days_ago(4), outcome=ANY_OTHER_CODE, comment=''),
        clean_deliveries_outcome_row(id='3', deldate=n_days_ago(3), outcome=OPT_OUT_CODE, comment='expected'),
        clean_deliveries_outcome_row(id='1', deldate=n_days_ago(2), outcome=ANY_OTHER_CODE, comment='')
    ]

    scenario_builder.insert_multiple_into_arbitrary_table(
        "clean_deliveries_outcome_staging", feedback_data
    )

    # WHEN
    # we build the latest wholesaler opt out stack and run it in presto
    query = build_query(tmp_path, 'sql_to_build/latest_wholesaler_opt_out_stack_TEMPLATE.sql')
    results = presto_transaction(query)

    # THEN
    # the 2 opt out lines are selected
    assert len(results) == 2
    assert results[0][3] == 'expected'
    assert results[0][3] == 'expected'


def test_wholesaler_feedback_selects_latest_opt_out_row(tmp_path: pytest.fixture,
                                                        scenario_builder: pytest.fixture):
    scenario_builder.reset()

    # GIVEN
    # the all_la_feedback view will return multiple opt out entries for one person
    build_clean_deliveries_outcome_as_table(scenario_builder)

    OPT_OUT_CODE = '3'
    feedback_data = [
        clean_deliveries_outcome_row(id='1', deldate=n_days_ago(5), outcome=OPT_OUT_CODE, comment=''),
        clean_deliveries_outcome_row(id='1', deldate=n_days_ago(1), outcome=OPT_OUT_CODE, comment='most recent'),
        clean_deliveries_outcome_row(id='1', deldate=n_days_ago(4), outcome=OPT_OUT_CODE, comment=''),
    ]

    scenario_builder.insert_multiple_into_arbitrary_table(
        "clean_deliveries_outcome_staging", feedback_data
    )

    # WHEN
    # we build the latest feedback deregister stack and run it in presto
    query = build_query(tmp_path, 'sql_to_build/latest_wholesaler_opt_out_stack_TEMPLATE.sql')
    results = presto_transaction(query)

    # THEN
    # only 1 result is returned and it is the most recent
    assert len(results) == 1
    assert results[0][3] == 'most recent'


def test_wholesaler_feedback_returns_one_row_if_multiple_latest(tmp_path: pytest.fixture,
                                                                scenario_builder: pytest.fixture):
    scenario_builder.reset()

    # GIVEN
    # the all_la_feedback view will return multiple opt out entries for one person on the latest day
    build_clean_deliveries_outcome_as_table(scenario_builder)

    OPT_OUT_CODE = '3'
    feedback_data = [
        clean_deliveries_outcome_row(id='1', deldate=n_days_ago(5), outcome=OPT_OUT_CODE, comment=''),
        clean_deliveries_outcome_row(id='1', deldate=n_days_ago(1), outcome=OPT_OUT_CODE, comment='most recent 1'),
        clean_deliveries_outcome_row(id='1', deldate=n_days_ago(1), outcome=OPT_OUT_CODE, comment='most recent 2'),
        clean_deliveries_outcome_row(id='1', deldate=n_days_ago(4), outcome=OPT_OUT_CODE, comment=''),
    ]

    scenario_builder.insert_multiple_into_arbitrary_table(
        "clean_deliveries_outcome_staging", feedback_data
    )

    # WHEN
    # we build the latest feedback deregister stack and run it in presto
    query = build_query(tmp_path, 'sql_to_build/latest_wholesaler_opt_out_stack_TEMPLATE.sql')
    results = presto_transaction(query)

    # THEN
    # only 1 result is returned and it is one of the most recent
    assert len(results) == 1
    assert {'most recent 1', 'most recent 2'}.__contains__(results[0][3])


def clean_deliveries_outcome_row(id, deldate, outcome, comment, company='Brakes', name='Test',
                                 address='somewhere', postcode='xxx xxx', ingested_datetime='10/05/2020 12:00:00',
                                 row_count='1'):
    return {
        'company': company,
        'id': id,
        'name': name,
        'address': address,
        'postcode': postcode,
        'deldate': deldate,
        'outcome': outcome,
        'comment': comment,
        'ingested_datetime': ingested_datetime,
        'row_count': row_count,
    }


def build_clean_deliveries_outcome_as_table(scenario_builder):
    table_schema = {
        'company': 'TEXT',
        'id': 'TEXT',
        'name': 'TEXT',
        'address': 'TEXT',
        'postcode': 'TEXT',
        'deldate': 'TEXT',
        'outcome': 'TEXT',
        'comment': 'TEXT',
        'ingested_datetime': 'TEXT',
        'row_count': 'TEXT',
    }
    scenario_builder.build_arbitrary_table("clean_deliveries_outcome_staging", table_schema)


def build_query(tmp_path, template):
    query_file = f"{tmp_path}/built.sql"
    build_sql(template_file=template, output_file=query_file)
    with open(query_file, 'r') as f:
        query = f.read()
    return query
