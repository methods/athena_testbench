import datetime
from operator import itemgetter

import pytest
from faker import Faker

from build_sql import build_sql
from utils.ScenarioBuilder import ScenarioBuilder
from utils.connections import presto_transaction, pg_connect
from utils.random_utils import n_days_ago, hash_nhs_number

NHS_NUMBER = 0
REF_ID = 1
PROVENANCE = 2
HAS_ESSENTIALS = 3
EVENT_DATE = 4
SYS_TIME_COL = 5
EVENT_INDEX = 6

# ============= TEST RECORDS ARE JOINING CORRECTLY ================

def test_status_records_limit_to_one_per_person(tmp_path: pytest.fixture):
    with pg_connect() as con:
        scenario_builder = ScenarioBuilder(con)

        # GIVEN
        # multiple ivr records have been added
        build_and_reset_data_sources(scenario_builder)

        add_registration_events(scenario_builder, [
            registration_event('1', 'IVR', 'NO', n_days_ago(5), n_days_ago(4), 1),
            registration_event('1', 'IVR', 'YES', n_days_ago(1), n_days_ago(1), 2),
            registration_event('2', 'IVR', 'NO', n_days_ago(4), n_days_ago(4), 1),
            registration_event('2', 'IVR', 'YES', n_days_ago(1), n_days_ago(1), 2),
            registration_event('3', 'IVR', 'NO', n_days_ago(5), n_days_ago(4), 1),
            registration_event('3', 'IVR', 'YES', n_days_ago(1), n_days_ago(1), 2),
        ])

        # WHEN
        # we build the registration timeline stack and run it in presto
        query = build_query(tmp_path, 'sql_to_build/registration_status_select.sql', parameters={'timestamp': n_days_ago(2)})
        results = presto_transaction(query)

        # THEN
        # all records are recorded on the registration timeline
        assert len(results) == 3


def test_status_returns_most_recent(tmp_path: pytest.fixture):
    with pg_connect() as con:
        scenario_builder = ScenarioBuilder(con)

        # GIVEN
        # multiple records have been added between 10 and 1 day ago
        build_and_reset_data_sources(scenario_builder)

        add_registration_events(scenario_builder, [
            registration_event('1', 'IVR', 'NO', n_days_ago(5), n_days_ago(10), 1),
            registration_event('1', 'WEB', 'YES', n_days_ago(1), n_days_ago(8), 2), # expected
            registration_event('1', 'IVR', 'NO', n_days_ago(4), n_days_ago(6), 3),
            registration_event('1', 'local authority', 'YES', n_days_ago(1), n_days_ago(1), 4)
        ])

        # WHEN
        # we query for the status 7 days ago
        query = build_query(tmp_path, 'sql_to_build/registration_status_select.sql', parameters={'timestamp': n_days_ago(7)})
        results = presto_transaction(query)

        # THEN
        # we expect to get back the result from 8 days ago
        assert len(results) == 1
        assert results[0][EVENT_INDEX] == 2


def test_status_returns_most_recent_from_multiple(tmp_path: pytest.fixture):
    with pg_connect() as con:
        scenario_builder = ScenarioBuilder(con)

        # GIVEN
        # multiple records have been added between 10 and 1 day ago
        build_and_reset_data_sources(scenario_builder)

        add_registration_events(scenario_builder, [
            registration_event('1', 'IVR', 'NO', n_days_ago(5), n_days_ago(10), 1),
            registration_event('1', 'WEB', 'YES', n_days_ago(1), n_days_ago(8), 2), # expected
            registration_event('1', 'IVR', 'NO', n_days_ago(4), n_days_ago(6), 3),
            registration_event('2', 'WEB', 'NO', n_days_ago(1), n_days_ago(10), 1), # expected
            registration_event('2', 'local authority', 'YES', n_days_ago(1), n_days_ago(1), 2)
        ])

        # WHEN
        # we query for the status 7 days ago
        query = build_query(tmp_path, 'sql_to_build/registration_status_select.sql', parameters={'timestamp': n_days_ago(7)})
        results = presto_transaction(query)

        # THEN
        # we expect to get back the result from 8 days ago
        assert len(results) == 2
        results.sort(key=itemgetter(NHS_NUMBER))
        assert results[0][EVENT_INDEX] == 2
        assert results[1][EVENT_INDEX] == 1


def test_status_returns_none_if_no_event_before_time(tmp_path: pytest.fixture):
    with pg_connect() as con:
        scenario_builder = ScenarioBuilder(con)

        # GIVEN
        # multiple records have been added between 10 and 1 day ago
        build_and_reset_data_sources(scenario_builder)

        add_registration_events(scenario_builder, [
            registration_event('1', 'IVR', 'NO', n_days_ago(5), n_days_ago(10), 1),
            registration_event('1', 'WEB', 'YES', n_days_ago(1), n_days_ago(8), 2),
            registration_event('1', 'IVR', 'NO', n_days_ago(4), n_days_ago(6), 3),
        ])

        # WHEN
        # we query for the status 7 days ago
        query = build_query(tmp_path, 'sql_to_build/registration_status_select.sql', parameters={'timestamp': n_days_ago(30)})
        results = presto_transaction(query)

        # THEN
        # we expect to get no results
        assert len(results) == 0


def test_status_returns_one_status_even_if_two_events_are_latest(tmp_path: pytest.fixture):
    with pg_connect() as con:
        scenario_builder = ScenarioBuilder(con)

        # GIVEN
        # multiple records have been added between 10 and 1 day ago
        build_and_reset_data_sources(scenario_builder)

        add_registration_events(scenario_builder, [
            registration_event('1', 'IVR', 'NO', n_days_ago(5), n_days_ago(10), 1),
            registration_event('1', 'WEB', 'YES', n_days_ago(1), n_days_ago(8), 2), # expected
            registration_event('1', 'WEB', 'YES', n_days_ago(1), n_days_ago(8), 3), # expected
            registration_event('1', 'IVR', 'NO', n_days_ago(4), n_days_ago(6), 4),
            registration_event('1', 'local authority', 'YES', n_days_ago(1), n_days_ago(1), 5)
        ])

        # WHEN
        # we query for the status 7 days ago
        query = build_query(tmp_path, 'sql_to_build/registration_status_select.sql', parameters={'timestamp': n_days_ago(7)})
        results = presto_transaction(query)

        # THEN
        # we expect to get back the result from 8 days ago
        assert len(results) == 1
        assert results[0][EVENT_INDEX] == 3

# ============= END TESTS ================



def build_and_reset_data_sources(scenario_builder):
    build_timeline_as_table(scenario_builder)


def add_registration_events(scenario_builder, events):
    scenario_builder.insert_multiple_into_arbitrary_table(
        "registration_timeline", events
    )


def registration_event(nhs_number, provenance, has_access_to_essential_supplies, event_datetime, ingested_datetime, event_order):
    return {
        'nhs_number': nhs_number,
        'id': hash_nhs_number(nhs_number),
        'provenance': provenance,
        'has_access_to_essential_supplies': has_access_to_essential_supplies,
        'event_datetime': event_datetime,
        'ingested_datetime': ingested_datetime,
        'event_order': event_order
    }


def build_timeline_as_table(scenario_builder):
    table_schema = {
        'nhs_number': 'TEXT',
        'id': 'TEXT',
        'provenance': 'TEXT',
        'has_access_to_essential_supplies': 'TEXT',
        'event_datetime': 'TIMESTAMP',
        'ingested_datetime': 'TIMESTAMP',
        'event_order': 'NUMERIC'
    }
    scenario_builder.build_arbitrary_table("registration_timeline", table_schema)


def build_query(tmp_path, template, parameters):
    query_file = f"{tmp_path}/built.sql"
    build_sql(template_file=template, output_file=query_file, parameters=parameters)
    with open(query_file, 'r') as f:
        query = f.read()
    return query
