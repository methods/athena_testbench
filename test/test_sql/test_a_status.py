import datetime

import pytest
from faker import Faker

from build_sql import build_sql
from utils.ScenarioBuilder import ScenarioBuilder
from utils.connections import presto_transaction, pg_connect
from utils.random_utils import n_days_ago, hash_nhs_number


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
