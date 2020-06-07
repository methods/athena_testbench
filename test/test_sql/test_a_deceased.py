import pytest

from build_sql import build_sql
from utils.ScenarioBuilder import ScenarioBuilder
from utils.connections import presto_transaction, pg_connect
from utils.random_utils import n_days_ago, hash_nhs_number


# ============= TESTS ================

def test_person_is_reported_as_deceased(tmp_path: pytest.fixture):
    with pg_connect() as con:
        scenario_builder = ScenarioBuilder(con)

        # GIVEN
        # a person is registered then reported as deceased
        build_and_reset_data_sources(scenario_builder)

        add_registration_events(scenario_builder, [
            registration_event('1', 'IVR', 'NO', n_days_ago(5), n_days_ago(4), 1),
            registration_event('1', 'local authority', 'YES', n_days_ago(5), n_days_ago(4), 2, event_code='D001'),
        ])

        # WHEN
        # we build the is deceased stack
        query = build_query(tmp_path, 'sql_to_build/is_deceased_from_timeline/is_deceased_from_timeline_test_stack.sql')
        results = presto_transaction(query)

        # THEN
        # a record for the person exists
        assert len(results) == 1


def test_person_can_be_timeline_deceased_even_if_they_have_not_been_registered(
        tmp_path: pytest.fixture):
    with pg_connect() as con:
        scenario_builder = ScenarioBuilder(con)

        # GIVEN
        # a person is registered then reported as deceased
        build_and_reset_data_sources(scenario_builder)

        add_registration_events(scenario_builder, [
            registration_event('1', 'local authority', 'YES', n_days_ago(5), n_days_ago(4), 2, event_code='D001'),
        ])

        # WHEN
        # we build the is deceased stack
        query = build_query(tmp_path, 'sql_to_build/is_deceased_from_timeline/is_deceased_from_timeline_test_stack.sql')
        results = presto_transaction(query)

        # THEN
        # a record for the person exists
        assert len(results) == 1


def test_person_is_not_timeline_deceasesd_if_reregistered_after_the_deceased_feedback(
        tmp_path: pytest.fixture):
    with pg_connect() as con:
        scenario_builder = ScenarioBuilder(con)

        # GIVEN
        # a person is registered then reported as deceased then reregistered
        build_and_reset_data_sources(scenario_builder)

        add_registration_events(scenario_builder, [
            registration_event('1', 'IVR', 'NO', n_days_ago(5), n_days_ago(4), 1),
            registration_event('1', 'local authority', 'YES', n_days_ago(5), n_days_ago(4), 2, event_code='D001'),
            registration_event('1', 'IVR', 'NO', n_days_ago(5), n_days_ago(4), 3),
        ])

        # WHEN
        # we build the is deceased stack
        query = build_query(tmp_path, 'sql_to_build/is_deceased_from_timeline/is_deceased_from_timeline_test_stack.sql')
        results = presto_transaction(query)

        # THEN
        # no record for the person exists
        assert len(results) == 0


def test_person_is_timeline_deceased_even_if_deregistered_with_a_different_code_after(
        tmp_path: pytest.fixture):
    with pg_connect() as con:
        scenario_builder = ScenarioBuilder(con)

        # GIVEN
        # a person is registered then reported as deceased then reregistered
        build_and_reset_data_sources(scenario_builder)

        add_registration_events(scenario_builder, [
            registration_event('1', 'IVR', 'NO', n_days_ago(5), n_days_ago(4), 1),
            registration_event('1', 'local authority', 'YES', n_days_ago(5), n_days_ago(4), 2, event_code='D001'),
            registration_event('1', 'wholesaler', 'YES', n_days_ago(5), n_days_ago(4), 3, event_code='3'),
        ])

        # WHEN
        # we build the is deceased stack
        query = build_query(tmp_path, 'sql_to_build/is_deceased_from_timeline/is_deceased_from_timeline_test_stack.sql')
        results = presto_transaction(query)

        # THEN
        # no record for the person exists
        assert len(results) == 1


# ============= END TESTS ================


def build_and_reset_data_sources(scenario_builder):
    build_timeline_as_table(scenario_builder)


def add_registration_events(scenario_builder, events):
    scenario_builder.insert_multiple_into_arbitrary_table(
        "registration_timeline", events
    )


def registration_event(nhs_number, provenance, has_access_to_essential_supplies, event_datetime, ingested_datetime,
                       event_order, event_code=''):
    return {
        'nhs_number': nhs_number,
        'id': hash_nhs_number(nhs_number),
        'provenance': provenance,
        'has_access_to_essential_supplies': has_access_to_essential_supplies,
        'event_datetime': event_datetime,
        'ingested_datetime': ingested_datetime,
        'event_order': event_order,
        'event_code': event_code
    }


def build_timeline_as_table(scenario_builder):
    table_schema = {
        'nhs_number': 'TEXT',
        'id': 'TEXT',
        'provenance': 'TEXT',
        'has_access_to_essential_supplies': 'TEXT',
        'event_datetime': 'TIMESTAMP',
        'ingested_datetime': 'TIMESTAMP',
        'event_order': 'NUMERIC',
        'event_code': 'TEXT'
    }
    scenario_builder.build_arbitrary_table("registration_timeline", table_schema)


def build_query(tmp_path, template):
    query_file = f"{tmp_path}/built.sql"
    build_sql(template_file=template, output_file=query_file)
    with open(query_file, 'r') as f:
        query = f.read()
    return query
