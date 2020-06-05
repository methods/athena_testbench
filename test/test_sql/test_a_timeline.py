import datetime

import pytest
from faker import Faker

from build_sql import build_sql
from utils.ScenarioBuilder import ScenarioBuilder
from utils.connections import presto_transaction, pg_connect
from utils.generators import generate_nhs_clean_entry, generate_ivr_clean_entry, generate_web_clean_entry, \
    generate_raw_la_outcome, generate_deliveries_outcome
from utils.postcodes import PostcodeGenerator
from utils.random_utils import n_days_ago, random_covid_date, random_date_of_birth

fake = Faker()
postcodes = PostcodeGenerator()

HEX_ID_1 = '302aac04af01d0eed013b6dd32bb10da'

HAS_ACCESS = 14
POSTCODE = 10
LAST_NAME = 4
MOBILE = 12

"""
these tests are based off test_wholesaler_latest_user_submission_test_stack.sql

this means our data sources are the ones that feed into full_submission_record and gives us a greater ability to test
various scenarios

we also need datasources for nhs records and the latest_address view
"""


# ============= TEST RECORDS ARE JOINING CORRECTLY ================

def test_ivr_submissions_added_to_the_timeline(tmp_path: pytest.fixture):
    with pg_connect() as con:
        scenario_builder = ScenarioBuilder(con)

        # GIVEN
        # multiple ivr records have been added
        build_and_reset_data_sources(scenario_builder)

        scenario_builder.insert_multiple_into_arbitrary_table(
            "nhs_clean_staging", [
                generate_nhs_clean_entry(nhs_number='1')
            ]
        )
        scenario_builder.insert_multiple_into_arbitrary_table(
            "ivr_clean_staging", [
                generate_ivr_clean_entry(nhs_number=1, submission_time=n_days_ago(10)),
                generate_ivr_clean_entry(nhs_number=1, submission_time=n_days_ago(5)),
                generate_ivr_clean_entry(nhs_number=1, submission_time=n_days_ago(1))
            ]
        )

        # WHEN
        # we build the registration timeline stack and run it in presto
        query = build_query(tmp_path, 'sql_to_build/registration_timeline_TEMPLATE.sql')
        results = presto_transaction(query)

        # THEN
        # all records are recorded on the registration timeline
        assert len(results) == 3


def test_web_submissions_added_to_the_timeline(tmp_path: pytest.fixture):
    with pg_connect() as con:
        scenario_builder = ScenarioBuilder(con)

        # GIVEN
        # multiple web records have been added
        build_and_reset_data_sources(scenario_builder)

        scenario_builder.insert_multiple_into_arbitrary_table(
            "nhs_clean_staging", [
                generate_nhs_clean_entry(nhs_number='1')
            ]
        )
        scenario_builder.insert_multiple_into_arbitrary_table(
            "web_clean_staging", [
                generate_web_clean_entry(nhs_number=1, ref_id=HEX_ID_1, submission_time=n_days_ago(10)),
                generate_web_clean_entry(nhs_number=1, ref_id=HEX_ID_1, submission_time=n_days_ago(5)),
                generate_web_clean_entry(nhs_number=1, ref_id=HEX_ID_1, submission_time=n_days_ago(1))
            ]
        )

        # WHEN
        # we build the registration timeline stack and run it in presto
        query = build_query(tmp_path, 'sql_to_build/registration_timeline_TEMPLATE.sql')
        results = presto_transaction(query)

        # THEN
        # all records are recorded on the registration timeline
        assert len(results) == 3


def test_la_outcome_deregistration_added_to_the_timeline(tmp_path: pytest.fixture):
    with pg_connect() as con:
        scenario_builder = ScenarioBuilder(con)

        # GIVEN
        # multiple local authority deregistration records have been added
        build_and_reset_data_sources(scenario_builder)

        scenario_builder.insert_multiple_into_arbitrary_table(
            "nhs_clean_staging", [
                generate_nhs_clean_entry(nhs_number='1')
            ]
        )
        scenario_builder.insert_multiple_into_arbitrary_table(
            "raw_la_outcomes_staging", [
                generate_raw_la_outcome(nhs_number=1, ref_id=HEX_ID_1, code='F002', event_date=n_days_ago(10)),
                generate_raw_la_outcome(nhs_number=1, ref_id=HEX_ID_1, code='D001', event_date=n_days_ago(5)),
                generate_raw_la_outcome(nhs_number=1, ref_id=HEX_ID_1, code='W002', event_date=n_days_ago(1)),
                generate_raw_la_outcome(nhs_number=1, ref_id=HEX_ID_1, code='W003', event_date=n_days_ago(1)),
            ]
        )

        # WHEN
        # we build the registration timeline stack and run it in presto
        query = build_query(tmp_path, 'sql_to_build/registration_timeline_TEMPLATE.sql')
        results = presto_transaction(query)

        # THEN
        # all records are recorded on the registration timeline
        assert len(results) == 4


def test_la_outcome_that_isnt_deregistration_not_added_to_the_timeline(tmp_path: pytest.fixture):
    with pg_connect() as con:
        scenario_builder = ScenarioBuilder(con)

        # GIVEN
        # local authority feedback records have been added without deregistration codes
        build_and_reset_data_sources(scenario_builder)

        scenario_builder.insert_multiple_into_arbitrary_table(
            "nhs_clean_staging", [
                generate_nhs_clean_entry(nhs_number='1')
            ]
        )
        scenario_builder.insert_multiple_into_arbitrary_table(
            "raw_la_outcomes_staging", [
                generate_raw_la_outcome(nhs_number=1, ref_id=HEX_ID_1, code='CA001', event_date=n_days_ago(10)),
                generate_raw_la_outcome(nhs_number=1, ref_id=HEX_ID_1, code='CA002', event_date=n_days_ago(5)),
                generate_raw_la_outcome(nhs_number=1, ref_id=HEX_ID_1, code='LA01', event_date=n_days_ago(1)),
            ]
        )

        # WHEN
        # we build the registration timeline stack and run it in presto
        query = build_query(tmp_path, 'sql_to_build/registration_timeline_TEMPLATE.sql')
        results = presto_transaction(query)

        # THEN
        # no records are recorded on the registration timeline
        assert len(results) == 0


def test_wholesaler_feedback_is_added_to_the_timeline(tmp_path: pytest.fixture):
    with pg_connect() as con:
        scenario_builder = ScenarioBuilder(con)

        # GIVEN
        # wholesaler feedback records have been added
        build_and_reset_data_sources(scenario_builder)

        scenario_builder.insert_multiple_into_arbitrary_table(
            "nhs_clean_staging", [
                generate_nhs_clean_entry(nhs_number='1')
            ]
        )
        scenario_builder.insert_multiple_into_arbitrary_table(
            "clean_deliveries_outcome_staging", [
                generate_deliveries_outcome(ref_id=HEX_ID_1, code='3', delivery_date=n_days_ago(10, format='%d/%m/%Y'),
                                            ingested_datetime=n_days_ago(5)),
                generate_deliveries_outcome(ref_id=HEX_ID_1, code='3', delivery_date=n_days_ago(10, format='%d/%m/%Y'),
                                            ingested_datetime=n_days_ago(9)),
                generate_deliveries_outcome(ref_id=HEX_ID_1, code='3', delivery_date=n_days_ago(10, format='%d/%m/%Y'),
                                            ingested_datetime=n_days_ago(8)),
            ]
        )

        # WHEN
        # we build the registration timeline stack and run it in presto
        query = build_query(tmp_path, 'sql_to_build/registration_timeline_TEMPLATE.sql')
        results = presto_transaction(query)

        # THEN
        # all records are recorded on the registration timeline
        assert len(results) == 3


def test_wholesaler_feedback_that_is_not_code_3_is_not_added_to_the_timeline(tmp_path: pytest.fixture):
    with pg_connect() as con:
        scenario_builder = ScenarioBuilder(con)

        # GIVEN
        # wholesaler feedback records have been added with feedback codes other than 3
        build_and_reset_data_sources(scenario_builder)

        scenario_builder.insert_multiple_into_arbitrary_table(
            "nhs_clean_staging", [
                generate_nhs_clean_entry(nhs_number='1')
            ]
        )
        scenario_builder.insert_multiple_into_arbitrary_table(
            "clean_deliveries_outcome_staging", [
                generate_deliveries_outcome(ref_id=HEX_ID_1, code='1', delivery_date=n_days_ago(10, format='%d/%m/%Y'),
                                            ingested_datetime=n_days_ago(9)),
                generate_deliveries_outcome(ref_id=HEX_ID_1, code='7', delivery_date=n_days_ago(7, format='%d/%m/%Y'),
                                            ingested_datetime=n_days_ago(7)),
                generate_deliveries_outcome(ref_id=HEX_ID_1, code='2', delivery_date=n_days_ago(8, format='%d/%m/%Y'),
                                            ingested_datetime=n_days_ago(8)),
            ]
        )

        # WHEN
        # we build the registration timeline stack and run it in presto
        query = build_query(tmp_path, 'sql_to_build/registration_timeline_TEMPLATE.sql')
        results = presto_transaction(query)

        # THEN
        # no records are recorded on the registration timeline
        assert len(results) == 0


# ============= END TESTS ================


def build_and_reset_data_sources(scenario_builder):
    scenario_builder.reset_nhs_data()
    scenario_builder.reset_ivr_data()
    scenario_builder.reset_web_data()
    scenario_builder.reset_la_feedback_data()
    scenario_builder.reset_wholesaler_data()


def build_query(tmp_path, template):
    query_file = f"{tmp_path}/built.sql"
    build_sql(template_file=template, output_file=query_file)
    with open(query_file, 'r') as f:
        query = f.read()
    return query
