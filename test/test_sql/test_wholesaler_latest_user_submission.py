import datetime

import pytest
from faker import Faker

from build_sql import build_sql
from utils.ScenarioBuilder import ScenarioBuilder
from utils.connections import presto_transaction, pg_connect
from utils.postcodes import PostcodeGenerator
from utils.random_utils import n_days_ago, random_nhs_number, random_covid_date, random_date_of_birth

fake = Faker()
postcodes = PostcodeGenerator()

HEX_ID_1 = '302aac04af01d0eed013b6dd32bb10da'

HAS_ACCESS = 14
POSTCODE = 10
LAST_NAME = 4
MOBILE = 12

"""
deregistered_users_test_stack.sql includes full_submission_record.sql as a with

this means our data sources are the ones that feed into full_submission_record and gives us a greater ability to test
various scenarios

"""


# ============= TEST RECORDS ARE JOINING CORRECTLY ================

def test_submissions_feed_through_to_final_query(tmp_path: pytest.fixture):
    with pg_connect() as con:
        scenario_builder = ScenarioBuilder(con)

        # GIVEN
        # a simple situation where users have registered on the web or ivr
        build_and_reset_data_sources(scenario_builder)

        scenario_builder.insert_multiple_into_arbitrary_table(
            "latest_submission", [
                latest_submission_row('1', 'NO', n_days_ago(n=1)),
                latest_submission_row('2', 'NO', n_days_ago(n=1)),
                latest_submission_row('3', 'NO', n_days_ago(n=1)),
            ]
        )
        scenario_builder.insert_multiple_into_arbitrary_table(
            "nhs_clean_staging", [
                nhs_clean_staging_row(1),
                nhs_clean_staging_row(2),
                nhs_clean_staging_row(3),
            ]
        )

        # WHEN
        # we build the latest feedback deregister stack and run it in presto
        query = build_query(tmp_path, 'sql_to_build/wholesaler_latest_user_submission_test_stack_TEMPLATE.sql')
        results = presto_transaction(query)

        # THEN
        # there are 3 results returned
        assert len(results) == 3


def test_users_that_deregister_themselves_are_marked_as_having_access_to_food(tmp_path: pytest.fixture):
    with pg_connect() as con:
        scenario_builder = ScenarioBuilder(con)

        # GIVEN
        # a simple situation where users have registered on the web or ivr
        build_and_reset_data_sources(scenario_builder)

        scenario_builder.insert_multiple_into_arbitrary_table(
            "latest_submission", [
                latest_submission_row('1', 'YES', n_days_ago(n=1)),
            ]
        )
        scenario_builder.insert_multiple_into_arbitrary_table(
            "nhs_clean_staging", [
                nhs_clean_staging_row(1)
            ]
        )

        # WHEN
        # we build the latest feedback deregister stack and run it in presto
        query = build_query(tmp_path, 'sql_to_build/wholesaler_latest_user_submission_test_stack_TEMPLATE.sql')
        results = presto_transaction(query)

        # THEN
        # there are 3 results returned
        assert len(results) == 1
        assert results[0][HAS_ACCESS] == 'YES'


def test_users_that_are_deregistered_by_la_are_marked_as_having_access_to_food(tmp_path: pytest.fixture):
    with pg_connect() as con:
        scenario_builder = ScenarioBuilder(con)

        # GIVEN
        # a user who registered for food and has been deregistered by the la
        build_and_reset_data_sources(scenario_builder)

        scenario_builder.insert_multiple_into_arbitrary_table(
            "latest_submission", [
                latest_submission_row('1', 'NO', n_days_ago(n=10)),
            ]
        )
        scenario_builder.insert_multiple_into_arbitrary_table(
            "nhs_clean_staging", [
                nhs_clean_staging_row(1)
            ]
        )
        scenario_builder.insert_multiple_into_arbitrary_table(
            "latest_la_feedback_to_stop_boxes", [
                latest_la_feedback_opt_out_row('1', 'F002', n_days_ago(5), '')
            ]
        )

        # WHEN
        # we build the latest feedback deregister stack and run it in presto
        query = build_query(tmp_path, 'sql_to_build/wholesaler_latest_user_submission_test_stack_TEMPLATE.sql')
        results = presto_transaction(query)

        # THEN
        # there are 3 results returned
        assert len(results) == 1
        assert results[0][HAS_ACCESS] == 'YES'


def test_users_that_are_deregistered_by_la_then_reregister_marked_as_needing_food(tmp_path: pytest.fixture):
    with pg_connect() as con:
        scenario_builder = ScenarioBuilder(con)

        # GIVEN
        # a user who registered for food and has been deregistered by the la
        build_and_reset_data_sources(scenario_builder)

        scenario_builder.insert_multiple_into_arbitrary_table(
            "nhs_clean_staging", [
                nhs_clean_staging_row(1)
            ]
        )
        scenario_builder.insert_multiple_into_arbitrary_table(
            "latest_la_feedback_to_stop_boxes", [
                latest_la_feedback_opt_out_row('1', 'F002', n_days_ago(5), '')
            ]
        )
        scenario_builder.insert_multiple_into_arbitrary_table(
            "latest_submission", [
                latest_submission_row('1', 'NO', n_days_ago(n=1)),
            ]
        )

        # WHEN
        # we build the latest feedback deregister stack and run it in presto
        query = build_query(tmp_path, 'sql_to_build/wholesaler_latest_user_submission_test_stack_TEMPLATE.sql')
        results = presto_transaction(query)

        # THEN
        # there are 3 results returned
        assert len(results) == 1
        assert results[0][HAS_ACCESS] == 'NO'


def test_users_that_are_deregistered_by_wholesaler_are_marked_as_having_access_to_food(tmp_path: pytest.fixture):
    with pg_connect() as con:
        scenario_builder = ScenarioBuilder(con)

        # GIVEN
        # a user who registered for food and has been deregistered by the wholesaler
        build_and_reset_data_sources(scenario_builder)

        scenario_builder.insert_multiple_into_arbitrary_table(
            "latest_submission", [
                latest_submission_row('1', 'NO', n_days_ago(n=10)),
            ]
        )
        scenario_builder.insert_multiple_into_arbitrary_table(
            "nhs_clean_staging", [
                nhs_clean_staging_row(1)
            ]
        )
        scenario_builder.insert_multiple_into_arbitrary_table(
            "latest_wholesaler_opt_out", [
                latest_wholesaler_opt_out_row(HEX_ID_1, n_days_ago(1), '3', '')
            ]
        )

        # WHEN
        # we build the latest feedback deregister stack and run it in presto
        query = build_query(tmp_path, 'sql_to_build/wholesaler_latest_user_submission_test_stack_TEMPLATE.sql')
        results = presto_transaction(query)

        # THEN
        # the user is marked as having access to food
        assert len(results) == 1
        assert results[0][HAS_ACCESS] == 'YES'


def test_users_that_are_deregistered_by_wholesaler_then_reregister_are_marked_as_needing_food(tmp_path: pytest.fixture):
    with pg_connect() as con:
        scenario_builder = ScenarioBuilder(con)

        # GIVEN
        # a user who registered for food after being deregistered by the wholesaler
        build_and_reset_data_sources(scenario_builder)

        scenario_builder.insert_multiple_into_arbitrary_table(
            "nhs_clean_staging", [
                nhs_clean_staging_row(1)
            ]
        )
        scenario_builder.insert_multiple_into_arbitrary_table(
            "latest_wholesaler_opt_out", [
                latest_wholesaler_opt_out_row(HEX_ID_1, n_days_ago(5), '3', '')
            ]
        )
        scenario_builder.insert_multiple_into_arbitrary_table(
            "latest_submission", [
                latest_submission_row('1', 'NO', n_days_ago(n=1)),
            ]
        )

        # WHEN
        # we build the latest feedback user submission stack and run it in presto
        query = build_query(tmp_path, 'sql_to_build/wholesaler_latest_user_submission_test_stack_TEMPLATE.sql')
        results = presto_transaction(query)

        # THEN
        # the user is marked as needing food
        assert len(results) == 1
        assert results[0][HAS_ACCESS] == 'NO'


def test_users_that_are_deregistered_have_pii_surpressed(tmp_path: pytest.fixture):
    with pg_connect() as con:
        scenario_builder = ScenarioBuilder(con)

        # GIVEN
        # a user who has deregistered for food
        build_and_reset_data_sources(scenario_builder)

        scenario_builder.insert_multiple_into_arbitrary_table(
            "nhs_clean_staging", [
                nhs_clean_staging_row(1)
            ]
        )
        scenario_builder.insert_multiple_into_arbitrary_table(
            "latest_submission", [
                latest_submission_row('1', 'YES', n_days_ago(n=1)),
            ]
        )

        # WHEN
        # we build the latest feedback user submission stack and run it in presto
        query = build_query(tmp_path, 'sql_to_build/wholesaler_latest_user_submission_test_stack_TEMPLATE.sql')
        results = presto_transaction(query)

        # THEN
        # the user has most of their data surpressed
        assert len(results) == 1
        assert results[0][LAST_NAME] == ''
        assert results[0][POSTCODE] == ''
        assert results[0][MOBILE] == ''


def test_users_that_are_deceased_have_pii_surpressed_and_marked_as_not_needing_supplies(tmp_path: pytest.fixture):
    with pg_connect() as con:
        scenario_builder = ScenarioBuilder(con)

        # GIVEN
        # a user who has registered for food but is now deceased
        build_and_reset_data_sources(scenario_builder)

        scenario_builder.insert_multiple_into_arbitrary_table(
            "nhs_clean_staging", [
                nhs_clean_staging_row(1, nhs_deceased='1')
            ]
        )
        scenario_builder.insert_multiple_into_arbitrary_table(
            "latest_submission", [
                latest_submission_row('1', 'YES', n_days_ago(n=10)),
            ]
        )

        # WHEN
        # we build the latest feedback user submission stack and run it in presto
        query = build_query(tmp_path, 'sql_to_build/wholesaler_latest_user_submission_test_stack_TEMPLATE.sql')
        results = presto_transaction(query)

        # THEN
        # the user has most of their data surpressed
        assert len(results) == 1
        assert results[0][HAS_ACCESS] == 'YES'
        assert results[0][LAST_NAME] == ''
        assert results[0][POSTCODE] == ''
        assert results[0][MOBILE] == ''


def test_users_that_are_registered_have_pii_columns_available(tmp_path: pytest.fixture):
    with pg_connect() as con:
        scenario_builder = ScenarioBuilder(con)

        # GIVEN
        # a user who has registered for food
        build_and_reset_data_sources(scenario_builder)

        scenario_builder.insert_multiple_into_arbitrary_table(
            "nhs_clean_staging", [
                nhs_clean_staging_row(1)
            ]
        )
        scenario_builder.insert_multiple_into_arbitrary_table(
            "latest_submission", [
                latest_submission_row('1', 'NO', n_days_ago(n=1)),
            ]
        )

        # WHEN
        # we build the latest feedback user submission stack and run it in presto
        query = build_query(tmp_path, 'sql_to_build/wholesaler_latest_user_submission_test_stack_TEMPLATE.sql')
        results = presto_transaction(query)

        # THEN
        # the user has pii data available
        assert len(results) == 1
        assert results[0][LAST_NAME] != ''
        assert results[0][POSTCODE] != ''
        assert results[0][MOBILE] != ''


# ============= END TESTS ================

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


def latest_wholesaler_opt_out_row(wholesaler_id, wholesaler_delivery_date, wholesaler_outcome, wholesaler_comments):
    return {
        'wholesaler_id': wholesaler_id,
        'wholesaler_delivery_date': wholesaler_delivery_date,
        'wholesaler_outcome': wholesaler_outcome,
        'wholesaler_comments': wholesaler_comments,
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


def nhs_clean_staging_row(nhs_number, nhs_deceased='0', postcode=None):
    return {
        'nhs_nhs_number': nhs_number,
        'nhs_dob': random_date_of_birth(),
        'nhs_patient_title': '',
        'nhs_patients_first_name': fake.first_name(),
        'nhs_patients_other_name': fake.first_name(),
        'nhs_patients_surname': fake.last_name(),
        'nhs_patients_address_line1': fake.street_address(),
        'nhs_patients_address_line2': fake.city(),
        'nhs_patients_address_line3': fake.city(),
        'nhs_patients_address_line4': '',
        'nhs_patients_address_line5': '',
        'nhs_postcode': postcode if postcode else postcodes.get_postcode(),
        'nhs_practice_code': '',
        'nhs_practice_name': '',
        'nhs_contact_telephone': fake.phone_number(),
        'nhs_deceased': nhs_deceased,
        'nhs_data_source': '',
        'nhs_inception_date': random_covid_date(),
    }


def clean_latest_address_row(nhs_number, postcode=None):
    return {
        'nhs_number': nhs_number,
        'address_line1': fake.street_address(),
        'address_line2': fake.city(),
        'address_line3': fake.city(),
        'address_line4': '',
        'address_line5': '',
        'address_postcode': postcode if postcode else postcodes.get_postcode(),
        'lad_code': 'lad_code',
        'lad_name': 'lad_name',
        'hub': 'hub'
    }


def build_and_reset_data_sources(scenario_builder):
    build_latest_submissions_as_table(scenario_builder)
    build_latest_la_feedback_opt_in_as_table(scenario_builder)
    build_latest_la_feedback_opt_out_as_table(scenario_builder)
    build_latest_wholesaler_opt_out_as_table(scenario_builder)
    build_clean_latest_address_as_table(scenario_builder)
    scenario_builder.reset_nhs_data()


def build_latest_la_feedback_opt_in_as_table(scenario_builder):
    table_schema = {
        'nhs_number': 'TEXT',
        'feedback_code': 'TEXT',
        'feedback_time': 'timestamp',
        'feedback_comments': 'TEXT',
    }
    scenario_builder.build_arbitrary_table("latest_la_feedback_to_continue_boxes", table_schema)


def build_latest_wholesaler_opt_out_as_table(scenario_builder):
    table_schema = {
        'wholesaler_id': 'TEXT',
        'wholesaler_delivery_date': 'timestamp',
        'wholesaler_outcome': 'TEXT',
        'wholesaler_comments': 'TEXT',
    }
    scenario_builder.build_arbitrary_table("latest_wholesaler_opt_out", table_schema)


def build_latest_la_feedback_opt_out_as_table(scenario_builder):
    table_schema = {
        'nhs_number': 'TEXT',
        'feedback_code': 'TEXT',
        'feedback_time': 'timestamp',
        'feedback_comments': 'TEXT',
    }
    scenario_builder.build_arbitrary_table("latest_la_feedback_to_stop_boxes", table_schema)


def build_latest_submissions_as_table(scenario_builder):
    table_schema = {
        'provenance': 'TEXT',
        'nhs_number': 'TEXT',
        'submission_time': 'timestamp',
        'has_access_to_essential_supplies': 'TEXT',
        'is_able_to_carry_supplies': 'TEXT',
        'email_address': 'TEXT',
        'phone_number_calls': 'TEXT',
        'phone_number_texts': 'TEXT'
    }
    scenario_builder.build_arbitrary_table("latest_submission", table_schema)


def build_clean_latest_address_as_table(scenario_builder):
    table_schema = {
        'nhs_number': 'TEXT',
        'address_line1': 'TEXT',
        'address_line2': 'TEXT',
        'address_line3': 'TEXT',
        'address_line4': 'TEXT',
        'address_line5': 'TEXT',
        'address_postcode': 'TEXT',
        'lad_code': 'TEXT',
        'lad_name': 'TEXT',
        'hub': 'TEXT'
    }
    scenario_builder.build_arbitrary_table("clean_latest_address_staging", table_schema)


def build_query(tmp_path, template):
    query_file = f"{tmp_path}/built.sql"
    build_sql(template_file=template, output_file=query_file)
    with open(query_file, 'r') as f:
        query = f.read()
    return query
