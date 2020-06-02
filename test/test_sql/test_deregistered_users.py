import datetime

import pytest
from faker import Faker

from build_sql import build_sql
from utils.ScenarioBuilder import ScenarioBuilder
from utils.connections import presto_transaction, pg_connect, kill_running_presto_queries
from utils.postcodes import PostcodeGenerator
from utils.random_utils import n_days_ago, random_nhs_number, random_covid_date, random_date_of_birth

fake = Faker()
postcodes = PostcodeGenerator()

HEX_ID_1 = '302aac04af01d0eed013b6dd32bb10da'
HEX_ID_2 = 'e8d175f597be6ef29741fef06f7243c5'
HEX_ID_3 = 'c544695ada9d4be4f3279d288323e6ad'

SOURCE_COL = 12

"""
deregistered_users_test_stack.sql includes full_submission_record.sql as a with

this means our data sources are the ones that feed into full_submission_record and gives us a greater ability to test
various scenarios

"""


# ============= TEST RECORDS ARE JOINING CORRECTLY ================

def test_registered_users_do_not_feed_through(tmp_path: pytest.fixture):
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
        query = build_query(tmp_path, 'sql_to_build/deregistered_users_test_stack_TEMPLATE.sql')
        results = presto_transaction(query)

        # THEN
        # there are no results returned
        assert len(results) == 0


def test_users_that_deregister_themselves_do_not_get_listed(tmp_path: pytest.fixture):
    with pg_connect() as con:
        scenario_builder = ScenarioBuilder(con)

        # GIVEN
        # users have said they have access to resources on the web or ivr (deregistered themselves)
        build_and_reset_data_sources(scenario_builder)

        scenario_builder.insert_multiple_into_arbitrary_table(
            "latest_submission", [
                latest_submission_row('1', 'NO', n_days_ago(n=1)),
                latest_submission_row('2', 'YES', n_days_ago(n=1)),
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
        query = build_query(tmp_path, 'sql_to_build/deregistered_users_test_stack_TEMPLATE.sql')
        results = presto_transaction(query)

        # THEN
        # there are no results returned
        assert len(results) == 0


def test_users_that_are_deregistered_by_wholesaler_get_listed(tmp_path: pytest.fixture):
    with pg_connect() as con:
        scenario_builder = ScenarioBuilder(con)

        # GIVEN
        # users have registered and one has been deregistered by the wholesaler
        build_and_reset_data_sources(scenario_builder)

        scenario_builder.insert_multiple_into_arbitrary_table(
            "latest_submission", [
                latest_submission_row('1', 'NO', n_days_ago(n=10)),
                latest_submission_row('2', 'NO', n_days_ago(n=10)),
                latest_submission_row('3', 'NO', n_days_ago(n=10)),
            ]
        )
        scenario_builder.insert_multiple_into_arbitrary_table(
            "nhs_clean_staging", [
                nhs_clean_staging_row(1),
                nhs_clean_staging_row(2),
                nhs_clean_staging_row(3),
            ]
        )
        scenario_builder.insert_multiple_into_arbitrary_table(
            "latest_wholesaler_opt_out", [
                latest_wholesaler_opt_out_row(HEX_ID_1, n_days_ago(1), '3', '')
            ]
        )

        # WHEN
        # we build the latest feedback deregister stack and run it in presto
        query = build_query(tmp_path, 'sql_to_build/deregistered_users_test_stack_TEMPLATE.sql')
        results = presto_transaction(query)

        # THEN
        # there are no results returned
        assert len(results) == 1
        assert results[0][SOURCE_COL] == 'wholesaler'


def test_users_that_are_deregistered_by_local_authority_get_listed(tmp_path: pytest.fixture):
    with pg_connect() as con:
        scenario_builder = ScenarioBuilder(con)

        # GIVEN
        # users have registered and one has been deregistered by the wholesaler
        build_and_reset_data_sources(scenario_builder)

        scenario_builder.insert_multiple_into_arbitrary_table(
            "latest_submission", [
                latest_submission_row('1', 'NO', n_days_ago(n=10)),
                latest_submission_row('2', 'NO', n_days_ago(n=10)),
                latest_submission_row('3', 'NO', n_days_ago(n=10)),
            ]
        )
        scenario_builder.insert_multiple_into_arbitrary_table(
            "nhs_clean_staging", [
                nhs_clean_staging_row(1),
                nhs_clean_staging_row(2),
                nhs_clean_staging_row(3),
            ]
        )
        scenario_builder.insert_multiple_into_arbitrary_table(
            "latest_la_feedback_to_stop_boxes", [
                latest_la_feedback_opt_out_row('1', 'F002', n_days_ago(1), '')
            ]
        )

        # WHEN
        # we build the latest feedback deregister stack and run it in presto
        query = build_query(tmp_path, 'sql_to_build/deregistered_users_test_stack_TEMPLATE.sql')
        results = presto_transaction(query)

        # THEN
        # there are no results returned
        assert len(results) == 1
        assert results[0][SOURCE_COL] == 'local authority'


def test_users_that_are_deregistered_by_local_authority_then_reregister_are_not_listed(tmp_path: pytest.fixture):
    with pg_connect() as con:
        scenario_builder = ScenarioBuilder(con)

        # GIVEN
        # user is removed by an la - then re-registers
        build_and_reset_data_sources(scenario_builder)

        scenario_builder.insert_multiple_into_arbitrary_table(
            "nhs_clean_staging", [
                nhs_clean_staging_row(1),
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
        # we build the deregister users stack and run it in presto
        query = build_query(tmp_path, 'sql_to_build/deregistered_users_test_stack_TEMPLATE.sql')
        results = presto_transaction(query)

        # THEN
        # no results are returned
        assert len(results) == 0


def test_users_that_are_deregistered_by_wholesaler_then_reregister_are_not_listed(tmp_path: pytest.fixture):
    with pg_connect() as con:
        scenario_builder = ScenarioBuilder(con)

        # GIVEN
        # user is removed by an wholesaler - then re-registers
        build_and_reset_data_sources(scenario_builder)

        scenario_builder.insert_multiple_into_arbitrary_table(
            "nhs_clean_staging", [
                nhs_clean_staging_row(1),
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
        # we build the deregister users stack and run it in presto
        query = build_query(tmp_path, 'sql_to_build/deregistered_users_test_stack_TEMPLATE.sql')
        results = presto_transaction(query)

        # THEN
        # no results are returned
        assert len(results) == 0


def test_user_that_is_deregistered_by_la_and_wholesaler_only_registers_once(tmp_path: pytest.fixture):
    with pg_connect() as con:
        scenario_builder = ScenarioBuilder(con)

        # GIVEN
        # user is removed by an wholesaler - then re-registers
        build_and_reset_data_sources(scenario_builder)

        scenario_builder.insert_multiple_into_arbitrary_table(
            "nhs_clean_staging", [
                nhs_clean_staging_row(1),
            ]
        )
        scenario_builder.insert_multiple_into_arbitrary_table(
            "latest_submission", [
                latest_submission_row('1', 'NO', n_days_ago(n=10)),
            ]
        )
        scenario_builder.insert_multiple_into_arbitrary_table(
            "latest_wholesaler_opt_out", [
                latest_wholesaler_opt_out_row(HEX_ID_1, n_days_ago(5), '3', '')
            ]
        )
        scenario_builder.insert_multiple_into_arbitrary_table(
            "latest_la_feedback_to_stop_boxes", [
                latest_la_feedback_opt_out_row('1', 'F002', n_days_ago(5), '')
            ]
        )

        # WHEN
        # we build the deregister users stack and run it in presto
        query = build_query(tmp_path, 'sql_to_build/deregistered_users_test_stack_TEMPLATE.sql')
        results = presto_transaction(query)

        # THEN
        # no results are returned
        assert len(results) == 1


def test_user_that_is_marked_as_deceased_by_la_is_not_listed(tmp_path: pytest.fixture):
    with pg_connect() as con:
        scenario_builder = ScenarioBuilder(con)

        # GIVEN
        # user is removed by an wholesaler - then re-registers
        build_and_reset_data_sources(scenario_builder)

        scenario_builder.insert_multiple_into_arbitrary_table(
            "nhs_clean_staging", [
                nhs_clean_staging_row(1),
            ]
        )
        scenario_builder.insert_multiple_into_arbitrary_table(
            "latest_submission", [
                latest_submission_row('1', 'NO', n_days_ago(n=10)),
            ]
        )
        scenario_builder.insert_multiple_into_arbitrary_table(
            "latest_la_feedback_to_stop_boxes", [
                latest_la_feedback_opt_out_row('1', 'D001', n_days_ago(5), '')
            ]
        )

        # WHEN
        # we build the deregister users stack and run it in presto
        query = build_query(tmp_path, 'sql_to_build/deregistered_users_test_stack_TEMPLATE.sql')
        results = presto_transaction(query)

        # THEN
        # no results are returned
        assert len(results) == 0

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


def nhs_clean_staging_row(nhs_number, nhs_deceased='0'):
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
        'nhs_postcode': postcodes.get_postcode(),
        'nhs_practice_code': '',
        'nhs_practice_name': '',
        'nhs_contact_telephone': fake.phone_number(),
        'nhs_deceased': nhs_deceased,
        'nhs_data_source': '',
        'nhs_inception_date': random_covid_date(),
    }


def build_and_reset_data_sources(scenario_builder):
    build_latest_submissions_as_table(scenario_builder)
    build_latest_la_feedback_opt_in_as_table(scenario_builder)
    build_latest_la_feedback_opt_out_as_table(scenario_builder)
    build_latest_wholesaler_opt_out_as_table(scenario_builder)
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


def build_query(tmp_path, template):
    query_file = f"{tmp_path}/built.sql"
    build_sql(template_file=template, output_file=query_file)
    with open(query_file, 'r') as f:
        query = f.read()
    return query
