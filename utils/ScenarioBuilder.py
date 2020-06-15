import itertools
import pg8000 as pg
import prestodb.dbapi as presto
import pytest
import random
import subprocess

from utils.connections import pg_connect, presto_connect, presto_transaction
from utils.generators import generate_nhs_clean_entry, generate_ivr_clean_entry, generate_web_clean_entry, \
    generate_raw_la_outcome
from utils.random_utils import random_nhs_number


class ScenarioBuilder:
    arbitrary_tables = []

    def __init__(self):
        self.nhs_number_list = []
        self.open_connections()
        self.init_tables()

    def open_connections(self):
        self.pg_con = pg_connect()
        self.presto_con = presto_connect()

    def close_connections(self):
        try:
            self.pg_con.close()
        except InterfaceError:
            # already closed if we are here!
            pass
        try:
            self.presto_con.close()
        except InterfaceError:
            # already closed if we are here!
            pass

    def init_tables(self):
        self.create_nhs_table()
        self.create_ivr_table()
        self.create_web_table()
        self.create_la_feedback_table()

    def create_nhs_table(self):
        create_command = f'CREATE TABLE IF NOT EXISTS "nhs_clean_staging" (' \
            f'nhs_nhs_number TEXT,' \
            f'nhs_dob TEXT,' \
            f'nhs_patient_title VARCHAR(256),' \
            f'nhs_patients_first_name TEXT,' \
            f'nhs_patients_other_name TEXT,' \
            f'nhs_patients_surname TEXT,' \
            f'nhs_patients_address_line1 TEXT,' \
            f'nhs_patients_address_line2 TEXT,' \
            f'nhs_patients_address_line3 TEXT,' \
            f'nhs_patients_address_line4 TEXT,' \
            f'nhs_patients_address_line5 TEXT,' \
            f'nhs_postcode TEXT,' \
            f'nhs_practice_code TEXT,' \
            f'nhs_practice_name TEXT,' \
            f'nhs_contact_telephone TEXT,' \
            f'nhs_deceased TEXT,' \
            f'nhs_data_source TEXT,' \
            f'nhs_inception_date TEXT)'
        self.pg_con.run(create_command)
        self.pg_con.commit()

    def reset_nhs_data(self):
        self.create_nhs_table()
        self.pg_con.run('DELETE FROM "nhs_clean_staging"')
        self.pg_con.commit()

    def insert_random_nhs_records(self, count=100):
        for entry_number in range(0, count):
            entry = generate_nhs_clean_entry()
            self.nhs_number_list = self.nhs_number_list + [entry['nhs_nhs_number']]
            command = self.get_insert_command('nhs_clean_staging', entry)
            self.pg_con.run(command)
        self.pg_con.commit()

    def get_nhs_number(self):
        if len(self.nhs_number_list) == 0:
            random_nhs_number()
        else:
            return random.choice(self.nhs_number_list)

    def create_ivr_table(self):
        create_command = f'CREATE TABLE IF NOT EXISTS "ivr_clean_staging" (' \
            f'ivr_nhs_number TEXT,' \
            f'ivr_postcode TEXT,' \
            f'ivr_dob TEXT,' \
            f'ivr_customer_calling_number TEXT,' \
            f'ivr_current_item_id TEXT,' \
            f'ivr_transfer TEXT,' \
            f'ivr_fallback_time TEXT,' \
            f'ivr_nhs_known TEXT,' \
            f'ivr_contact_id TEXT,' \
            f'ivr_preferred_phone_number TEXT,' \
            f'ivr_postal_code_verified TEXT,' \
            f'ivr_delivery_supplies TEXT,' \
            f'ivr_phone_number_calls TEXT,' \
            f'ivr_carry_supplies TEXT,' \
            f'ivr_have_help TEXT,' \
            f'ivr_call_timestamp TEXT,' \
            f'ivr_unmet_needs TEXT)'
        self.pg_con.run(create_command)
        self.pg_con.commit()

    def reset_ivr_data(self):
        self.create_ivr_table()
        self.pg_con.run('DELETE FROM "ivr_clean_staging"')
        self.pg_con.commit()

    def insert_random_ivr_records(self, count=100):
        for entry_number in range(0, count):
            entry = generate_ivr_clean_entry(nhs_number=self.get_nhs_number())
            command = self.get_insert_command('ivr_clean_staging', entry)
            self.pg_con.run(command)
        self.pg_con.commit()

    def create_web_table(self):
        create_command = f'CREATE TABLE IF NOT EXISTS "web_clean_staging" (' \
            f'live_in_england TEXT,' \
            f'first_name TEXT,' \
            f'middle_name TEXT,' \
            f'last_name TEXT,' \
            f'city TEXT,' \
            f'address_l1 TEXT,' \
            f'address_l2 TEXT,' \
            f'county TEXT,' \
            f'postcode TEXT,' \
            f'nhs_number TEXT,' \
            f'carry_supplies TEXT,' \
            f'reference_id TEXT,' \
            f'dob_day TEXT,' \
            f'dob_month TEXT,' \
            f'dob_year TEXT,' \
            f'full_dob TEXT,' \
            f'phone_number_calls TEXT,' \
            f'phone_number_texts TEXT,' \
            f'contact TEXT,' \
            f'know_nhs_number TEXT,' \
            f'check_answers_seen TEXT,' \
            f'nhs_letter TEXT,' \
            f'basic_care_needs TEXT,' \
            f'dietary_requirements TEXT,' \
            f'medical_conditions TEXT,' \
            f'essential_supplies TEXT,' \
            f'updated_at NUMERIC,' \
            f'referenceid TEXT,' \
            f'unixtimestamp NUMERIC,' \
            f'created_at NUMERIC)'
        self.pg_con.run(create_command)
        self.pg_con.commit()

    def reset_web_data(self):
        self.create_web_table()
        self.pg_con.run('DELETE FROM "web_clean_staging"')
        self.pg_con.commit()

    def insert_random_web_records(self, count=100):
        for entry_number in range(0, count):
            entry = generate_web_clean_entry(nhs_number=self.get_nhs_number())
            command = self.get_insert_command('web_clean_staging', entry)
            self.pg_con.run(command)
        self.pg_con.commit()

    def create_la_feedback_table(self):
        create_command = f'CREATE TABLE IF NOT EXISTS "raw_la_outcomes_staging" (' \
            f'reference TEXT,' \
            f'id TEXT,' \
            f'sequencenumber NUMERIC,' \
            f'nhsnumber TEXT,' \
            f'firstname TEXT,' \
            f'middlename TEXT,' \
            f'lastname TEXT,' \
            f'dateofbirth TEXT,' \
            f'localauthoritydistrict TEXT,' \
            f'postcode TEXT,' \
            f'servicecode TEXT,' \
            f'servicename TEXT,' \
            f'gdsoutcomecode TEXT,' \
            f'gdsoutcomedescription TEXT,' \
            f'localoutcomecode TEXT,' \
            f'completedoutcomedatetime TEXT,' \
            f'nextplanneddate TEXT,' \
            f'outcomecomments TEXT,' \
            f'inputoutcomecode TEXT,' \
            f'inputcompletedoutcomedate TEXT,' \
            f'inputoutcomecomments TEXT)'
        self.pg_con.run(create_command)
        self.pg_con.commit()

    def reset_la_feedback_data(self):
        self.create_la_feedback_table()
        self.pg_con.run('DELETE FROM "raw_la_outcomes_staging"')
        self.pg_con.commit()

    def insert_random_la_feedback(self, count=100):
        for entry_number in range(0, count):
            entry = generate_raw_la_outcome(nhs_number=self.get_nhs_number())
            command = self.get_insert_command('raw_la_outcomes_staging', entry)
            self.pg_con.run(command)
        self.pg_con.commit()

    def insert_la_feedback(self, nhsnumber, inputoutcomecode, inputcompletedoutcomedate, inputoutcomecomments):
        entry = generate_raw_la_outcome(nhs_number=nhsnumber)
        entry['inputoutcomecode'] = inputoutcomecode
        entry['inputcompletedoutcomedate'] = inputcompletedoutcomedate
        entry['inputoutcomecomments'] = inputoutcomecomments
        command = self.get_insert_command('raw_la_outcomes_staging', entry)
        self.pg_con.run(command)
        self.pg_con.commit()

    def get_insert_command(self, table_name, record):
        fields = ','.join([f'"{key}"' for key in record])
        values = ','.join([f"'{record[key]}'" for key in record])
        command = f'INSERT INTO {table_name} ({fields}) VALUES ({values})'
        return command

    def build_arbitrary_table(self, table_name: str, column_data_types: dict):
        create_command = (
            f'CREATE TABLE IF NOT EXISTS {table_name} ('
            f'{",".join([f"{key} {column_data_types[key]}" for key in column_data_types.keys()])}'
            ')'
        )
        self.pg_con.run(create_command)
        self.pg_con.run(f'DELETE FROM "{table_name}"')
        self.pg_con.commit()
        self.arbitrary_tables.append(table_name)

    def insert_into_arbitrary_table(self, table_name: str, record: dict):
        insert_command = self.get_insert_command(table_name, record)
        self.pg_con.run(insert_command)
        self.pg_con.commit()

    def insert_multiple_into_arbitrary_table(self, table_name: str, records):
        for record in records:
            insert_command = self.get_insert_command(table_name, record)
            self.pg_con.run(insert_command)
        self.pg_con.commit()

    def drop_arbitrary_tables(self):
        for table in self.arbitrary_tables:
            self.pg_con.run(f"DROP TABLE IF EXISTS {table}")
        self.pg_con.commit()
        self.arbitrary_tables = []

    def reset_arbitrary_tables(self):
        for table in self.arbitrary_tables:
            self.pg_con.run(f'DELETE FROM "{table}"')
        self.pg_con.commit()

    def kill_running_presto_queries(self):
        all_queries = presto_transaction("SELECT * FROM system.runtime.queries", self.presto_con)
        running_query_ids = (query[1] for query in all_queries if 'RUNNING' in query)
        for query_id in running_query_ids:
            presto_transaction(f"CALL system.runtime.kill_query(query_id => '{query_id}')", self.presto_con)

    def down_server_drop_postgres_tables_reup_server(self):
        self.close_connections()
        subprocess.call("bin/stop-server.sh")
        subprocess.call("bin/start-server-postgres-only.sh")
        self.open_connections()
        self.drop_arbitrary_tables()
        self.close_connections()
        subprocess.call("bin/stop-server.sh")
        subprocess.call("bin/start-server.sh")
        self.open_connections()

    def reset(self):
        self.reset_nhs_data()
        self.reset_ivr_data()
        self.reset_web_data()
        self.reset_la_feedback_data()
        self.reset_arbitrary_tables()

    def hard_reset(self):
        self.reset_nhs_data()
        self.reset_ivr_data()
        self.reset_web_data()
        self.reset_la_feedback_data()
        self.kill_running_presto_queries()
        self.down_server_drop_postgres_tables_reup_server()

    def list_postgres_tables(self) -> list:
        """
        List all tables in postgres db.
        """
        with self.pg_con.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT * FROM information_schema.tables
                """
            )
            tables = cursor.fetchall()

        self.pg_con.commit()

        return list(itertools.chain(*tables))



@pytest.fixture(scope="session")
def scenario_builder():
    """
    Auto-resetting, instantiated scenario builder with managed connection as
    pytest fixture.
    """
    scenario_builder = ScenarioBuilder()
    yield scenario_builder
    scenario_builder.hard_reset()
    scenario_builder.close_connections()
