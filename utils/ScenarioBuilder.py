"""
ToDo:
    - is the reset feature using TRUNCATE what slows tests down at random?
"""

import itertools
import random
import pg8000

from utils.connections import pg_connect
from utils.generators import generate_nhs_clean_entry, generate_ivr_clean_entry, generate_web_clean_entry, \
    generate_raw_la_outcome
from utils.random_utils import random_nhs_number


class ScenarioBuilder:
    nhs_number_list = []
    arbitrary_tables = []

    def _reset_nhs_data(self):
        with pg_connect() as con:
            create_command = f'CREATE TABLE IF NOT EXISTS "nhs_clean_staging" (' \
                f'nhs_nhs_number TEXT,' \
                f'nhs_dob TEXT,' \
                f'nhs_patient_title VARCHAR(256),' \
                f'nhs_patients_first_name TEXT,' \
                f'nhs_patients_other_name TEXT,' \
                f'nhs_patients_surname TEXT,' \
                f'nhs_patients_address_line_1 TEXT,' \
                f'nhs_patients_address_line_2 TEXT,' \
                f'nhs_patients_address_line_3 TEXT,' \
                f'nhs_patients_address_line_4 TEXT,' \
                f'nhs_patients_address_line_5 TEXT,' \
                f'nhs_postcode TEXT,' \
                f'nhs_practice_code TEXT,' \
                f'nhs_practice_name TEXT,' \
                f'nhs_contact_telephone TEXT,' \
                f'nhs_deceased TEXT,' \
                f'nhs_data_source TEXT,' \
                f'nhs_inception_date TEXT)'
            con.run(create_command)
            con.run('TRUNCATE "nhs_clean_staging"')
            con.commit()

    def _reset_ivr_data(self):
        with pg_connect() as con:
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
            con.run(create_command)
            con.run('TRUNCATE "ivr_clean_staging"')
            con.commit()

    def _reset_web_data(self):
        with pg_connect() as con:
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
            con.run(create_command)
            con.run('TRUNCATE "web_clean_staging"')
            con.commit()

    def _reset_la_feedback_data(self):
        with pg_connect() as con:
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
            con.run(create_command)
            con.run('TRUNCATE "raw_la_outcomes_staging"')
            con.commit()

    def get_insert_command(self, table_name, record):
        fields = ','.join([f'"{key}"' for key in record])
        values = ','.join([f"'{record[key]}'" for key in record])
        command = f'INSERT INTO {table_name} ({fields}) VALUES ({values})'
        return command

    def build_arbitrary_table(self, table_name: str, column_data_types: dict):
        with pg_connect() as con:
            create_command = (
                f'CREATE TABLE IF NOT EXISTS {table_name} ('
                f'{",".join([f"{key} {column_data_types[key]}" for key in column_data_types.keys()])}'
                ')'
            )
            con.run(create_command)
            con.run(f'TRUNCATE "{table_name}"')
            con.commit()
            self.arbitrary_tables.append(table_name)

    def insert_into_arbitrary_table(self, table_name: str, record: dict):
        with pg_connect() as con:
            insert_command = self.get_insert_command(table_name, record)
            con.run(insert_command)
            con.commit()

    def drop_arbitrary_tables(self):
        with pg_connect() as con:
            while self.arbitrary_tables:
                table = self.arbitrary_tables.pop()
                con.run(f"DROP TABLE IF EXISTS {table}")

            con.commit()

    def truncate_arbitrary_tables(self):
        with pg_connect() as con:
            for table in self.arbitrary_tables:
                    con.run(f"TRUNCATE {table}")
            con.commit()


    def reset(self):
        self._reset_nhs_data()
        self._reset_ivr_data()
        self._reset_web_data()
        self._reset_la_feedback_data()
        self.drop_arbitrary_tables()

    def soft_reset(self):
        self._reset_nhs_data()
        self._reset_ivr_data()
        self._reset_web_data()
        self._reset_la_feedback_data()
        self.truncate_arbitrary_tables()

    def insert_random_nhs_records(self, count=100):
        with pg_connect() as con:
            for entry_number in range(0, count):
                entry = generate_nhs_clean_entry()
                self.nhs_number_list = self.nhs_number_list + [entry['nhs_nhs_number']]
                command = self.get_insert_command('nhs_clean_staging', entry)
                con.run(command)
            con.commit()

    def get_nhs_number(self):
        if len(self.nhs_number_list) == 0:
            random_nhs_number()
        else:
            return random.choice(self.nhs_number_list)

    def insert_random_ivr_records(self, count=100):
        with pg_connect() as con:
            for entry_number in range(0, count):
                entry = generate_ivr_clean_entry(nhs_number=self.get_nhs_number())
                command = self.get_insert_command('ivr_clean_staging', entry)
                con.run(command)
            con.commit()

    def insert_random_web_records(self, count=100):
        with pg_connect() as con:
            for entry_number in range(0, count):
                entry = generate_web_clean_entry(nhs_number=self.get_nhs_number())
                command = self.get_insert_command('web_clean_staging', entry)
                con.run(command)
            con.commit()

    def insert_random_la_feedback(self, count=100):
        with pg_connect() as con:
            for entry_number in range(0, count):
                entry = generate_raw_la_outcome(nhs_number=self.get_nhs_number())
                command = self.get_insert_command('raw_la_outcomes_staging', entry)
                con.run(command)
            con.commit()

    def insert_la_feedback(self, nhsnumber, inputoutcomecode, inputcompletedoutcomedate, inputoutcomecomments):

        with pg_connect() as con:
            entry = generate_raw_la_outcome(nhs_number=nhsnumber)
            entry['inputoutcomecode'] = inputoutcomecode
            entry['inputcompletedoutcomedate'] = inputcompletedoutcomedate
            entry['inputoutcomecomments'] = inputoutcomecomments
            command = self.get_insert_command('raw_la_outcomes_staging', entry)
            con.run(command)
            con.commit()

    def list_postgres_tables(self) -> list:
        """
        List all tables in postgres db.
        """
        with pg_connect() as con:
            with con.cursor() as cursor:
                cursor.execute(
                    f"""
                    SELECT * FROM information_schema.tables
                    """
                )
                tables = cursor.fetchall()

            con.commit()

        return list(itertools.chain(*tables))
