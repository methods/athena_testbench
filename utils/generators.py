import time
import uuid
from datetime import datetime

from faker import Faker
import random
from utils.postcodes import PostcodeGenerator
from utils.random_utils import random_covid_date, random_date_of_birth, random_nhs_number
fake = Faker()
postcodes = PostcodeGenerator()


def generate_nhs_clean_entry():
    entry = {
        'nhs_nhs_number': random_nhs_number(),
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
        'nhs_deceased': random.choice(['1', '0', '0', '0']),
        'nhs_data_source': '',
        'nhs_inception_date': random_covid_date(),
    }
    return entry


def generate_ivr_clean_entry():
    entry = {
        'ivr_nhs_number': random_nhs_number(),
        'ivr_dob': random_date_of_birth(),
        'ivr_postcode': postcodes.get_postcode(),
        'ivr_postal_code_verified': random.choice(['yes', 'no']),
        'ivr_delivery_supplies': 1,
        'ivr_customer_calling_number': 1,
        'ivr_current_item_id': 1,
        'ivr_transfer': 1,
        'ivr_fallback_time': 1,
        'ivr_nhs_known': 1,
        'ivr_contact_id': 1,
        'ivr_preferred_phone_number': 1,
        'ivr_phone_number_calls': 1,
        'ivr_have_help': 1,
        'ivr_carry_supplies': 1,
        'ivr_call_timestamp': 1,
        'ivr_unmet_needs': 1,
    }
    return entry


def generate_web_clean_entry():
    form_time = time.time()
    form_time = form_time - random.randint(0, 1000 * 60 * 60 * 24)

    dob = random_date_of_birth(output_format="%d/%m/%Y")
    dob_date = datetime.strptime(dob, "%d/%m/%Y")

    know_nhs_number = random.choice(['yes'] * 4 + ['no'])
    ref_id = str(uuid.uuid1())

    entry = {
        'live_in_england': random.choice(['yes'] * 4 + ['no']),
        'first_name': fake.first_name(),
        'middle_name': fake.first_name(),
        'last_name': fake.last_name(),
        'city': fake.city(),
        'address_l1': fake.street_address(),
        'address_l2': fake.street_address(),
        'county': '',
        'postcode': postcodes.get_postcode(),
        'nhs_number': random_nhs_number() if know_nhs_number == 'yes' else '',
        'carry_supplies': random.choice(['yes'] * 2 + ['no']),
        'reference_id': ref_id,
        'dob_day': dob_date.day,
        'dob_month': dob_date.month,
        'dob_year': dob_date.year,
        'full_dob': dob,
        'phone_number_calls': fake.phone_number(),
        'phone_number_texts': fake.phone_number(),
        'contact': fake.email(),
        'know_nhs_number': know_nhs_number,
        'check_answers_seen': '',
        'nhs_letter': random.choice(['yes'] * 4 + ['no']),
        'basic_care_needs': random.choice(['yes','no']),
        'dietary_requirements': random.choice(['yes','no']),
        'medical_conditions': random.choice(['yes','no']),
        'essential_supplies': random.choice(['yes','no']),
        'updated_at': form_time,
        'referenceid': ref_id,
        'unixtimestamp': form_time,
        'created_at': form_time,
    }
    return entry


def generate_raw_la_outcome():
    entry = {
        'reference': 1,
        'id': 1,
        'sequencenumber': 1,
        'nhsnumberrr': 1,
        'firstname': 1,
        'middlename': 1,
        'lastname': 1,
        'dateofbirth': 1,
        'localauthoritydistrict': 1,
        'postcode': 1,
        'servicecode': 1,
        'servicename': 1,
        'gdsoutcomecode': 1,
        'gdsoutcomedescription': 1,
        'localoutcomecode': 1,
        'completedoutcomedatetime': 1,
        'outcomecomments': 1,
        'inputoutcomecode': 1,
        'inputcompletedoutcomedate': 1,
        'inputoutcomecomments': 1,
    }
    return entry
