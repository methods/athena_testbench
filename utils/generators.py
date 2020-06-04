import time
import uuid
from datetime import datetime

from faker import Faker
import random
from utils.postcodes import PostcodeGenerator
from utils.random_utils import random_covid_date, random_date_of_birth, random_nhs_number, random_time_in_last_n_days

fake = Faker()
postcodes = PostcodeGenerator()


def generate_nhs_clean_entry(nhs_number=None):
    entry = {
        'nhs_nhs_number': nhs_number if nhs_number else random_nhs_number(),
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


def generate_ivr_clean_entry(nhs_number=None, has_essential_supplies_TRUE_FALSE=None, submission_time=None, submission_time_format='%Y-%m-%d %H:%i:%s'):
    correct_nhs = True if nhs_number else random.choice([True, False])
    specified_number = nhs_number if nhs_number else random_nhs_number()

    entry = {
        'ivr_nhs_number': f'{specified_number}' if correct_nhs else f'x-{specified_number}',
        'ivr_dob': random_date_of_birth(),
        'ivr_postcode': postcodes.get_postcode(),
        'ivr_postal_code_verified': random.choice(['yes', 'no']),
        'ivr_delivery_supplies': has_essential_supplies_TRUE_FALSE if has_essential_supplies_TRUE_FALSE else random.choice(['TRUE','FALSE']),
        'ivr_customer_calling_number': f'{fake.phone_number()}',
        'ivr_current_item_id': '17' if correct_nhs else '5',
        'ivr_transfer': '',
        'ivr_fallback_time': '',
        'ivr_nhs_known': 'TRUE' if correct_nhs else 'FALSE',
        'ivr_contact_id': 1,
        'ivr_preferred_phone_number': fake.phone_number(),
        'ivr_phone_number_calls': fake.phone_number(),
        'ivr_have_help': random.choice(['TRUE','FALSE']),
        'ivr_carry_supplies': random.choice(['TRUE','FALSE']),
        'ivr_call_timestamp': submission_time if submission_time else random_time_in_last_n_days(n=60),
        'ivr_unmet_needs': 'NA',
    }
    return entry


def generate_web_clean_entry(nhs_number=None, ref_id=None, has_essential_supplies_yes_no=None, submission_time=None, submission_time_format='%Y-%m-%d %H:%M:%S'):

    if submission_time:
        form_time = time.mktime(datetime.strptime(submission_time, submission_time_format).timetuple())
    else:
        form_time = time.time()
        form_time = form_time - random.randint(0, 1000 * 60 * 60 * 24)

    dob = random_date_of_birth(output_format="%d/%m/%Y")
    dob_date = datetime.strptime(dob, "%d/%m/%Y")

    know_nhs_number = 'yes' if nhs_number else random.choice(['yes'] * 4 + ['no'])
    specified_number = nhs_number if nhs_number else random_nhs_number()

    ref = ref_id if ref_id else str(uuid.uuid1())

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
        'nhs_number': specified_number if know_nhs_number == 'yes' else '',
        'carry_supplies': random.choice(['yes'] * 2 + ['no']),
        'reference_id': ref,
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
        'essential_supplies': has_essential_supplies_yes_no if has_essential_supplies_yes_no else random.choice(['yes','no']),
        'updated_at': form_time,
        'referenceid': ref,
        'unixtimestamp': form_time,
        'created_at': form_time,
    }
    return entry


def generate_raw_la_outcome(nhs_number=None, ref_id=None, code=None, event_date=None, ingested_datetime=None):
    ref = ref_id if ref_id else str(uuid.uuid1())

    entry = {
        'inputoutcomecode': code if code else random.choice(['']*10 + ['D001', 'W003', 'W004', 'W006']),
        'inputcompletedoutcomedate': event_date if event_date else random_time_in_last_n_days(5, "%d/%m/%Y"),
        'inputoutcomecomments': fake.sentence(5),
        'reference': ref,
        'id': ref,
        'sequencenumber': '1',
        'nhsnumber': nhs_number if nhs_number else random_nhs_number(),
        'firstname': fake.first_name(),
        'middlename': fake.first_name(),
        'lastname': fake.last_name(),
        'dateofbirth': random_date_of_birth(output_format="%d/%m/%Y"),
        'localauthoritydistrict': 'CA008',
        'postcode': postcodes.get_postcode(),
        'servicecode': '',
        'servicename': '',
        'gdsoutcomecode': '',
        'gdsoutcomedescription': '',
        'localoutcomecode': '',
        'completedoutcomedatetime': '',
        'outcomecomments': '',
        'ingested_datetime': ingested_datetime if ingested_datetime else random_time_in_last_n_days(10),
        'row_count': 1
    }
    return entry


def generate_deliveries_outcome(ref_id=None, code=None, delivery_date=None, ingested_datetime=None):
    deldate = delivery_date if delivery_date else random_time_in_last_n_days(30, '%d/%m/%Y')

    entry = {
        'company': 'Brakes',
        'id': ref_id if ref_id else str(uuid.uuid1()),
        'name': fake.name(),
        'address': fake.street_address(),
        'postcode': postcodes.get_postcode(),
        'deldate': deldate,
        'outcome': code if code else '3',
        'comment': fake.sentence(5),
        'ingested_datetime': ingested_datetime if ingested_datetime else random_time_in_last_n_days(10),
        'row_count': 1
    }
    return entry