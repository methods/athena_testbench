import random
import time

# https://stackoverflow.com/questions/553303/generate-a-random-date-between-two-other-dates
from datetime import datetime, timedelta


def str_time_prop(start, end, prop, input_format):
    """Get a time at a proportion of a range of two formatted times.

    start and end should be strings specifying times formated in the
    given format (strftime-style), giving an interval [start, end].
    prop specifies how a proportion of the interval to be taken after
    start.  The returned time will be in the specified format.
    """

    stime = time.mktime(time.strptime(start, input_format))
    etime = time.mktime(time.strptime(end, input_format))

    ptime = stime + prop * (etime - stime)

    return time.localtime(ptime)


def random_date_of_birth(start="01/01/1920", end="01/01/2000", input_format='%d/%m/%Y', output_format='%d/%m/%Y'):
    prop = random.random()
    random_date = str_time_prop(start, end, prop, input_format)
    return time.strftime(output_format, random_date)


def random_covid_date(start="01/03/2020", end="27/05/2000", input_format='%d/%m/%Y', output_format='%d/%m/%Y'):
    prop = random.random()
    random_date = str_time_prop(start, end, prop, input_format)
    return time.strftime(output_format, random_date)


def random_time_in_last_n_days(n=30, format="%Y-%m-%d %H:%M:%S"):
    now_time = time.time()
    rand_time = now_time - random.randint(0, 1000 * 60 * 60 * 24 * n)
    return time.strftime(format, time.localtime(rand_time))


def random_nhs_number():
    return str(random.randrange(1000000000, 9999999999))


def random_time_n_days_ago(n, format="%Y-%m-%d %H:%M:%S"):
    time1 = datetime.strftime(datetime.today() - timedelta(n), "%Y-%m-%d %H:%M:%S")
    time2 = datetime.strftime(datetime.today() - timedelta(n - 1), "%Y-%m-%d %H:%M:%S")

    rand_time = str_time_prop(time1, time2, random.random(), "%Y-%m-%d %H:%M:%S")
    return time.strftime(format, rand_time)


def n_days_ago(n, time_str="00:00:00", format="%Y-%m-%d %H:%M:%S"):
    date_str = datetime.strftime(datetime.today() - timedelta(n), "%Y-%m-%d")
    complete_str = f'{date_str} {time_str}'
    complete_time = time.mktime(time.strptime(complete_str, "%Y-%m-%d %H:%M:%S"))
    return time.strftime(format, time.localtime(complete_time))
