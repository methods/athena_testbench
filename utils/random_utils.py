import random
import time


# https://stackoverflow.com/questions/553303/generate-a-random-date-between-two-other-dates

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


def random_nhs_number():
    return str(random.randrange(1000000000, 9999999999))
