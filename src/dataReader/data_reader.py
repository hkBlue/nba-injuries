'''
    A module which contains helper classes and functions for
    reading project data from CSV's.
'''
#pylint: disable=line-too-long
import csv
from datetime import datetime
from src.runner.runner import Runner

def get_time_range_covered(file_name, date_format):
    '''
        Returns the (earliest, most recent date) covered by the data in file,
        as datetime objects.

        ASSUMPTIONS:
            -that the first row is the header of the CSV file.
            -that the name of the column is instance either 'Date' or 'date'.

        param file : path to file to open and check time range for.
        param date_format: format of dates in the file.
    '''

    with open(file_name) as data_file:
        reader = csv.DictReader(data_file)
        header = reader.next()
        date_idx = 0
        try:
            date_idx = header.index("Date")
        except ValueError:
            try:
                date_idx = header.index("date")
            except ValueError:
                raise ValueError("Netiher, 'Date' nor 'date' is a column header for : " + str(file_name))

        earliest = Runner.EARLIEST_START_DATE
        latest = Runner.EARLIEST_START_DATE
        for row in reader:
            date = datetime.strptime(row[date_idx], date_format)
            earliest = min(date, earliest)
            latest = max(date, latest)

        return earliest, latest


