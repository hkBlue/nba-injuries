'''
    DataGatherer contains functions which are useful for data gathering.abs
    Primarily: gather_data_for(...) should be called, rest should be handled by the function.
'''
#pylint: disable=line-too-long,too-many-arguments,E1121
from src.shared.dataSourceType import DataSourceType
from src.shared.modelType import ModelType
from src.dataReader import data_reader as dr
from src.dataGatherer import cleaner, scraper
from datetime import datetime
import os

def get_time_range_to_gather(file_name, start_time, end_time):
    '''
        Returns the time range which is uncovered in the given file_name.abs
    '''
    earliest, latest = dr.get_time_range_covered(file_name, "%Y-%m-%d")

    print "\t Data in " + file_name + " already exists from (" + str(earliest) + ", " + str(latest) + ")."
    final_start_time = earliest if earliest > start_time else start_time
    final_end_time = latest if latest < end_time else end_time

    return final_start_time, final_end_time

def _gather_data_for(config, data_type, file_name, start_time, end_time, force=False):
    if not file_name.endswith(".csv"):
        raise ValueError("output_file_name MUST be of a csv format")

    time_range_begin = start_time
    time_range_end = end_time
    if os.path.isfile(file_name) and not force:
        time_range_begin, time_range_end = get_time_range_to_gather(file_name, start_time, end_time)

        if time_range_begin < start_time and time_range_end > end_time:
            print "Data in " + file_name + " is from (" + str(time_range_begin) + ", " + str(time_range_end) + ") which contains time period (" + str(start_time) + ", " + str(end_time) + "). Enter -force=True, to run anyways."
            print "Ending gather data for data_type: " + str(data_type) + "..."
            return

    print "\tGathering data from " + str(time_range_begin) + " to " + str(time_range_end)

    print "\tBeginning to scrape data..."
    scraper.scrape(config, data_type, file_name, time_range_begin, time_range_end)
    print "\tFinished scraping data!"

    print "\tBeginning to clean data..."
    cleaner.clean(config, data_type, file_name, time_range_begin, time_range_end)
    print "\tFinished cleaning data!"

    return

def gather_data(data_types, config, start_time, end_time, force=False):
    '''
        param data_types : list of types of data to collect
        param config : a map to the defined config in config.yml
        param start_time : start time to have data for in the output file.
        param end_time : end time to to have data for in the output file.
        param force : indicates whether or not to force gather_data to gather, even if time range is covered by CSV.abs

        Gathers data for the given time range for the given source type, and dumps it into the given file path.abs

        force is False, then gather_data will not run if the passed in time range already appears in the data.
        if force is True, then gather_data will run even if the passed in time range already appears in the data.

        Example when you may want to use force=True:
            when for some reason the previous data gathering session did not gather all the DataSourceType
            for a given time range
    '''
    absolute_path_to_output_file = config['outputFilePath']['raw']

    absolute_path = absolute_path_to_output_file if absolute_path_to_output_file.endswith("/") else absolute_path_to_output_file + "/"

    for data_type in data_types:
        print "Beginning to gather data for: " + str(data_type)
        file_name = absolute_path + config['outputFileName'][str(data_type)]
        _gather_data_for(config, data_type, file_name, start_time, end_time, force)
        print "Finished gathering data for: " + str(data_type)
    #Scrape the relevant data.
    return



