# pylint: disable=line-too-long
import argparse
import sys
from datetime import datetime
from src.shared.dataSourceType import DataSourceType
from src.shared.modelType import ModelType

class Runner(object):
    """Main class for running parts of the project."""
    EARLIEST_START_DATE = datetime.strptime("2006-07-01", "%Y-%m-%d")
    LATEST_END_DATE = datetime.now()
    _MODEL_ARG = "model"
    _DATA_ARG = "data"
    _START_DATE_ARG = "startDate"
    _END_DATE_ARG = "endDate"

    def __init__(self):
        return

    def _valid_date(self, date_str):
        """Returns a datetime object corresponding to date_str if it is in a valid date format, otherwise raises an argument exception."""
        date = None
        try:
            date = datetime.strptime(date_str, "%Y-%m-%d")
        except ValueError:
            try:
                date = datetime.strptime(date_str, "%Y%m%d")
            except ValueError:
                msg = "Not a valid date format, please enter in Y-m-d or Ymd format: '{0}'".format(date_str)
                raise argparse.ArgumentTypeError(msg)
        if date < Runner.EARLIEST_START_DATE or date > Runner.LATEST_END_DATE:
            raise argparse.ArgumentTypeError("Date argument not in valid range. MUST be between " + str(Runner.EARLIEST_START_DATE) + " and " + str(Runner.LATEST_END_DATE)) 
        else:
            return date

    def _valid_model_type(self, model_str):
        try:
            return ModelType[model_str.lower()]
        except KeyError:
            raise ValueError(model_str + " is not a valid ModelType, see usage for more info.")

    def _valid_data_type_list(self, data_str):
        try:
            data_list = [item for item in data_str.split(' ')]
            ret_val = []
            for data_type in data_list:
                ret_val.append(DataSourceType[data_type.lower()])
                return ret_val
        except KeyError:
            raise ValueError(data_str + " is not a valid DataSourceType, see usage for more info.")

    def _create_parser(self):
        '''Creates the argument parser for Runner.'''

        valid_models = [str(x) for x in ModelType]
        valid_data = [str(x) for x in DataSourceType]

        parser = argparse.ArgumentParser()
        parser.add_argument('--' + Runner._MODEL_ARG, \
            help="Generate a model from the data for the given time frame (if no time frame is given, it uses all the data). One of: " + str(valid_models), \
            type=self._valid_model_type, \
            choices=list(ModelType), \
            required=False)

        parser.add_argument('--' + Runner._DATA_ARG, \
            help="Gathers data from the given source. If this option is used, a startTime must be specified. Enter as a SPACE seperated list. Note: injuries gathers data from prosports. One of: " + str(valid_data), \
            nargs='*', \
            type=self._valid_data_type_list, \
            choices=list(DataSourceType), \
            required=False)

        parser.add_argument('--' + Runner._START_DATE_ARG, \
            help="Specify the startTime of the mode/gathering of data. Default is: " + str(Runner.EARLIEST_START_DATE) + ". MUST be between " + str(Runner.EARLIEST_START_DATE) + " and " + str(Runner.LATEST_END_DATE), \
            nargs='?', \
            default=Runner.EARLIEST_START_DATE, \
            type=self._valid_date, \
            required=False)

        parser.add_argument('--' + Runner._END_DATE_ARG, \
            help="Specify the endTime of the mode/gathering of data. Default is datetime.now(). MUST be between " + str(Runner.EARLIEST_START_DATE) + " and " + str(Runner.LATEST_END_DATE), \
            nargs='?', \
            default=Runner.LATEST_END_DATE, \
            type=self._valid_date, \
            required=False)

        return parser

    def parse_args(self, args):
        """Creates and validates parsed arguments for the Runner to use to parse command line arguments."""
        parser = self._create_parser()
        parsed_args = vars(parser.parse_args(args))

        if parsed_args[Runner._MODEL_ARG] is None and parsed_args[Runner._DATA_ARG] is None:
            parser.error("Must indicate either --" + Runner._MODEL_ARG + " and/or --" + Runner._DATA_ARG + " options")
        elif parsed_args[Runner._START_DATE_ARG] >= parsed_args[Runner._END_DATE_ARG]:
            parser.error("--startTime must be strictly less than --endTime argument!!")
        else:
            return parsed_args

    def Run(self):
        '''Main function to run logic of models/gather data.'''
        args = self.parse_args(sys.argv[1:])

        print "Running with args: " + str(args)

        ###TO-DO:execution.
        return
