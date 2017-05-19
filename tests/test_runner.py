#pylint: disable=W0212,line-too-long,C0103
import unittest
from argparse import ArgumentError
from src.main.runner import Runner
from src.main.dataSourceType import DataSourceType
from src.main.modelType import ModelType

class TestRunner(unittest.TestCase):
    '''
        Class which tests the main logic of the Runner class in src/main/Runner.py
    '''

    ############### TESTS FOR ARGUMENT PARSING ##############################
    def run_parse_args(self, model=None, data=None, start_date=None, end_date=None, raises_system_error=False, raises_argument_error=False):
        '''
            wrapper function to create a runner and test behavior of runner.parse_args(...)
        '''

        args = list()

        if model is not None:
            args.extend(["--" + Runner._MODEL_ARG, model])

        if data is not None:
            args.extend(["--" + Runner._DATA_ARG, data])

        if start_date is not None:
            args.extend(["--" + Runner._START_DATE_ARG, start_date])

        if end_date is not None:
            args.extend(["--" + Runner._END_DATE_ARG, end_date])

        runner = Runner()

        if raises_system_error:
            with self.assertRaises(SystemExit):
                runner.parse_args(args)

        elif raises_argument_error:
            with self.assertRaises(ArgumentError):
                runner.parse_args(args)
        else:
            arg_map = runner.parse_args(args)

            actual_model = arg_map[Runner._MODEL_ARG]
            actual_data = arg_map[Runner._DATA_ARG]
            actual_start_date = arg_map[Runner._START_DATE_ARG]
            actual_end_date = arg_map[Runner._END_DATE_ARG]

            if model is not None:
                self.assertEqual(actual_model, ModelType[model], "model does not match actual: " + str(actual_model) + ", expected: " + model)

            if data is not None:
                self.assertEqual(actual_data, DataSourceType[data], "data does not match. actual: " + str(actual_data) + ", expected:" + data)

            expected_start_date = runner._valid_date(start_date) if start_date is not None else Runner.EARLIEST_START_DATE
            expected_end_date = runner._valid_date(end_date) if end_date is not None else Runner.LATEST_END_DATE

            self.assertEqual(actual_start_date, expected_start_date, Runner._START_DATE_ARG + " does not match. actual: " + str(actual_start_date) + ", expected: " + str(expected_start_date))
            self.assertEqual(actual_end_date, expected_end_date, Runner._END_DATE_ARG + " does not match. actual: " + str(actual_end_date) + ", expected: " + str(expected_end_date))


    def test_arg_parse_model_invalid(self):
        '''Tests the scenario in which a user enters an invalid model argument'''
        invalid_model = 'notARealModel'
        self.run_parse_args(model=invalid_model, raises_system_error=True)

    def test_arg_parse_model_invalid_data_valid(self):
        '''Tests the scenario in which a user enters an invalid model argument, but a valid data argument'''
        invalid_model = 'notARealModel'
        valid_data = str(DataSourceType.all)
        self.run_parse_args(model=invalid_model, data=valid_data, raises_system_error=True)

    def test_arg_parse_data_invalid(self):
        '''Tests the scenario in which a user enters an invalid data argument'''
        invalid_data = 'notARealData'
        self.run_parse_args(data=invalid_data, raises_system_error=True)

    def test_data_invalid_model_valid(self):
        '''Tests the scenario in which a user enters an invalid data argument, but a valid model argument'''
        valid_model = str(ModelType.placeholder)
        invalid_data = 'notARealData'
        self.run_parse_args(model=valid_model, data=invalid_data, raises_system_error=True)

    def test_model_invalid_data_invalid(self):
        '''Tests the scenario in which a user enters an invalid data argument and model argument'''
        invalid_model = 'notARealModel'
        invalid_data = 'notARealData'
        self.run_parse_args(model=invalid_model, data=invalid_data, raises_system_error=True)

    def test_model_none_data_none(self):
        '''Tests the scenario in which a user enters neither data argument and model argument'''
        self.run_parse_args(raises_system_error=True)

    def test_start_date_too_late(self):
        '''Tests the scenario in which a user enters a start date prior to LATEST_END_DATE'''
        valid_model = str(ModelType.placeholder)
        invalid_start_date = '3000-01-01'
        self.run_parse_args(model=valid_model, start_date=invalid_start_date, raises_system_error=True)

    def test_start_date_too_early(self):
        '''Tests the scenario in which a user enters a start date prior to EARLIEST_START_DATE'''
        valid_model = str(ModelType.placeholder)
        invalid_start_date = '1999-12-01'
        self.run_parse_args(model=valid_model, start_date=invalid_start_date, raises_system_error=True)

    def test_start_date_earlier_than_end_date(self):
        '''Tests the scenario in which a user enters a start date prior to end_date'''
        valid_model = str(ModelType.placeholder)
        invalid_start_date = '2017-01-01'
        invalid_end_date = '2016-12-31'
        self.run_parse_args(model=valid_model, start_date=invalid_start_date, end_date=invalid_end_date, raises_system_error=True)

    def test_end_date_too_early(self):
        '''Tests the scenario in which a user enters an end_date before the earliest start date'''
        valid_model = str(ModelType.placeholder)
        invalid_end_date = '1999-12-31'
        self.run_parse_args(model=valid_model, end_date=invalid_end_date, raises_system_error=True)

    def test_end_date_too_late(self):
        '''Tests the scenario in which a user enters an end_date after the LATEST_END_DATE'''
        valid_model = str(ModelType.placeholder)
        invalid_end_date = '1999-12-31'
        self.run_parse_args(model=valid_model, end_date=invalid_end_date, raises_system_error=True)

    def test_valid_model(self):
        '''Tests the scenario in which a user enters a valid model'''
        valid_model = str(ModelType.placeholder)
        self.run_parse_args(model=valid_model)

    def test_valid_data(self):
        '''Tests the scenario in which a user enters a valid data'''
        valid_data = str(DataSourceType.all)
        self.run_parse_args(data=valid_data)

    def test_valid_start_time(self):
        '''Tests the scenario in which a user enters a valid start time'''
        valid_model = str(ModelType.placeholder)
        valid_start_date = '2017-01-01'
        self.run_parse_args(model=valid_model, start_date=valid_start_date)

    def test_valid_end_time(self):
        '''Tests the scenario in which a user enters a valid start time'''
        valid_model = str(ModelType.placeholder)
        valid_end_date = '2017-01-01'
        self.run_parse_args(model=valid_model, end_date=valid_end_date)

    def test_valid_all(self):
        '''Tests the scenario in which a user enters a valid start time'''
        valid_model = str(ModelType.placeholder)
        valid_data = str(DataSourceType.all)
        valid_start_date = '2016-01-01'
        valid_end_date = '2017-01-01'
        self.run_parse_args(model=valid_model, data=valid_data, start_date=valid_start_date, end_date=valid_end_date)

    ############### END OF TESTS FOR ARGUMENT PARSING ##############################
