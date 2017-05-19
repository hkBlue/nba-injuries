from src.shared.dataSourceType import DataSourceType

class PgtdCleaner(object):
    def clean(self, config, file_name, time_range_begin, time_range_end):
        pass

class PgpdCleaner(object):
    def clean(self, config, file_name, time_range_begin, time_range_end):
        pass

class InjuriesCleaner(object):
    def clean(self, config, file_name, time_range_begin, time_range_end):
        pass

def clean(config, data_type, file_name, time_range_begin, time_range_end):
    '''
        Function which delegates cleaning to the proper helper function
        given the data_type.
    '''
    cleaner = None
    if data_type == DataSourceType.injuries:
        cleaner = PgtdCleaner()
    elif data_type == DataSourceType.pgpd:
        cleaner = PgpdCleaner()
    elif data_type == DataSourceType.pgtd:
        cleaner = InjuriesCleaner()
    else:
        raise  ValueError("Data source type not found.")
    cleaner.clean(config, file_name, time_range_begin, time_range_end)
    return
