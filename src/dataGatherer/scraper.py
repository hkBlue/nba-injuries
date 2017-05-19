from src.shared.dataSourceType import DataSourceType

class PgtdScraper:
    '''
        Class which contains methods to help in scraping Pgtd data.
    '''
    def scrape(self, config, file_name, time_range_begin, time_range_end):
        pass

class PgpdScraper:
    '''
        Class which contains methods to help in scraping Pgpd data.
    '''
    def scrape(self, config, file_name, time_range_begin, time_range_end):
        pass

class InjuriesScraper:
    '''
        Class which contains methods to help in scraping Injuries data.
    '''
    def scrape(self, config, file_name, time_range_begin, time_range_end):
        pass

def scrape(config, data_type, file_name, time_range_begin, time_range_end):
    '''
        Function to call to scrape data for the given data_type.abs
        Delegates to the proper helper method.
    '''
    scraper = None
    if data_type == DataSourceType.injuries:
        scraper = InjuriesScraper()
    elif data_type == DataSourceType.pgpd:
        scraper = PgpdScraper()
    elif data_type == DataSourceType.pgtd:
        scraper = PgtdScraper()
    else:
        raise  ValueError("Data source type not found.")
    scraper.scrape(config, file_name, time_range_begin, time_range_end)

    return
