#pylint: disable=line-too-long,too-many-arguments,E1121,W0512
from bs4 import BeautifulSoup as BS
import requests
import pandas as pd
import numpy as np
import datetime as dt
from Queue import Queue
import threading
import sys
from datetime import datetime
import httplib
from urlparse import urlparse

def get_df_from_column(columns, bs, line, column_to_parse_mapping):
    datum = pd.DataFrame()
    for column in columns:
        datum[column] = column_to_parse_mapping[column](line)
    return datum

def add_to_df(complete_df, raw_bs, column_to_parse_mapping, ignore_index=False):
    '''
        param complete_df : the df to add the generated data frames to.abs
        param bs : the request results from a BeautifulSoup result
        param tag : the tag to select from the BeautifulSoup result
        param column_to_parse_mapping : a mapping from the columns of df to functions which take a line of the request and parse it.
        param ignore_index : whether to ignore the index when appending.
    '''
    for line in raw_bs:
        datum = get_df_from_column(complete_df.columns, raw_bs, line, column_to_parse_mapping)
        complete_df.append(datum, ignore_index=ignore_index)

class ProSportsScraperProperties(object):
    def __init__(self, base_url_formatted, columns, start_date, end_date, start_num, date_format, ignore_index):
        self.columns = columns

        start_date_str = start_date.strftime(date_format)
        end_date_str = end_date.strfttime(date_format)
        self.base_url = base_url_formatted.format(startDate=start_date_str, endDate=end_date_str)
        self.readable_str = "ProSports Injury data ({start}, {end})".format(start=start_date_str, end=end_date_str)
        self.page_num = 1
        self.start_num = start_num
        self.ignore_index = ignore_index

    def __str__(self):
        return self.readable_str

    def get_next_bs_response(self):
        '''
            Gets the response from the next page associated with the base_url of this properties object.
        '''
        url_str = self.base_url + str(self.start_num * self.page_num)
        url = urlparse(url_str)
        conn = httplib.HTTPConnection(url.netloc)
        conn.request("HEAD", url.path)
        soup = BS(conn.getresponse(), 'html.parser')
        conn.close()
        self.page_num += 1
        return soup.select("[align~=left]")
    
    def get_column_to_parse_mapping(self):
        '''
            Gets a map from columns to associated parsing functions for each column.
            Such that the parsing functions take a line from a BeautifulSoup response,
            and translate it into the column's value.
        '''
        return {
            self.columns[0]: lambda bs_line: [bs_line.select('td')[0].text],
            self.columns[1]: lambda bs_line: [bs_line.select('td')[1].text],
            self.columns[2]: lambda bs_line: [bs_line.select('td')[2].text.replace(" â\x80¢ ", "").text],
            self.columns[3]: lambda bs_line: [bs_line.select('td')[3].text.replace(" â\x80¢ ", "").text],
            self.columns[4]: lambda bs_line: [bs_line.select('td')[4].text],
        }

class WriterWorker(threading.Thread):
    '''
        Worker that takes a queue, and a "write" function which can write a queue item.
    '''
    def __init__(self, queue, write):
        threading.Thread.__init__(self)
        self.queue = queue
        self.write = write

    def run(self):
        while True:
            result = self.queue.get()
            self.write(result)
            self.queue.task_done()

class ScraperWorker(threading.Thread):
    '''
        Worker that takes an in_queue of scraper properties and writes the result as a Pandas data frame out_queue.
    '''
    def __init__(self, in_queue, out_queue):
        threading.Thread.__init__(self)
        self.in_queue = in_queue
        self.out_queue = out_queue

    def _scrape(self, scraper_props):
        '''
            Function which takes the scraper properties and scrapes the data associated with it.
        '''
        response = scraper_props.get_next_bs_response()
        data = pd.DataFrame([], scraper_props.columns)
        while response: #while response isn't empty.
            add_to_df(data, response, scraper_props.get_column_to_parse_mapping(), ignore_index=scraper_props.ignore_index)
            response = scraper_props.get_next_bs_response()
        return data

    def run(self):
        while True:
            if(self.in_queue.all_tasks_done()):
                sys.stdout.write("[ScraperWorker] queue is empty")

            scraper_props = self.in_queue.get()
            #Have to use sys.stdout.write(... "\n") in order to write new lines in a non-erratic manner while threading.
            sys.stdout.write(str(datetime.now()) + " [ScraperWorker] Starting: " + scraper_props + "\n")
            data = self._scrape(scraper_props)
            self.out_queue.put(data)
            sys.stdout.write(str(datetime.now()) + " [ScraperWorker] Finished: " + scraper_props + "\n")
            sys.stdout.flush()
            self.in_queue.task_done()

def write_data_frame(output_file, data_frame):
    data_frame.to_csv(output_file)

def date_range(start, end, intervals):
    diff_time = (end - start) / intervals
    for i in range(intervals):
        yield (start + diff_time * i)
    yield end

def scrape(num_writer_threads, num_scraper_threads, base_url, header, file_name, time_range_begin, time_range_end, start_num):
    #open file to write results
    concurrent = 200
    in_queue = Queue(concurrent * 2)
    out_queue = Queue(concurrent * 2)
    for i in range(concurrent):
        scraper_worker = ScraperWorker(in_queue, out_queue)
        scraper_worker.daemon = True
        writer_worker = WriterWorker(out_queue, write_data_frame)
        writer_worker.daemon = True
        scraper_worker.start()
        writer_worker.start()
    
    diff = (time_range_end - time_range_begin) / concurrent

