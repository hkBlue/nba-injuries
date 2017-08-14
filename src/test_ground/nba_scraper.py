#pylint: disable=line-too-long,too-many-arguments,E1121,W0512
import threading
from Queue import Queue
import sys
from urlparse import urlparse
import urllib2
from datetime import datetime, timedelta
import pandas as pd
from bs4 import BeautifulSoup as BS

def get_df_from_column(line, column_to_parse_mapping):
    '''
        translates line from BeautfiulSoup into a column for our data frame
    '''
    datum = pd.DataFrame()
    for column in column_to_parse_mapping:
        value = column_to_parse_mapping[column](line)
        datum.set_value(0, column, value)
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
        datum = get_df_from_column(line, column_to_parse_mapping)
        sys.stdout.write("\tFrom html: " + str(line) + "\n")

        sys.stdout.write("\tAdding data: \n" + str(datum) + "\n")
        complete_df = complete_df.append(datum, ignore_index=False)

    return complete_df

class ThreadSafeCounter(object):
    '''
        A counter to use for thread safe operations on a counter variable.
    '''
    def __init__(self):
        self.lock = threading.Lock()
        self.counter = 0

    def increment(self):
        '''
            Thread safe increment.
        '''
        with self.lock:
            self.counter += 1
            return self.counter


    def decrement(self):
        '''
            Thread safe decrement.
        '''
        with self.lock:
            self.counter -= 1
            return self.counter

class ProSportsScraperProperties(object):
    '''
        Class for encapsulating properties needed for Beautiful Soup scraping of ProSports
    '''
    def __init__(self, base_url_formatted, columns, start_date, end_date, date_format, ignore_index):
        self.columns = columns

        #This is just an artefact of how the page loads. It loads 25 results per page.
        self.max_results_per_page = 25

        start_date_str = start_date.strftime(date_format)
        end_date_str = end_date.strftime(date_format)
        self.base_url = base_url_formatted.format(startDate=start_date_str, endDate=end_date_str)
        self.readable_str = "ProSports Injury data ({start}, {end})".format(start=start_date_str, end=end_date_str)
        self.page_num = ThreadSafeCounter()
        self.page_num.decrement() # since increment is called every time
        self.ignore_index = ignore_index

    def __str__(self):
        return self.readable_str

    def get_num_pages(self):
        url_str = self.base_url + str(0)
        soup = self._get_bs_response(url_str)
        sys.stderr.write(",".join([str(x) for x in soup.find("p", class_="bodyCopy").find_all('a')]) + "\n")
        sys.stdout.flush()
        max_page_number = int(soup.find("table", {"width" : "75%", "cellpadding" : "5", "align" : "center" }).find_all("p", class_="bodyCopy")[1].find_all('a')[-1].text)
        return max_page_number

    def _get_bs_response(self, url):
        sys.stdout.write("[ProSportsProperties] Requesting info from URL: " + url + "\n")
        try:
            response = urllib2.urlopen(url).read()
        except Exception as e:
            sys.stdout.flush()
            sys.stdout.write("[ProSportsProperties] Failed getting info from URL: " + url + "\n")

        soup = BS(response, 'html.parser')

        return soup


    def get_next_bs_response(self):
        '''
            Gets the response from the next page associated with the base_url of this properties object.
        '''
        current_page_num = self.page_num.increment()
        url_str = self.base_url + str(self.max_results_per_page * current_page_num)

        return self._get_bs_response(url_str).find_all('tr', {'align' : 'left'})

    def get_column_to_parse_mapping(self):
        '''
            Gets a map from columns to associated parsing functions for each column.
            Such that the parsing functions take a line from a BeautifulSoup response,
            and translate it into the column's value.
            ["Date","Team","Acquired","Relinquished","Notes"]
        '''
        name_parse = lambda names: names.split('/', 1)[0].lstrip().rstrip()
        return {
            #Date
            self.columns[0]: lambda bs_line: bs_line.select('td')[0].text.lstrip(),

            #Team
            self.columns[1]: lambda bs_line: bs_line.select('td')[1].text.lstrip(),

            #Accquired - grab only first official name -- '\u2022 Maurice Williams / Mo Williams' -> Maurice Williams
            self.columns[2]: lambda bs_line: name_parse(bs_line.select('td')[2].text.replace(u' \u2022 ', "")),

            #Relinquished - grab only first official name -- '\u2022 Maurice Williams / Mo Williams' -> Maurice Williams
            self.columns[3]: lambda bs_line: name_parse(bs_line.select('td')[3].text.replace(u' \u2022 ', "")),

            #Notes
            self.columns[4]: lambda bs_line: bs_line.select('td')[4].text.lstrip(),
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
            try:
                result = self.queue.get()
                sys.stdout.write("[WriterWorker] Writing: \n\t" + str(result) + "\n")
                self.write(result)
            except Exception as e:
                sys.stdout.flush()
                sys.stdout.write("[WriterWorker] Error caught: " + str(e))
            finally:
                self.queue.task_done()

class ScraperWorker(threading.Thread):
    '''
        Worker that takes an in_queue of scraper properties and writes the result as a Pandas data frame out_queue.
    '''
    def __init__(self, in_queue, out_queue):
        threading.Thread.__init__(self)
        self._in_queue = in_queue
        self._out_queue = out_queue

    def _scrape(self, scraper_props):
        '''
            Function which takes the scraper properties and scrapes the data associated with it.
        '''
        response = scraper_props.get_next_bs_response()
        data = pd.DataFrame()
        while response: #while response isn't empty.
            data = add_to_df(data, response, scraper_props.get_column_to_parse_mapping(), ignore_index=scraper_props.ignore_index)
            response = scraper_props.get_next_bs_response()
        return data

    def run(self):
        while True:
            try:
                scraper_props = self._in_queue.get()
                #Have to use sys.stdout.write(... "\n") in order to write new lines in a non-erratic manner while threading.
                sys.stdout.write(str(datetime.now()) + " [ScraperWorker] Starting: " + str(scraper_props) + "\n")
                data = self._scrape(scraper_props)
                self._out_queue.put(data)
                sys.stdout.write(str(datetime.now()) + " [ScraperWorker] Finished: " + str(scraper_props) + "\n")
                sys.stdout.flush()
            except Exception as e:
                sys.stdout.flush()
                sys.stdout.write("[ScraperWorker] Error caught: " + str(e))
            finally:
                self._in_queue.task_done()

class DateFrameWriter:
    '''
        Writer that writes a date frame to a csv.
    '''
    def __init__(self, output_file, overwrite=True):
        self._output_file = output_file
        if overwrite:
            open(self._output_file, 'w').close()

    def _write_header(self, columns):
        with open(self._output_file, 'r') as f:
            header = f.readline()
            if not header:
                with open(self._output_file, 'w') as o:
                    sys.stdout.write("Writing header..." + str(columns) + "\n")
                    o.write("Index," + ",".join(columns) + "\n")

    def write_data_frame(self, data_frame):
        '''
            write data frame to csv
        '''
        columns = list(data_frame.columns.values)
        self._write_header(columns)
        with open(self._output_file, 'a+') as f:
            data_frame.to_csv(f, header=False)

def date_range(start, end, intervals):
    '''
        Divides the start to end time into a intervals number of dates.abs
        If the interval results in the average difference being < 1 day,
        then defaults to using 1 day intervals
    '''
    diff_time = (end - start) / intervals
    if diff_time > timedelta(1):
        diff_time = timedelta(1)
        intervals = (end - start).days / diff_time.days
    result_dates = list()
    for i in range(intervals):
        result_dates.append(start + diff_time * i)
    result_dates.append(end)
    return result_dates

def scrape(num_scraper_threads, base_url, columns, file_name, time_range_begin, time_range_end):
    '''
        Main function which handles scraping.
    '''
    #open file to write results
    concurrent = num_scraper_threads
    in_queue = Queue(concurrent * 2)
    out_queue = Queue(concurrent * 2)
    for i in range(concurrent):
        scraper_worker = ScraperWorker(in_queue, out_queue)
        scraper_worker.daemon = True
        scraper_worker.start()

    writer = DateFrameWriter(file_name)
    writer_worker = WriterWorker(out_queue, lambda data_frame: writer.write_data_frame(data_frame))
    writer_worker.daemon = True

    writer_worker.start()

    #We take advantage of the structure of basketball reference to do a broad search and then just iterate the page numbers to get the rest of the information.
    #This is much faster than searching every single day for information.
    dates_for_props = date_range(time_range_begin, time_range_end, concurrent)
    properties = ProSportsScraperProperties(base_url, columns, time_range_begin, time_range_end, "%Y-%m-%d", False)
    max_page_number = properties.get_num_pages()
    map(in_queue.put, [properties for i in range(max_page_number)])

    try:
        sys.stdout.write("WAITING ON QUEUES TO FINISH...\n")
        in_queue.join()
        out_queue.join()
    except KeyboardInterrupt:
        sys.exit(1)


def test_scrape():
    scrape(8, "http://www.prosportstransactions.com/basketball/Search/SearchResults.php?Player=&Team=&BeginDate={startDate}&EndDate={endDate}&InjuriesChkBx=yes&Submit=Search&start=", ["Date","Team","Acquired","Relinquished","Notes"], "./out.txt", datetime(2004,1,1), datetime(2017,7,1))