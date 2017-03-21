from bs4 import BeautifulSoup
import urllib

#open file to write results
import_file = open('injury_import.txt', 'w') 

#set and read html doc
num_pages_returned = 490 #total pages returned from query
url = 'http://www.prosportstransactions.com/basketball/Search/SearchResults.php?Player=&Team=&BeginDate=2006-07-01&EndDate=2017-03-31&InjuriesChkBx=yes&Submit=Search&start='

for start_val in range(num_pages_returned): #iterate through all the pages
	curr_url = url + str(start_val*25) #update url for current page
	html_doc = urllib.urlopen(curr_url).read() #open url with urllib
	soup = BeautifulSoup(html_doc, 'lxml') 	#initialize beautiful soup
	elements = soup.table.findAll('td') #find all td elements in table

	if start_val % 10 == 0: #print progress
		print start_val + 1

	#extract relevant elements
	count = 1 #initialize count for output
	for el in elements:
		import_file.write(u''.join((el.get_text())).encode('utf-8')) #write data value in td element 
		if count % 5 != 0: #if still writing row
			import_file.write(',') #write , to delimeate file
		else:
			import_file.write('\n') #start new line
		count = count + 1 #iterate count