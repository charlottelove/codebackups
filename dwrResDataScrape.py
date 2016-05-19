'''
Script to pull daily reservoir storage data tables from California DWR's
webpages at http://cdec.water.ca.gov/misc/daily_res.html 
Assembles each data table into pandas DataFrame and appends to a csv 
file. 

Improvements needed:
	- Really, really need to figure out how to handle "shifting"
	data columns. The data tables gain new data columns through
	time, and sometimes the order of columns changes. 
	Ideas: Set headers for final DataFrame at start, and match 
		the headers read in to the pre-set headers. Fill
		empty columns with NaN. 
	- Once the above issue is accounted for, only print columns
	headers once before looping.
	* Currently set to print headers each table, since the columns
	shift between pages. (Annoying since it requires manually 
	editing the final CSV.)

 Charlotte Love <calove@uci.edu>
 05/18/2016
'''

from urllib import urlopen
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import datetime

## setup for url that we are scrapping
# currently set to start on Friant Dam (station name: MIL) daily reservoir 
# data table for 01-Jan-1994
urlstart = 'http://cdec.water.ca.gov/cgi-progs/queryDaily?MIL&d=01-Jan-1994+13:07&span=30days'
urlparts = urlstart.split("+")
urlpart1 = urlparts[0][:-11]
urlpart2 = '+' + urlparts[1]
url = urlstart # for first loop


end_date = datetime.datetime.strptime('17-May-2016',  "%d-%b-%Y") # for exiting while loop
datenewpage = datetime.datetime.strptime('01-Jan-1994',  "%d-%b-%Y") # just to get into loop

# output file name
fname = "FriantDam_DWR_DataTables.csv"

## Scrape data from web-based data tables ===================================
while datenewpage!=end_date: # risky conditional statement... need to update this

	html = urlopen(url) # open page

	soup = BeautifulSoup(html, "lxml") # grab html code

	## grab and clean-up headers ------------------------------------------
	col_headers = soup.findAll('tr', limit=2)[0].findAll('font')

	# date header only
	dayheader = col_headers[0]
	dayheader = dayheader.contents

	# data headers (have different html coding)
	dataheader = col_headers[1:]

	for i in range(len(dataheader)):
		colheader = dataheader[i]
		colheader = colheader.a
		colheader = colheader.contents
		dataheader[i] = colheader

	headersALL = np.append(dayheader, dataheader)
	headersALL = [str(x) for x in headersALL]

	## data rows -----------------------------------------------------------
	data_rows = soup.findAll('tr')[2:]

	res_data = [[td.getText() for td in data_rows[i].findAll('td')]
            	    for i in range(len(data_rows))]

	# convert from list to pandas DataFrame
	df = pd.DataFrame(res_data)

	# remove even columns (empty)
	Ncol = df.shape[1] # number of columns
	colcut = np.linspace(0,(Ncol+2),((Ncol+2)/2), endpoint=False)
	colcut = colcut[1:]
	colcut = [int(x) for x in colcut] # convert to int

	## combine data and headers --------------------------------------------
	df = df.drop(df.columns[colcut], axis=1)
	Ncol = df.shape[1] # new number of columns
	
	NcolHeader = len(headersALL)

	# add columns with NaN if there are extra headers
	if NcolHeader > Ncol:
		extraHdrs = NcolHeader - Ncol
		extraHdrs = np.linspace(1,extraHdrs,extraHdrs)
		lastidx = df.dtypes.index[-1:]
		newidx = np.array(lastidx) + extraHdrs
		df = pd.concat([df,pd.DataFrame(columns=list(newidx))])
	
	df.columns = headersALL
	df.set_index(headersALL[0])
	
	## date for next url ---------------------------------------------------
	Nrows = df.shape[0]
	lastdate = df.ix[Nrows-1][0] # pull last date in table
	# ^^ use this date to calculate start date for next page 
	
	formatdate = "%m/%d/%Y"
	lastdate = datetime.datetime.strptime(lastdate, formatdate) # convert format

	# add 30 days to get url date (which is last date in next pages table)
	datenewpage = lastdate + datetime.timedelta(days=30) 

	# convert to day-month-year string
	newformat = "%d-%b-%Y"
	urldate = datetime.datetime.strftime(datenewpage, newformat)

	# create new url string :)
	url = urlpart1 + urldate + urlpart2

	## append table to file ------------------------------------------------
	df.to_csv(fname, mode='a')

