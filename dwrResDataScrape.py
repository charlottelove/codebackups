# -*- coding: utf-8 -*-
"""
Created on Thu May 19 20:33:31 2016

Script to pull daily reservoir storage data tables from California DWR's
webpages at http://cdec.water.ca.gov/misc/daily_res.html 
Assembles each data table into pandas DataFrame and appends to a csv 
file. 

Currently set to pull data for Friant Dam from 01/01/1994 - present
To change, update:
	fname = output filename as a string
	start_date = last date of first page as string in %d-%b-%Y format
	stationstr = station's 3-letter abbreviation as a string in all-caps

author: Charlotte Love <calove@uci.edu>
"""

from urllib import urlopen
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import datetime
import time
import csv

## USER EDITS =================================================================

fname = "FriantDam_DWR_DataTables.csv" # output file name
start_date = "30-Jan-1994" # list here the last date in the table on first page
stationstr = "MIL" # station 3-letter abbreviation

## END USER EDITS =============================================================

## setup for url that we are scrapping
urlstart = ('http://cdec.water.ca.gov/cgi-progs/queryDaily?' + stationstr +
		'&d=' + start_date + '+10:32&span=30days')
url = urlstart # for first loop

# pull out parts before/after date for updating page each loop
urlparts = urlstart.split("+")
urlpart1 = urlparts[0][:-11]
urlpart2 = '+' + urlparts[1]

# end date for exiting while loop
datetoday = time.strftime("%d-%b-%Y")
end_date = ( datetime.datetime.strptime(datetoday,  "%d-%b-%Y") + 
	    datetime.timedelta(days=30) ) # for exiting while loop

# grab final header list for correct data column sorting
urlhdr = urlpart1 + datetoday + urlpart2
html = urlopen(urlhdr)
soup = BeautifulSoup(html, "lxml") # grab html code
colhdr = soup.findAll('tr', limit=2)[0].findAll('font')
dayhdr = colhdr[0].contents
datahdr = colhdr[1:]

# column names
for i in range(len(datahdr)):
	colheader = datahdr[i]
	colheader = colheader.a
	colheader = colheader.contents
	datahdr[i] = colheader

headersALL = np.append(dayhdr, datahdr)
headersALL = [str(x) for x in headersALL] # will be used to organize columns

# data units
unitshdr = soup.findAll('tr', limit=2)[1].findAll('font')
dataunits = unitshdr[1:]
for i in range(len(dataunits)):
	data_unit = dataunits[i]
	data_unit = data_unit.b.contents
	dataunits[i] = data_unit
dataunits = np.append(" ", dataunits)
dataunits = [str(x) for x in dataunits]

# write headers to final CSV file
with open(fname, 'wb') as myfile:
	wr = csv.writer(myfile)
	wr.writerow(headersALL)
	wr.writerow(dataunits)

# start date for enter while loop
datenewpage = datetime.datetime.strptime(start_date,  "%d-%b-%Y") 

## Scrape data from web-based data tables =====================================
while datenewpage<end_date:

	html = urlopen(url) # open page

	soup = BeautifulSoup(html, "lxml") # grab html code

	## grab and clean-up headers ------------------------------------------
	col_headers = soup.findAll('tr', limit=2)[0].findAll('font')

	# date header only
	dayheader = col_headers[0].contents

	# data headers (have different html coding)
	dataheader = col_headers[1:]

	for i in range(len(dataheader)):
		colheader = dataheader[i]
		colheader = colheader.a
		colheader = colheader.contents
		dataheader[i] = colheader

	headersTbl = np.append(dayheader, dataheader)
	headersTbl = [str(x) for x in headersTbl]

	## data rows -----------------------------------------------------------
	data_rows = soup.findAll('tr')[2:]

	res_data = [[td.getText() for td in data_rows[i].findAll('td')]
            	    for i in range(len(data_rows))]

	# convert from list to pandas DataFrame
	df = pd.DataFrame(res_data)

	# remove even columns (empty)
	Ncol = df.shape[1] # number of columns
	colcut = np.linspace(0,(Ncol+2),((Ncol+2)/2), endpoint=False)[1:]
	colcut = [int(x) for x in colcut] # convert to int

	## combine data and headers --------------------------------------------
	df = df.drop(df.columns[colcut], axis=1)
	Ncol = df.shape[1] # new number of columns
	NcolHeader = len(headersTbl)

	# add columns with NaN if there are extra headers
	if NcolHeader > Ncol:
		extraHdrs = NcolHeader - Ncol
		extraHdrs = np.linspace(1,extraHdrs,extraHdrs)
		lastidx = df.dtypes.index[-1:]
		newidx = np.array(lastidx) + extraHdrs
		df = pd.concat([df,pd.DataFrame(columns=list(newidx))])
	
	df.columns = headersTbl

	## sort columns --------------------------------------------------------
	newdf = pd.DataFrame(np.nan, index=range(30), columns=range(len(headersALL)))
     	newdf.columns = headersALL
      
     	for i in range(len(headersALL)):
		try:
              		newdf[headersALL[i]] = df[headersALL[i]]
          	except:
              		pass
	
	## replace "--" with "NaN" ---------------------------------------------
	newdf = newdf.replace(u'        --', 'NaN')
          
	## append to output file -----------------------------------------------
	newdf.to_csv(fname, na_rep='NaN', header=False, index=False, mode='a')
	
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

