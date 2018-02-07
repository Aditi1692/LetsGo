import sys
import boto3

def set_up(self):
	baseUrl = "https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2017-01.csv"
	#Yellow/green cab filename prefix
	yCabFNPrefix = "/yellow_tripdata_"
	gCabFNPrefix = "/green_tripdata_"

	#Availaiblity of data set by month & year
	yDict = {}
	gDict = {}

	#availablity for Yellow cab
	yDict[2017] = range(1,7) #available till jun 2017
	yDict[2016] = range(1,13)
	yDict[2015] = range(1,13)

	#availablity for Green cab
	gDict[2017] = range(1,7) #available till jun 2017
	gDict[2016] = range(1,13)
	gDict[2015] = range(8,13) #avialable only from august 2015


def make_files(self):
	#  Yellow cab data file name list
	# file name is of format:  yellow_tripdata_2015-01.csv
	yCabUrls = []
	yCabFilenames = []
	for year, monthList in yDict.iteritems():
	    yearStr = str(year)
	    for month in monthList:
        	monthStr = str(month)
	        if len(monthStr) == 1:
        	    monthStr = "0"+monthStr    
	        url = baseUrl+yearStr+yCabFNPrefix+yearStr+'-'+monthStr+".csv"
        	yCabUrls.append(url)
	        yCabFilenames.append(yCabFNPrefix+yearStr+'-'+monthStr+".csv")

	#  green cab data file name list
		gCabUrls = []
	gCabFilenames = []
	for year, monthList in gDict.iteritems():
	    yearStr = str(year)
	    for month in monthList:
        	monthStr = str(month)
	        if len(monthStr) == 1:
	            monthStr = "0"+monthStr    
        	url = baseUrl+yearStr+gCabFNPrefix+yearStr+'-'+monthStr+".csv"
	        gCabFilenames.append(gCabFNPrefix+yearStr+'-'+monthStr+".csv")
	        gCabUrls.append(url)


