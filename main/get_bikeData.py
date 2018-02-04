baseUrl = "https://s3.amazonaws.com/tripdata/index.html/"
#Bike filename prefix
bikePrefix = "/bike_tripdata__"

#Availaiblity of data set by month & year
citibikeDict = {}

citibikeDict[2017] = range(1,7)

bikeUrls = []
bikeFilenames = []
for year, monthList in citibikeDict.iteritems():
    yearStr = str(year)
    for month in monthList:
        monthStr = str(month)
        if len(monthStr) == 1:
            monthStr = "0"+monthStr    
        url = baseUrl+'JC-'+yearStr+monthStr+"-citibike-tripdata.csv.zip"
        bikeUrls.append(url)
        bikeFilenames.append(bikePrefix+yearStr+'-'+monthStr+".csv")

for url in bikeUrls:
    wget $url -P datafile/nyc --trust-server-names
