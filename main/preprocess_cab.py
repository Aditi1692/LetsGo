import time
from datetime import date
import math

def date_extractor(date_str,b,minutes_per_bin):
    # Takes a datetime object as a parameter
    # and extracts and returns a tuple of the form: (as per the data specification)
    # (time_cat, time_num, time_cos, time_sin, day_cat, day_num, day_cos, day_sin, weekend)
    # Split date string into list of date, time
    
    d = date_str.split()
    
    #safety check
    if len(d) != 2:
        return tuple([None,])
    
    # TIME (eg. for 16:56:20 and 15 mins per bin)
    #list of hour,min,sec (e.g. [16,56,20])
    time_list = [int(t) for t in d[1].split(':')]
    
    #safety check
    if len(time_list) != 3:
        return tuple([None,])
    
    # calculate number of minute into the day (eg. 1016)
    num_minutes = time_list[0] * 60 + time_list[1]
    
    # Time of the start of the bin
    time_bin = num_minutes / minutes_per_bin     # eg. 1005
    hour_bin = num_minutes / 60                  # eg. 16
    min_bin = (time_bin * minutes_per_bin) % 60  # eg. 45
    
    #get time_cat
    hour_str = str(hour_bin) if hour_bin / 10 > 0 else "0" + str(hour_bin)  # eg. "16"
    min_str = str(min_bin) if min_bin / 10 > 0 else "0" + str(min_bin)      # eg. "45"
    time_cat = hour_str + ":" + min_str                                     # eg. "16:45"
    
    # Get a floating point representation of the center of the time bin
    time_num = (hour_bin*60 + min_bin + minutes_per_bin / 2.0)/(60*24)      # eg. 0.7065972222222222
    
    time_cos = math.cos(time_num * 2 * math.pi)
    time_sin = math.sin(time_num * 2 * math.pi)
       
    return (year, month, day, time_cat, time_num, time_cos, time_sin)

def data_cleaner(zipped_row):
    # takes a tuple (row,g,b,minutes_per_bin) as a parameter and returns a tuple of the form:
    # (time_cat, time_num, time_cos, time_sin)
    row = zipped_row[0]
    g = zipped_row[1]
    b = zipped_row[2]
    minutes_per_bin = zipped_row[3]
    # The indices of pickup datetime, longitude, and latitude respectively
    indices = (1, 6, 5)
    
    #safety check: make sure row has enough features
    if len(row) < 7:
        return None
    
    #extract day of the week and hour
    date_str = row[indices[0]]
    clean_date = date_extractor(date_str,b,minutes_per_bin)
    #get geo hash

    latitude = float(row[indices[1]])
    longitude = float(row[indices[2]])
    location = None
    #safety check: make sure latitude and longitude are valid
    if latitude < 41.1 and latitude > 40.5 and longitude < -73.6 and longitude > -74.1:
        location = geohash.encode(latitude,longitude, g)
    else:
        return None

    return tuple(list(clean_date)+[location])


def create_feature(self):
    gclean_rdd = g_rdd.map(lambda row: (row, g, b, minutes_per_bin))\
		      .map(data_cleaner)\
		      .filter(lambda row: row != None)\
		      .map(lambda row: (row,1))\
  		      .reduceByKey(lambda a,b: a + b)\
		      .map(lambda row: (row,'g'))        

    yclean_rdd = y_rdd.map(lambda row: (row, g, b, minutes_per_bin))\
		.map(data_cleaner)\
		.filter(lambda row: row != None)\
		.map(lambda row: (row,1))\
		.reduceByKey(lambda a,b: a + b)\
		.map(lambda row: (row,'y'))         
    
    combined_rdd = yclean_rdd.union(gclean_rdd)   # Create a combined dataset of yellow and green taxis
						  #get rid of g, y letters and reduce
    final_rdd = combined_rdd.map(lambda row: row[0])\
		.reduceByKey(lambda a,b: a + b)\
		.map(lambda (a,b): (a,b,np.random.random()))  # Add a random number


def clean_data(self):
    y_rdd = sc.textFile("s3://" + bucket + "/nyc/yellow*.csv")
    y_rdd = y_rdd.map(lambda line: tuple(line.split(',')))
    g_rdd = sc.textFile("s3://" + bucket + "/nycg/green*.csv")
    g_rdd = g_rdd.map(lambda line: tuple(line.split(',')))


if __name__ == "__main__":
    create_feature()
