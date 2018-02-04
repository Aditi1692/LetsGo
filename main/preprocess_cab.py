import time
from datetime import date
import math

def data_cleaner(zipped_row):
    # takes a tuple (row,g,b,minutes_per_bin) as a parameter and returns a tuple of the form:
    # (time_cat, time_num, time_cos, time_sin, day_cat, day_num, day_cos, day_sin, weekend,geohash)
    row = zipped_row[0]
    g = zipped_row[1]
    b = zipped_row[2]
    minutes_per_bin = zipped_row[3]
    # The indices of longitude, and latitude respectively
    indices = (6, 5)
    
    #safety check: make sure row has enough features
    if len(row) < 7:
        return None
    
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


if __init__ == '__main__':
    create_feature()
