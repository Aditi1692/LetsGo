from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

""" These are the only functions in the app that interface with the Citibike API

JSON Information about the stations is obtained from these links
"feeds":[  
    {  
       "name":"station_information",
       "url":"https://gbfs.citibikenyc.com/gbfs/en/station_information.json"
    },
    {  
       "name":"system_information",
       "url":"https://gbfs.citibikenyc.com/gbfs/en/system_information.json"
    },
    {  
       "name":"station_status",
       "url":"https://gbfs.citibikenyc.com/gbfs/en/station_status.json"
    },
    {  
       "name":"system_alerts",
       "url":"https://gbfs.citibikenyc.com/gbfs/en/system_alerts.json"
    },
    {  
       "name":"system_regions",
       "url":"https://gbfs.citibikenyc.com/gbfs/en/system_regions.json"
    }
 ]

      """
import requests, json
from model import db, Station, Trip
from sqlalchemy import exc
import boto3
import zipfile
import io
import time
from preprocess_cab import data_cleaner, clean_data

def seed_station_information():
	"""Get's and formats all station information in order to update stations in
	the database.

		point formatting is: 'POINT(lon lat)'

		Bike and dock information is initialized at zero

		https://gbfs.citibikenyc.com/gbfs/en/station_information.json"""

	response = requests.get('https://gbfs.citibikenyc.com/gbfs/en/station_information.json')
	response = json.loads(response.text)

	for station in response['data']['stations']:
		point = 'POINT(' + str(station['lon']) + ' ' + str(station['lat']) + ')'

		#print(point)
		new_station = Station(
							id = int(station['station_id']),
							name = station['name'],
							point = point,
							num_bikes_available=0, 
							num_docks_available=0)
		try:
			db.session.add(new_station)
			db.session.commit()
			print('Commitetd')
		except exc.IntegrityError:
			db.session.rollback()

def update_data():
	"Get all the details of batch processed data"

	session = boto3.session.Session(
        aws_access_key_id="AKIAITBW5KDFRGWH6U3Q",
        aws_secret_access_key="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"
        )

        s3 = session.resource("s3")
        bucket = s3.Bucket('citibikenycdataset')
        obj = bucket.Object('201307-201402-citibike-tripdata.zip')
	i=0
        with io.BytesIO(obj.get()["Body"].read()) as tf:

                # rewind the file
                tf.seek(0)

                # Read the file as a zipfile and process the members
                zfile =  zipfile.ZipFile(tf, mode='r')
                for finfo in zfile.infolist():
			first = True
                        ifile = zfile.open(finfo)
                        trip = ifile.readlines()
			for line in trip:
				data = line.split(',')
				trip_duration=data[0].replace('"', '')
				start_lat = data[5].replace('"','')
				start_lon = data[6].replace('"', '')
				end_lat = data[9].replace('"','')
	                        end_lon = data[10].replace('"', '')
				#print(start_lat)
				if not trip_duration == 'tripduration':
					i = i+1
		                        start_point = 'POINT(' + start_lat + ' ' + start_lon + ')'
					end_point = 'POINT(' + end_lat + ' ' + end_lon + ')'
				        #print(start_point)	
					new_trip = Trip(
							id = i,
							trip_duration = trip_duration,
							start_point = start_point,
							end_point = end_point)
					#print(new_trip)
					try:
						#print('Inside try')
						db.session.add(new_trip)
						#print('Added to db')						
						db.session.commit()
						#print('Committed to database')
					except exc.IntegrityError:
						db.session.rollback()
					


def seed_cab_info():
	cab_file = 'nyc_taxi_dataset'
 	with open('./data/' + cab_file,'r') as in_fp:
		for line in in_fp:
			time, start_lat, start_lon, end_lat, end_lon, fare = data_cleaner(chunk)
			start_point = 'POINT(' + start_lat + ' ' + start_lon + ')'
		        end_point = 'POINT(' + end_lat + ' ' + end_lon + ')'
		        #print(start_point)     
        		new_trip = Cab_Trip(id = i,
                			           trip_duration = time,
                                                        start_point = start_point,
                                                        end_point = end_point, fare = fare)
                                        #print(new_trip)
                                        try:
                                                #print('Inside try')
                                                db.session.add(new_trip)
                                                #print('Added to db')                                           
                                                db.session.commit()
                                        except exc.IntegrityError:
                                                db.session.rollback()


def update_station_status():
	"""Updates the database with the status of all of the stations.
			
	https://gbfs.citibikenyc.com/gbfs/en/station_status.json
	
	This function takes approximately 1.5 seconds to run
	"""

	response = requests.get('https://gbfs.citibikenyc.com/gbfs/en/station_status.json')
	response = json.loads(response.text)

	for station in response['data']['stations']:
		try:
			s = db.session.query(Station).filter(Station.id == station['station_id'])

			s.update({Station.num_bikes_available: station['num_bikes_available'],
									Station.num_docks_available: station['num_docks_available']})
			db.session.commit()
		except exc.IntegrityError:
			db.session.rollback()


def consume_data():
	 sc = SparkContext(appName="Lets Go")
	 ssc = StreamingContext(sc, 1)

    	zkQuorum, topic = sys.argv[1:]
    	kvs = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 1})
    	lines = kvs.map(lambda x: x[1])
    	counts = lines.flatMap(lambda line: line.split(" ")) \
        	.map(lambda row: (row, update_station_status())) \
	        .reduceByKey(lambda a, b: a+b)
    

    	ssc.start()
    	ssc.awaitTermination()


def system_alerts():
	"""Get alerts about the system. 

		https://gbfs.citibikenyc.com/gbfs/en/system_alerts.json"""
	pass

"""if __name__ == '__main__':
	update_data()"""
