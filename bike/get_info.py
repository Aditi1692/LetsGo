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
from model import db, Station
from sqlalchemy import exc

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
		except exc.IntegrityError:
			db.session.rollback()

def update_data():
	"Get all the details of batch processed data"

	session = boto3.session.Session(
        aws_access_key_id="AKIAJ2DTCQ33IONOME4Q",
        aws_secret_access_key="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"
        )

        s3 = session.resource("s3")
        bucket = s3.Bucket('citibikenycdataset')
        obj = bucket.Object('201307-201402-citibike-tripdata.zip')

        with io.BytesIO(obj.get()["Body"].read()) as tf:

                # rewind the file
                tf.seek(0)

                # Read the file as a zipfile and process the members
                zfile =  zipfile.ZipFile(tf, mode='r')
                for finfo in zfile.infolist():
                        ifile = zfile.open(finfo)
                        trip = ifile.readlines()
                        start_point = 'POINT(' + str(trip[]) + ' ' + str(trips['start_lat']) + ')'
			end_point = 'POINT(' + str(trips['stop_lon']) + ' ' + str(trips['stop_lat']) + ')'

			new_trip = Trip(
						trip_duration = trip[],
						start_point = start_point,
						end_point = end_point)

			try:
				db.session.add(new_trip)
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

def system_alerts():
	"""Get alerts about the system. 

		https://gbfs.citibikenyc.com/gbfs/en/system_alerts.json"""
	pass


