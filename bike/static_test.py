import unittest
from geopy.geocoders import Nominatim
from server import app
from model import connect_to_db, db, Station, User, example_data, Trip

import server
import get_info

class TestHelperFunctions(unittest.TestCase):
	def setUp(self):
		self.client = app.test_client()
                app.config['TESTING'] = True
                connect_to_db(app, 'postgresql://aditi:gisdata@localhost:5432/bike_test')
                db.create_all()
                #example_data()

	"""def tearDown(self):
		db.session.close()
		db.drop_all()"""

	def test_create_new_user(self):
		"""Add new user to db, hash pw
		Create new user takes the immutable dict request.from
		and creates a new user record."""

		"""data = {'username': u'Test_User',
					'password': u'test',
					'work': u'88 7th Avenue, New York, NY, United States',
					'home': u'152 Lexington Avenue, New York, NY, United States',
					'homeLngLat': u'-73.98199699999998 40.743772',
					'workLngLat': u'-74.0014936 40.7396046'}"""

		# data = {'Stanton St & Chrystie St'
		data = {'username': u'Test_User',
                                        'password': u'test',
                                        'home': u'NYU Silver Center for Arts and Science, 100 Washington Square E, New York, NY United States',
                                        'work': u'35 Stanton St, New York, NY, United States',
                                        'workLngLat': u'40.722473 -73.991673',
                                        'homeLngLat': u'40.730348 -73.995595'}

		# Add Test_User to the database
		server.create_new_user(data)

		new_user = db.session.query(User).filter(User.username=='Test_User').one()
		print('added new user: Test_User')
		# new_user would return none if it did not exist in the db
		self.assertTrue(new_user, 'Test_User was not sucessfully added to db.')
		self.assertNotEqual(new_user.password, 'password', 'Password likely not hashed before stored in db.')

	def test_seed_station_information(self):
		"""Seed stations and initialize counts to 0
		Fetches the station information from the Citibike API
		and adds stations to the database with bike/dock values of 0"""
		#get_info.seed_station_information()
		#get_info.update_data()
		
		"""MacDougal_Prince = db.session.query(Station).filter(Station.id == 128).one()
		self.assertTrue(MacDougal_Prince, 'Station at MacDogual/Pride did not get sucessfully added.')

		self.assertEqual(MacDougal_Prince.num_bikes_available, 0, 'Bike counts were not initialized properly')
		self.assertEqual(MacDougal_Prince.num_docks_available, 0, 'Dock counts were not initialized properly')
		print('station information')"""

	def test_get_user_by_username(self):
		username_in_db = server.get_user_by_username('Test_User')
		geolocator = Nominatim()
		start_location = geolocator.geocode(username_in_db.home_address)
		home_point = 'POINT(' + str(start_location.latitude) + ' ' + str(start_location.longitude) + ')'	
		home_stations = server.get_closest_stations(home_point)
		end_location = geolocator.geocode(username_in_db.work_address)
		print('home', home_stations[0].name)
		work_point = 'POINT(' + str(end_location.latitude) + ' ' + str(end_location.longitude) + ')' 
		work_stations = server.get_closest_stations(work_point)
		print('work', work_stations[0].point)

		"""home_point = "POINT(40.73049393 -73.9957214)"
		work_point = "POINT(40.72229346 -73.99147535)"
		new_trip = server.get_trip_by_location(home_point, work_point)"""
		new_trip = server.get_trip_by_location(home_stations[0].point, work_stations[0].point)
		print('trip_duration: ', new_trip.trip_duration)
		self.assertTrue(username_in_db, 'Query did not fetch user object.')
		username_not_in_db = server.get_user_by_username('xyz')
		self.assertFalse(username_not_in_db, 'Query fetched user that did not exist (xyz).')
		print('get user by username')

	"""def test_get_closest_stations(self):
		point = "POINT(40.71911552 -74.00666661)"
		stations = set(server.get_closest_stations(point))
		# find the closest stations, make them a set of objects see if sets intersect completely
		print(stations)
		print('get closest station')"""


if __name__ == "__main__":
	print('Unit test has started')
	unittest.main()

