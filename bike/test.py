import unittest

from server import app
from model import connect_to_db, db, Station, User, example_data

import server
import get_info

class TestHelperFunctions(unittest.TestCase):
	"""Tests helper functions that do not interface
	with the database"""

class TestHelperFunctionsDB_Read():
	"""Tests helper functions that read from the database.
	Database for these tests is seeded with example_data()
	get_user_by_username()
	get_closest_stations()"""

	def setUp(self):
		self.client = app.test_client()
		app.config['TESTING'] = True
		connect_to_db(app, 'postgresql://aditi:gisdata@localhost:5432/bike_test')
		db.create_all()
		example_data()
		print('read setup done')

	def tearDown(self):
		db.session.close()
		db.drop_all()
		print('tearDown read')

	def test_get_user_by_username(self):
		"""Retrieve user object from db by username"""
		username_in_db = server.get_user_by_username('Test_User')
		self.assertTrue(username_in_db, 'Query did not fetch user object.')
		username_not_in_db = server.get_user_by_username('xyz')
		self.assertFalse(username_not_in_db, 'Query fetched user that did not exist (xyz).')
		

	def test_get_closest_stations(self):
		"""Get the 5 closest station opbject to given point and WKT point."""
		point = "POINT(40.71911552 -74.00666661)"
		stations = set(server.get_closest_stations(point))
		# find the closest stations, make them a set of objects see if sets intersect completely


class TestHelperFunctionsDB_Write(unittest.TestCase):
	"""Tests helper functions that write to the database
	create_new_user()
	seed_station_data()
	update_station_status()
	"""

	def setUp(self):
		self.client = app.test_client()
		app.config['TESTING'] = True
		connect_to_db(app, 'postgresql://aditi:gisdata@localhost:5432/bike_test')
		db.create_all()
		print('inside write setup')
		example_data()


	def tearDown(self):
		db.session.close()
		db.drop_all()
		print('tearDown write')

	def test_create_new_user(self):
		"""Add new user to db, hash pw
		Create new user takes the immutable dict request.from
		and creates a new user record."""

		data = {'username': u'Test_User',
					'password': u'test',
					'work': u'88 7th Avenue, New York, NY, United States',
					'home': u'152 Lexington Avenue, New York, NY, United States',
					'homeLngLat': u'-73.98199699999998 40.743772',
					'workLngLat': u'-74.0014936 40.7396046'}

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
		get_info.seed_station_information()
		get_info.update_data()
		MacDougal_Prince = db.session.query(Station).filter(Station.id == 128).one()
		self.assertTrue(MacDougal_Prince, 'Station at MacDogual/Pride did not get sucessfully added.')

		self.assertEqual(MacDougal_Prince.num_bikes_available, 0, 'Bike counts were not initialized properly')
		self.assertEqual(MacDougal_Prince.num_docks_available, 0, 'Dock counts were not initialized properly')
		print('station information')

	def test_update_station_status(self):
		"""Update bike/dock counts in db
		Fetches station status information from Citibike API
		and updates the num bikes/docks available in the db"""
		# Seed the db and initialize all counts to 0
		get_info.seed_station_information()

		# Save number of bikes/docks before update
		E40th_5thave = db.session.query(Station).filter(Station.id == 72).one()
		bikes_before = E40th_5thave.num_bikes_available
		docks_before = E40th_5thave.num_docks_available

		# Update bike/dock numbers
		get_info.update_station_status()

		E40th_5thave = db.session.query(Station).filter(Station.id == 72).one()
		bikes_after = E40th_5thave.num_bikes_available
		docks_after = E40th_5thave.num_docks_available

		self.assertNotEqual(bikes_before + docks_before, bikes_after + docks_after, 'Bikes did not update, or station is disabled.')
		print('update station')

	def test_get_user_by_username(self):
		"""Retrieve user object from db by username"""
		username_in_db = server.get_user_by_username('Test_User')
		self.assertTrue(username_in_db, 'Query did not fetch user object.')
		username_not_in_db = server.get_user_by_username('xyz')
		self.assertFalse(username_not_in_db, 'Query fetched user that did not exist (xyz).')
		print('get user by username')

	def test_get_closest_stations(self):
		"""Get the 5 closest station opbject to given point and WKT point."""
		point = "POINT(40.71911552 -74.00666661)"
		stations = set(server.get_closest_stations(point))
		# find the closest stations, make them a set of objects see if sets intersect completely
		print('get closest station')



class TestRoutes(unittest.TestCase):
	"""End to end testing of all routes """

	#def setUp(self):
		#pass

	#def tearDown(self):
		#pass

	#def test_login_get(self):
		#pass

	#def test_login_post(self):
		#pass

	#def test_register_get(self):
		#pass

	#def test_register_post(self):
		#pass

		# response = self.client.post('/register',
		# 	data = {'username': u'Test_User',
		# 			'password': u'test',
		# 			'work': u'88 7th Avenue, New York, NY, United States'
		# 			'home': u'152 Lexington Avenue, New York, NY, United States',
		# 			'homeLngLat', u'-73.98199699999998 40.743772',
		# 			'workLngLat', u'-73.98199699999998 40.743772'
		# 	}, follow_redirects=True)


if __name__ == "__main__":
    print('Unit test has started')
    unittest.main()
