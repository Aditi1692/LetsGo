# home, work = db.session.query(User.home, User.work).filter_by(User.id==user_id)
# 

from jinja2 import StrictUndefined
from flask import Flask, render_template, jsonify, request, redirect, flash, session
from flask_debugtoolbar import DebugToolbarExtension
import bcrypt
import re
from geoalchemy2 import func
from sqlalchemy import exc


from model import connect_to_db, db, User, Station, Bike_Trip, Cab_Trip
from get_info import seed_bike_station_information, update_bike_station_status, system_alerts

app = Flask(__name__)
app.secret_key = "ursusmaritimus"
app.jinja_env.undefined = StrictUndefined


#---------------------------------------------------------------------#
# Helper Functions
#---------------------------------------------------------------------#
def get_biketrip_by_location(start_location, end_location):
	return Bike_Trip.query.filter(Bike_Trip.start_point==start_location, Bike_Trip.end_point==end_location).first()

def get_cabtrip_by_location(start_location, end_location):
	return Cab_Trip.query.filter(Cab_Trip.start_point==start_location, Cab_Trip.end_point==end_location).first()

def find_bikepath(start_station, end_station):
	""" This function takes the source station and destination station and
	forms the path connecting them."""
	i = 0
	j = 0
	while start_station[i].num_bikes_available == 0:
		i=i+1
	while end_station[j].num_docks_available == 0:
		j=j+1 
	bike_trip = get_biketrip_by_location(start_station[i], end_station[j])
        trip_duration = bike_trip.trip_duration
	cost = trip_duration/60 * 9.95
	return trip_duration, cost		


def find_cabpath(start_station, end_station):
	trip_duration, fare = Cab_Trip.query(Cab_Trip.trip_duration, Cab_Trip.fare).filter(Cab_Trip.start_point==start_location, Cab_Trip.end_point==end_location).first()


def create_new_user(user_info):
	"""This function takes the request.form objcet passed to the register route and
	parses it out to create a new user in the database."""

	home = 'POINT(' + user_info['homeLngLat'] + ')'
	work = 'POINT(' + user_info['workLngLat'] + ')'

	new_user = User(username = user_info['username'],
					password = bcrypt.hashpw(user_info['password'].encode('utf-8'), bcrypt.gensalt()),
					home_address = user_info['home'],
					work_address = user_info['work'],
					home_point = home,
					work_point = work)
	try:
		db.session.add(new_user)
		db.session.commit()
	except exc.IntegrityError:
		db.session.rollback()

def update_existing_user(user_info):
	"""This function takes the request.form object passed to the update route and
	parses it out to update the user settings"""

	user = get_user_by_id(session['user_id'])

	if user_info['homeLngLat']:
		user.home_address = user_info['home']
		user.home_point = 'POINT(' + user_info['homeLngLat'] + ')'

	if user_info['workLngLat']:
		user.work_address = user_info['work']
		user.work_point = 'POINT(' + user_info['workLngLat'] + ')'

	try:
		db.session.add(user)
		db.session.commit()
	except exc.IntegrityError:
		db.session.rollback()

def get_user_by_id(id):
	"""Takes user id and returns user object"""
	return User.query.filter(User.id==id).first()

def get_user_by_username(username):
    """Takes username and returns user object, else returns None """
    return User.query.filter(User.username==username).first()
    
def login_attempt_sucessful(username, password):
	"""Checks to see if the username/password combination is valid. 
	If the combination is valid the user object is returned. The user must exist
	and have entered the correct password else the function returns false.
	"""
	user = get_user_by_username(username)
	if user and bcrypt.hashpw(password, hashed) == hashed:
		return user
	else:
		return False 

def get_closest_stations(location):
	"""Given a location (home, work, or the user location), return the top 5
	closest stations to that point"""

	query = db.session.query(Station).order_by(func.ST_Distance(Station.point, 
		location)).limit(5)
	
	return query.all()

#---------------------------------------------------------------------#
# Routes
#---------------------------------------------------------------------#

@app.before_request
def before_request():
    # When you import jinja2 macros, they get cached which is annoying for local
    # development, so wipe the cache every request.
    session['active'] = True
    session['user_id'] = 1
    app.jinja_env.cache = {}

@app.route('/')
def index():
    """Show main map view. """
    if session['active']:
    	user = get_user_by_id(session['user_id'])
	#print(user.home_point)
    	home = get_closest_stations(user.home_point)
	#citi_home_point = get_trip_by_location(home, work)
    	work = get_closest_stations(user.work_point)
        bike_trip_duration, bike_cost = find_bikepath(home, work)
	cab_trip_duration, cab_cost = find_cabpath(home[0], work[0])
    	return render_template('test.html', user=user, home=home, work=work, bike_trip_duration=bike_trip_duration, bike_cost=bike_cost, cab_trip_duration=cab_trip_duration, cab_cost=cab_cost)

    return render_template('map_view.html')

@app.route('/login', methods=['GET', 'POST'])
def login():
	"""Render Login Form and handle login form submissions. """
	if request.method == 'GET':
		return render_template('login.html')
	else:
		username = request.form.get('username')
    	password = request.form.get('password')

    	logged_in_user = login_attempt_sucessful(username, password)

    	if logged_in_user:
    		session['active'] = True
    		session['user_id'] = user.id
    		return redirect('/')
    	else:
    		flash('Incorrect username or password')
    		return redirect('/login')


@app.route('/register', methods=['GET', 'POST'])
def register():
	"""Render the registration form and handle regestration events. """
	if request.method == 'GET':
		return render_template('register.html')
	else:
		username = request.form.get('username')
		print request.form
    	if get_user_by_username(username):
    		flash('Username Taken')
    		return redirect('/register')
    	else:
    		create_new_user(request.form)
    		print "new user created"
    		return redirect('/')

@app.route('/update', methods=['GET', 'POST'])
def update():
	"""Render the update form and handle changes to user info"""
	if request.method == 'GET':
		return render_template('update.html')
	else:
		update_existing_user(request.form)
		return redirect('/')
	# with a message saying preferences updated

@app.route('/test')
def test():
	return render_template('css_tests.html')


@app.route('/seed')
def seed_and_update():
	"""seed and update the database - temporart solution"""
	update_bike_station_status()
	print 'Stations updated'

	return '<h1>DB Seeded</h1>'


#---------------------------------------------------------------------#
# JSON Routes
#---------------------------------------------------------------------#

@app.route('/user-location.JSON')
def send_user_location_availability(location):
	"""Gets location from the user and finds closest stations and availability"""
	pass


if __name__ == "__main__":
    app.debug = True
    connect_to_db(app, 'postgres://aditi:gisdata@localhost:5432/bike_test')
    DebugToolbarExtension(app)
    app.config['DEBUG_TB_INTERCEPT_REDIRECTS'] = False
    app.config['TEMPLATES_AUTO_RELOAD'] = True

    app.run(port=5000, host="0.0.0.0")
