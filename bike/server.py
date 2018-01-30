# home, work = db.session.query(User.home, User.work).filter_by(User.id==user_id)
# 

from jinja2 import StrictUndefined
from flask import Flask, render_template, jsonify, request, redirect, flash, session
from flask_debugtoolbar import DebugToolbarExtension
import bcrypt
import re
from geoalchemy2 import func
from sqlalchemy import exc


from model import connect_to_db, db, User, Station
from get_info import seed_station_information, update_station_status, system_alerts

app = Flask(__name__)
app.secret_key = "ursusmaritimus"
app.jinja_env.undefined = StrictUndefined


#---------------------------------------------------------------------#
# Helper Functions
#---------------------------------------------------------------------#
def find_path(g, start_station, end_station):
	""" This function takes the source station and destination station and
	forms the path connecting them."""
	if(start_station == end_station):
		return
	possible_stations = db.session.query(end_point, trip_duration).filter_by(Trips.start_point==start_station)
	for station in possible_stations:
		g.addedge(start_station, station, trip_duration)
		find_path(station, end_station)
	

def get_distance(g):
	"""Algorithm: Dijkstra algorithm"""
	

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
		location)).limit(10)

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
	print(user.home_point)
    	home = get_closest_stations(user.home_point)
    	work = get_closest_stations(user.work_point)

    	return render_template('test.html', user=user, home=home, work=work)

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
	update_station_status()
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
