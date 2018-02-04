"""This script will run every 90 seconds in order to update the bike/dock 
counts"""

import time
# import timeit
from get_info import update_station_status
from model import db, init_app, connect_to_db

def init_app():
    """Creating Flask app in order to run SQLAlchemy"""

    from flask import Flask
    app = Flask(__name__)

    # this file connects to a test db
    connect_to_db(app, 'postgres://aditi:gisdata@localhost:5432/bike_test')
    print "Connected to DB."


def connect_to_db(app, database_URI):
    """Connect the database to Flask app."""

    app.config['SQLALCHEMY_DATABASE_URI'] = database_URI
    app.config['SQLALCHEMY_ECHO'] = False
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    db.app = app
    db.init_app(app)

if __name__=='__main__':
	init_app()
	print 'initialized'
	while True:
		update_station_status()
		# print timeit.timeit(stmt='update_station_status()', 
		# setup="from __main__ import update_station_status", 
		# number=1)
		time.sleep(60)
