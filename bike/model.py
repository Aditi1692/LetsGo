"""Data model for go-bike-go.

V1: Two tables: User and Station. 
Note: Station primary_key is an ID provided directly from citibike
Class names are singular - table names are plural"""

from flask_sqlalchemy import SQLAlchemy
from geoalchemy2 import Geography, func
from shapely.wkb import loads

db = SQLAlchemy()


class User(db.Model):
    """Basic information about the user including home and work locations."""

    __tablename__ = 'users'

    id = db.Column(db.Integer,
                   nullable=False,
                   autoincrement=True,
                   primary_key=True)
    username = db.Column(db.String(50), nullable=False, unique=True)
    password = db.Column(db.String(100), nullable=False)
    home_address = db.Column(db.String(150), nullable=False)
    work_address = db.Column(db.String(150), nullable=False)
    home_point = db.Column(Geography(geometry_type='POINT', srid=4326))
    work_point = db.Column(Geography(geometry_type='POINT', srid=4326))

    def __repr__(self):
        return '<User id:{id} username:{username}>'.format(username=self.username,
                                                           id=self.id)


class Station(db.Model):
    """Stores information about all available bike stations. Stores bike
                availability information"""

    __tablename__ = 'stations'

    id = db.Column(db.Integer,
                   nullable=False,
                   autoincrement=False,
                   primary_key=True)
    name = db.Column(db.String(50), nullable=False)
    point = db.Column(Geography(geometry_type='POINT', srid=4326), nullable=False)
    num_bikes_available = db.Column(db.Integer, nullable=False)
    num_docks_available = db.Column(db.Integer, nullable=False)

    def lat(self):
        """Return the latitude of the station"""
        coordinates = loads(bytes(self.point.data))
        return coordinates.y

    def lng(self):
        """Return the longitude of the station"""
        coordinates = loads(bytes(self.point.data))
        return coordinates.x

    def __repr__(self):
        return '<Station id:{id}, Bike:Dock {bike}:{dock}>'.format(id=self.id, 
            bike=self.num_bikes_available, dock=self.num_docks_available)

##########################################################################

class Trip(db.Model):
	"""Stores information about start station to end station with their trip
		duration."""

	__tablename__ = 'trips'

	id = db.Column(db.Integer, 
			nullable=False,
			autoincrement=False,
			primary_key=True)
	trip_duration = db.Column(db.Integer, nullable=False)
	start_point = db.Column(Geography(geometry_type='POINT', srid=4326), nullable=False)
	end_point = db.Column(Geography(geometry_type='POINT', srid=4326), nullable=False)

	def start_lat(self):
		coordinates = load(bytes(self.start_point.data))
		return coordinates.y
	
	def start_lng(self):
                coordinates = load(bytes(self.start_point.data))
                return coordinates.x

	def stop_lat(self):
                coordinates = load(bytes(self.stop_point.data))
                return coordinates.y

	def stop_lng(self):
                coordinates = load(bytes(self.stop_point.data))
                return coordinates.x


	def __repr__(self):
		return '<Trip duration:{trip_duration}>'.format(trip_duration=self.trip_duration)

def example_data():
    """Add some example stations and a user to the db"""

    spot1 = "POINT(40.71911552 -74.00666661)"
    spot2 = "POINT(40.71117416 -74.00016545)"
    spot3 = "POINT(40.73172428 -74.00674436)"
    station1 = Station(id=79, name="Franklin St & W Broadway", point=spot1,
        num_bikes_available=0, num_docks_available=0)
    station2 = Station(id=82, name="St James Pl & Pearl St", point=spot2,
        num_bikes_available=0, num_docks_available=0)
    station3 = Station(id=127, name="Barrow St & Hudson St", point=spot3,
        num_bikes_available=0, num_docks_available=0)

    db.session.add_all([station1, station2, station3])
    db.session.commit()

##########################################################################


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

if __name__ == '__main__':

    init_app()
    db.create_all()
    print 'created all'
