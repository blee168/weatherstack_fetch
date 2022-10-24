import argparse
import datetime
import mysql.connector
import requests
import threading
import time

class Fetcher:

    """Class extracting weatherstackAPI data to load into a local MySQL database.
    """

    def __init__(self, db, api_key, location = 'Singapore'):
        """
        INPUTS:
            db: mysql.connector database object
            api_key: string for accessing weatherstackAPI
            location: string for determining where to check data. see
                weatherstackAPI for valid locations
        """
        self.db = db
        self.api_key = api_key
        self.location = location
        self.cursor = db.cursor()

    def sql_formula(self, args):
        """Clean up the list of arguments to be passed into the MySQL query."""

        argstring = "CAST('" + args[0] + "' AS DateTime),"
        for arg in args[1:-1]:
            if type(arg) == str:
                if arg == "":
                    argstring += "NULL"
                else:
                    arg = "'" + arg + "'"
            if type(arg) == list:
                if len(arg) == 1:
                    argstring += "'" + arg[0] + "',"
                else:
                    argstring += "'"
                    for x in args[:-1]:
                        argstring += x + ","
                    argstring += args[-1] + "'"
            else:
                argstring += str(arg) + ","

        argstring += str(args[-1])

        return "INSERT INTO weather (local_time,timezone_id,lon,lat,region,country,name,is_day,visibility,uv_index,feelslike,cloudcover,humidity,precip,pressure,wind_dir,wind_degree,wind_speed,weather_descriptions,weather_code,temperature) VALUES(" + argstring + ");"

    def single_request(self):
        """Returns a JSON of weather data at a location. See weatherstack API"""

        self.r = requests.get('http://api.weatherstack.com/current?access_key='+self.api_key+'&query='+self.location).json()

    def load(self):
        """After fetching weather data, drop some features and combine for
        passing into MySQL query. Then uploads to given database using
        given cursor."""

        self.single_request()
        data = self.r['current']
        data.update(self.r['location'])
        data.pop('weather_icons')
        data.pop('observation_time')
        data.pop('utc_offset')
        data.pop('localtime_epoch')
        self.cursor.execute(self.sql_formula(list(data.values())[::-1]))
        self.db.commit()

    def periodic_load(self, delay = 10, interval = 1):
        """Periodically fetches data from weatherstack API and
        uploads to a MySQL database.
        Delay in seconds, total interval of collection in minutes."""

        db = self.db
        cursor = self.cursor
        api_key = self.api_key
        location = self.location

        next_call = time.time()
        n = int(interval * 60 / delay)
        for i in range(n + 1):
            print(datetime.datetime.now())
            self.load()
            next_call = next_call + delay
            time.sleep(next_call - time.time())
