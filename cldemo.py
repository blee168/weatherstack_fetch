"""
EXAMPLE USAGE
=============

For example, if you want to get London's weather every 10 minutes for an hour:
    $ python3 cldemo.py -hn <HOSTNAME> -l London -d 600 -i 60 -k <API_KEY>
        -u <USERNAME> -p <PASSWORD> -n <DATABASENAME>
"""

import argparse
import mysql.connector
from fetcher import Fetcher

parser = argparse.ArgumentParser()
parser.add_argument('-hn', '--host_name', type=str, default='localhost',
                    help="MySQL host name of server.")
parser.add_argument('-u', '--username', type=str, default='root',
                    help="MySQL username.")
parser.add_argument('-p', '--password', type=str, default="password",
                    help="MySQL connection password")
parser.add_argument('-n', '--database_name', type=str, default="db",
                    help="MySQL database name")
parser.add_argument('-l', '--location', type=str, default='Singapore',
                    help="Location. See WeatherstackAPI for valid locations")
parser.add_argument('-d', '--delay', type=int, default = 10,
                    help="How often (in minutes) do you want to fetch?")
parser.add_argument('-i', '--interval', type=float, default = 1,
                    help="Over how many minutes do you want to fetch?")
parser.add_argument('-k', '--api_key', type=str,
                    help="WeatherstackAPI key goes here.")
parser.add_argument('-t', '--table_name', type=str,
                    help="Table name")

args = parser.parse_args()

mydb = mysql.connector.connect(
    host=args.host_name,
    user=args.username,
    passwd=args.password,
    database=args.database_name
)

if args.api_key == None:
    print("Please pass in a valid API key!")
else:
    fetcher = Fetcher(mydb, args.api_key, args.location, args.table_name)
    fetcher.periodic_load(args.delay, args.interval)
