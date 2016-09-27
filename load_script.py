__author__ = 'Praneetha'

# This program uploads a CSV file to a MySQL database
# The file being used contains information concerning the Zika virus
# and was obtained via kaggle.com
# The current program uploads the data to a temporary table, but
# this program was used to upload data to a non-temporary table in the database

import MySQLdb as mysql
import os

path = (os.getcwd() + os.sep + 'zika.csv').replace(os.sep, '/')
print "The csv file is located in: " + path

db = mysql.connect(host='localhost', user='root', db="praneetha_example")
print "Connected to database"

cur = db.cursor()
print "Loading CSV into table from database \n"

# Creates temporary table, ZikaChart, for the CSV file
cur.execute("""CREATE TEMPORARY TABLE ZikaChart(
report_date DATE
, location VARCHAR(70)
, location_type VARCHAR(20)
, data_field VARCHAR(70)
, data_field_code CHAR(6)
, time_period CHAR(2)
, time_period_type CHAR(2)
, value MEDIUMINT SIGNED DEFAULT NULL
, unit VARCHAR(15));""")

# Loads data from CSV file into table
cur.execute("""LOAD DATA INFILE '"""+path+"""'
INTO TABLE ZikaChart
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS
(report_date, location, location_type
, data_field, data_field_code, time_period
, time_period_type, value, unit);""")

# Cursor selects the first 5 rows from the ZikaChart
cur.execute("SELECT * FROM ZikaChart LIMIT 5;")

print 'First 5 rows:'
# Prints the rows selected by the cursor
for c in cur:
    print c

print "\nSuccessfully loaded CSV into table of database"

cur.close()

