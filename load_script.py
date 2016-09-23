__author__ = 'Praneetha'

# The csv file concerning the Zika virus was obtained from kaggle.com

import MySQLdb as mysql
import os

path = (os.getcwd() + os.sep + 'zika.csv').replace(os.sep, '/')
print "The csv file is located in: " + path

db = mysql.connect(host='localhost', user='root', db="praneetha_example")

cur = db.cursor()
cur.execute("""CREATE TEMPORARY TABLE ZikaChart(
report_date DATE
, location VARCHAR(30)
, location_type VARCHAR(20)
, data_field VARCHAR(40)
, data_field_code CHAR(6)
, time_period CHAR(2)
, time_period_type CHAR(2)
, value MEDIUMINT UNSIGNED DEFAULT 0
, unit VARCHAR(15));""")

cur.execute("""LOAD DATA INFILE '"""+path+"""'
INTO TABLE ZikaChart
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS
(report_date, location, location_type
, data_field, data_field_code, time_period
, time_period_type, value, unit);""")

cur.execute("SELECT * FROM ZikaChart WHERE location = 'Brazil-Piaui' AND data_field = 'microcephaly_fatal_not';")
for c in cur:
    print c, '\n'

cur.close()

