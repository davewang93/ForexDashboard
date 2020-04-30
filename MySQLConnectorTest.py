import mysql.connector
from configparser import ConfigParser 

parser = ConfigParser()
parser.read('config.ini')

host = parser.get('macrodashboard','host')
user = parser.get('macrodashboard','user')
passwd = parser.get('macrodashboard','passwd')
database = parser.get('macrodashboard','database')


gbpusd = mysql.connector.connect(
    host = host,
    user = user,
    passwd = passwd,
    database = database
)

#create cursor
cursor = gbpusd.cursor()

#create a db
#cursor.execute("CREATE DATABASE gbpusd")
'''cursor.execute("SHOW DATABASES")
for db in cursor:
    print(db[0])'''

#create a table
#cursor.execute("CREATE TABLE SpotPrices(Date date, Price float(10) )")

#delete table
cursor.execute("DROP TABLE spotprices")

