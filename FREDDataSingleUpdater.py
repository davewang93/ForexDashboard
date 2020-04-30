from fredapi import Fred
import pandas_datareader as pdr
import pandas as pd
import numpy as np
import sqlalchemy
from sqlalchemy import create_engine
import mysql.connector
from datetime import datetime, timedelta
from configparser import ConfigParser 

parser = ConfigParser()
parser.read('config.ini')

host = parser.get('macrodashboard','host')
user = parser.get('macrodashboard','user')
passwd = parser.get('macrodashboard','passwd')
database = parser.get('macrodashboard','database')

engine = parser.get('engines','macrodbengine')

frdkey = parser.get('keys','frdkey')

#connect to specific db w/ both mysql connector and sqlalchemy. sqlalchemy for pushing and mysql for pulling
mydb = mysql.connector.connect(
    host = host,
    user = user,
    passwd = passwd,
    database = database,
)

#connect to db using sqlalchemy
engine = create_engine(engine)

fred = Fred(api_key=frdkey)

#create sql.connector cursor, often called "self"
my_cursor = mydb.cursor()

#for full doc see DatasetUpdater.py

my_cursor.execute("SELECT DATE FROM 10yrtreasbenchmark ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
dataseries = fred.get_series('DGS10',StartDate,)
table = dataseries.reset_index()
table.columns = ['Date','Value']
#table = table.iloc[1:]
print(table)
#table.to_sql("10yrtreasbenchmark",engine,if_exists='append')