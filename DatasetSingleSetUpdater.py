import pandas_datareader as pdr
import quandl as ql
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
username = parser.get('macrodashboard','user')
passwd = parser.get('macrodashboard','passwd')
database = parser.get('macrodashboard','database')

engine = parser.get('engines','macrodbengine')

qlkey = parser.get('keys','qlkey')

#connect to specific db w/ both mysql connector and sqlalchemy. sqlalchemy for pushing and mysql for pulling
mydb = mysql.connector.connect(
    host = host,
    user = username,
    passwd = passwd,
    database = database,
)

#connect to db using sqlalchemy
engine = create_engine(engine)

#quandl API Key
ql.ApiConfig.api_key = qlkey

#for documentation for below see datasetupdater.py
my_cursor = mydb.cursor()
my_cursor.execute("SELECT DATE FROM boebankrate ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
RawUpdateTable = ql.get("BOE/IUDBEDR", start_date=StartDate, paginate=True)
RawUpdateTable.to_sql("boebankrate",engine,if_exists='append')
print(RawUpdateTable)