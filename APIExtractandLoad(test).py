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
user = parser.get('macrodashboard','user')
passwd = parser.get('macrodashboard','passwd')
database = parser.get('macrodashboard','database')

engine = parser.get('engines','macrodbengine')

qlkey = parser.get('keys','qlkey')

#config Quanld API and pull a table, print statement verifies pull
ql.ApiConfig.api_key = qlkey

#GBPUSDRaw = ql.get("BOE/XUDLUSS", start_date="2000-01-01", paginate=True)

#print(GBPUSDRaw.head())


#connect to local sql server, print statement tests connection (using mysql_connector)
gbpusd = mysql.connector.connect(
    host = host,
    user = user,
    passwd = passwd,
    database = database,
)

#connect to db using sqlalchemy
engine = create_engine(engine)

#push dataframe to sql table (creates table)
#GBPUSDRaw.to_sql("spotprices",engine)

#create sql.connector cursor, often called "self"
my_cursor = gbpusd.cursor()
#call last record of sql table - notice it outputs as list in list even when select specific column, ie "Date"
my_cursor.execute("SELECT DATE FROM spotprices ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
#print(LastRecord)

#extract date from list in list
LastDate = LastRecord[0][0]
print(LastDate)

#Drop the time using .date() and add 1 day to get new start date
#StartDate = LastDate.date() + timedelta(days=1)
#print(StartDate)

#pull new values going forward from start date
#GBPUSDUpdate = ql.get("BOE/XUDLUSS", start_date=StartDate, paginate=True)

#print(GBPUSDUpdate)

#append into existing sql table
#GBPUSDUpdate.to_sql("spotprices",engine,if_exists='append')

