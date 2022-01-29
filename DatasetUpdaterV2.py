import pandas_datareader as pdr
import quandl as ql
import pandas as pd
import numpy as np
import sqlalchemy
from sqlalchemy import create_engine
import mysql.connector
from datetime import datetime, timedelta
from configparser import ConfigParser 
import os

directory = os.path.dirname(os.path.abspath(__file__))
configfile = os.path.join(directory, 'config.ini')
parser = ConfigParser()
parser.read(configfile)

host = parser.get('macrodashboard','host')
user = parser.get('macrodashboard','user')
passwd = parser.get('macrodashboard','passwd')
database = parser.get('macrodashboard','database')

engine = parser.get('engines','macrodbengine')

qlkey = parser.get('keys','qlkey')

#connect to specific db w/ both mysql connector and sqlalchemy. sqlalchemy for pushing and mysql for pulling
mydb = mysql.connector.connect(
    host = host,
    user = user, 
    passwd = passwd,
    database = database,
)

#connect to db using sqlalchemy
engine = create_engine(engine)

#quandl API Key
ql.ApiConfig.api_key = qlkey

#create sql.connector cursor, often called "self"
my_cursor = mydb.cursor()

QuandlList = os.path.join(directory, 'QuandlList.csv')
QuandlDF = pd.read_csv(QuandlList, engine='python')

CheckDate = QuandlDF


for index,row in QuandlDF.iterrows():
    #create sql command
    sqlcmd = "SELECT DATE FROM " +row['table'] + " ORDER BY DATE DESC LIMIT 1"
    getID = row['ID']
    tableID = row['table']

    my_cursor.execute(sqlcmd)
    LastRecord = my_cursor.fetchall()
    LastDate = LastRecord[0][0]
    StartDate = LastDate.date() + timedelta(days=1)
    RawUpdateTable = ql.get(getID, start_date=StartDate, paginate=True)
    RawUpdateTable.to_sql(tableID,engine,if_exists='append')
    print(tableID)
    print(RawUpdateTable)

#separate code snippet for AU Government yields
sqlcmd = "SELECT DATE FROM ausgovtyield ORDER BY DATE DESC LIMIT 1"
getID = 'RBA/F02'
tableID = 'ausgovtyield'
my_cursor.execute(sqlcmd)
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
RawUpdateTable = ql.get(getID, start_date=StartDate, paginate=True)
RawUpdateTable.columns = ['2yr','3yr','5yr','10yr','idx','nsw1','nsw2','nsw3']
RawUpdateTable.drop(['idx','nsw1','nsw2','nsw3'], axis=1, inplace=True)
RawUpdateTable.to_sql(tableID,engine,if_exists='append')
print(tableID)
print(RawUpdateTable)