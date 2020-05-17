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

#import index file of all indicators
FrdDF = pd.read_csv("FredList.csv", engine='python')
FrdDailyDF = pd.read_csv("FredListDailies.csv", engine='python')

#iterate through index file dataframe
for index,row in FrdDF.iterrows():
    #create sql command
    sqlcmd = "SELECT DATE FROM " +row['table'] + " ORDER BY DATE DESC LIMIT 1"
    #get individual values
    getID = row['ID']
    tableID = row['table']

    my_cursor.execute(sqlcmd)
    LastRecord = my_cursor.fetchall()
    LastDate = LastRecord[0][0]
    StartDate = LastDate.date() + timedelta(days=1)
    dataseries = fred.get_series(getID,StartDate)
    table = dataseries.reset_index()
    table.columns = ['Date','Value']
    #this line recreates the table without the top row ([0]). This is necessary for some of these 
    # because the query is inclusive of the immediate previous value and i can't bother to figure out why
    table = table.iloc[1:]
    print(tableID)
    print(table)
    table.to_sql(tableID,engine,if_exists='append')

#Daily Fred indicators don't require iloc statement so separate updater for them here
for index,row in FrdDailyDF.iterrows():
    #create sql command
    sqlcmd = "SELECT DATE FROM " +row['table'] + " ORDER BY DATE DESC LIMIT 1"
    getID = row['ID']
    tableID = row['table']

    my_cursor.execute(sqlcmd)
    LastRecord = my_cursor.fetchall()
    LastDate = LastRecord[0][0]
    StartDate = LastDate.date() + timedelta(days=1)
    dataseries = fred.get_series(getID,StartDate)
    table = dataseries.reset_index()
    table.columns = ['Date','Value']
    print(tableID)
    print(table)
    table.to_sql(tableID,engine,if_exists='append')