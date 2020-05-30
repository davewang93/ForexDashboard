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

#connect to specific db w/ both mysql connector and sqlalchemy. sqlalchemy for pushing and mysql for pulling
mydb = mysql.connector.connect(
    host = host,
    user = user,
    passwd = passwd,
    database = database,
)

#connect to db using sqlalchemy
engine = create_engine(engine)

#create sql.connector cursor, often called "self"
my_cursor = mydb.cursor()

CheckDate = pd.read_csv("MasterList.csv", engine='python')

'''
Purpose of this script is to grab last 2 dates in SQL tables and export them to CSV so I can quickly compare against the last value I have in my macro dashboard. 
(Some of the tables in dashboard don't auto update to latest date which is a pain) 
Grabbing the "Previous Date" value also allows me to quick check to make sure the API pulls are not skipping data
'''
for index,row in CheckDate.iterrows():
    sqlcmd = "SELECT DATE FROM " +row['table'] + " ORDER BY DATE DESC LIMIT 2"
    my_cursor.execute(sqlcmd)
    LastRecord = my_cursor.fetchall()
    #in this line, we convert our list into an numpy array, so I don't have to try deal with itterating through the list just to get the second value (can't access second value with iteration)
    Dates = np.array(LastRecord)
    #grabbing dates from numpy array and stripping time value
    DateExtract = Dates[0][0]
    LastDate =  DateExtract.date()
    DateExtract = Dates[1][0]
    PrevDate =  DateExtract.date()
    #here we add the value to the specific row using the .loc and index 
    CheckDate.loc[index,'Last Update'] = LastDate
    CheckDate.loc[index,'Prev Update'] = PrevDate

#print(CheckDate)
CheckDate.to_csv('LatestDates.csv', index = False)
