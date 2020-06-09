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

frdkey = parser.get('keys','frdkey')

#using this api wrapper: https://github.com/mortada/fredapi

#fred api credentials
fred = Fred(api_key=frdkey)

dataseries = fred.get_series('LRUN64TTITQ156S')
table = dataseries.reset_index()
#see end of doc for explanation of line below
table.columns = ['Date','Value']
#optional line to clean up null values at head of table
#table = table.iloc[60:]
print(table.head())
table.to_excel(r"D:\OneDrive\David\src\Forex APIs Test Region\filename2.xlsx")

#have to do some manipulation data to return dataframe instead of series. per author:
'''
So actually the name `df` is a bit misleading because what is returned is actually a pd.Series

If you want a DataFrame with both the date and price as columns you could do

>>> s = fred.get_series('SP500')
>>> df = s.reset_index()
>>> df.columns = ['Date', 'Price']

Then you'd have
Date Price

if you want to just work with the pd.Series, you can also set a name on the index or values

>>> s.index.name = 'Date'
>>> s.name = 'Price'
>>> s.head()
'''