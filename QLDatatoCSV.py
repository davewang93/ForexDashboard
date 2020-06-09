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

qlkey = parser.get('keys','qlkey')

#quandl API Key
ql.ApiConfig.api_key = qlkey

#block below is used to initialize a new indicator - pulls the initial historical data points and creates table in database.

RawTable = ql.get("CHRIS/CME_SP1", paginate=True)

#push dataframe to sql table (creates table)

RawTable.to_excel(r"D:\OneDrive\David\src\Forex APIs Test Region\SP500Fut.xlsx")





