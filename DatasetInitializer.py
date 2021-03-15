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

# Create a new DB in mySQL w/ block below

'''mydb = mysql.connector.connect(
        host = host,
        user = username,
        passwd = passwd,
    )

#create cursor
    cursor = mydb.cursor()

#create a db
    cursor.execute("CREATE DATABASE macrodashboard")'''

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

#block below is used to initialize a new indicator - pulls the initial historical data points and creates table in database.

'''list of current indicators and associated table name in db

Quandl Value : table name in macrodashboard


USTREASURY/YIELD : usgovtyieldcurve
BOE/IUDBEDR : boebankrate
FRED/DFII10 : 10yrtips
FRED/DFF : usfedfundsrate
FRED/ICSA : usjoblessclaims
BOE/IUDSNPY : 5yrgilt
BOE/IUDMNPY : 10yrgilt
BOE/IUDLNPY : 20yrgilt
RICI/RICIA : intlagriidx
RICI/RICIM : intlmetalsidx
RICI/RICIE : intlenergyidx
OPEC/ORB : opeccrude
CFTC/096742_F_L_ALL : cotgpb
CFTC/098662_F_L_ALL : cotusd
CFTC/232741_F_L_ALL : cotaud
CFTC/099741_F_L_ALL : coteur
CFTC/097741_F_L_ALL : cotyen
CHRIS/CME_W1 : wheatfut
CHRIS/CME_LC1 : cattlefut
CHRIS/CME_S1 : soybeanfut
CHRIS/MCX_NI1 : nickelfut
CHRIS/CME_SI1 : silverfut
CHRIS/SHFE_ZN1 : zincfut
MOFJ/INTEREST_RATE_JAPAN : jpngovtyields
BUNDESBANK/BBK01_WT3030 : grmygovtyield30
BUNDESBANK/BBK01_WT1010 : grmygovtyield10
ML/AAAEY : aaayieldidx
ML/BBBEY : bbbyieldidx
ML/EMHGY : eminvestmentidx
ML/CCCY : cccyieldidx
ML/AAOAS : aaoasidx
ML/BOAS : boasidx
ML/HYOAS : hyoasdidx
ML/EMCBI : emoasidx
ISM/MAN_PMI : manupmi
ISM/NONMAN_NMI : nonmanunmi
UMICH/SOC1 : umichcsi

'''

RawTable = ql.get("CHRIS/CME_NK1", paginate=True)

#RawTable.columns = ['2yr','3yr','5yr','10yr','idx','nsw1','nsw2','nsw3']

#RawTable.drop(['idx','nsw1','nsw2','nsw3'], axis=1, inplace=True)

#print(RawTable)

#push dataframe to sql table (creates table)
RawTable.to_sql("nikfut",engine)





