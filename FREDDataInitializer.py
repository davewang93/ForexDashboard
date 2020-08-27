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

#using this api wrapper: https://github.com/mortada/fredapi

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

'''list of current indicators and associated table name in db

FRED Value : table name in macrodashboard

GDP : usgdp : quarterly
DTWEXBGS : dollarindexbroad :daily (weekly lag)
DTWEXAFEGS : dollarindexadvanced :daily (weekly lag)
CP : corpprofittaxed : quarterly
A446RC1Q027SBEA : corpprofituntaxed : quarterly
WALCL : fredtotalassets : wednesdays
WORAL : fedassetsrepos : wednesdays
WSHOMCB : fredassetsmbs : wednesdays
TREAST : fredassetstreas : wednesdays
SWPT : fedassetscbswaps : wednesdays
WFCDA : fedassetsforeign : wednesdays
M2 : m2 : mondays (monthly lag)
GFDEGDQ188S : usdebtgdp : quarterly
MTSDS133FMS : federalbudget : monthly
HOUST : housingstarts : monthly
RELACBW027SBOG : usrealestateloans : weekly (monthly lag?)
DRSREACBS : delinquencyrealestatesecured : quarterly
PERMIT : newbuildingpermits : monthly
CPIAUCSL : usurbancpi : monthly
DGS10 : 10yrtreasbenchmark : daily
CPALTT01GBM657N : ukcpiall : monthly
IRLTLT01GBM156N : uk10yrgilt : monthly
LMUNRRTTGBM156N : ukunemploy : monthly
TRESEGGBM052N : ukforeignreserves : monthly 
GBP1MTD156N : uk1mlibor : daily
CPMNACNSAB1GQUK : ukgdp : quarterly
BSPRFT02GBM460S :  ukfuturemanufacturingsurvey : monthly
GBRPROMANMISMEI : uktotamanuoutput : monthly
CPMEURNSAB1GQEU272020 : eugdp27 : quarterly
EU28CPALTT01GPM : eucpiall : monthly
ECBASSETSW : eucbassets : weekly
PRMNTO01EUQ661S : eumanuprod : quarterly
CPALTT01DEM657N : germcpiall : monthly
CPALTT01FRM657N : frcpiall : monthly
CPALTT01ITM657N : itcpiall : monthly
IRLTLT01FRM156N : fr10yrbench : monthly
IRLTLT01ITM156N : it10yrbench : monthly
BSCICP03ITM665S : itmanusentiment : monthly
BSCICP03DEM665S : germmanusentiment : monthly
BSCICP03FRM665S : frmanusentiment : monthly
LMUNRRTTDEM156N : germunemploy : monthly
LMUNRLTTFRM647S : frunemploy : monthly
LRUN64TTITQ156S : itunemploy : monthly
JPNNGDP : jpgdp : quaterly 
IR3TIB01JPQ156N : jpbankrate : quarterly
JPNPROINDMISMEI : jpinduprod : monthly
LRUNTTTTJPM156N : jpunemploy : monthly
TRESEGJPM052N : jpforeignreserves : monthly
IRLTLT01JPQ156N : jp10yrbench : quarterly
CPALCY01JPQ661N : jpcpiall : quaterly
CSCICP03JPM665S : jpconsumersentiment : monthly
JPNASSETS : jptotalassets : monthly
PRMNTO01AUQ156S : aumanuprod : quarterly
AUSGDPRQDSMEI : augdp : quarterly
TRESEGAUM052N : auforeignreserves : monthly
AUSCPIALLQINMEI : aucpiall : quarterly
MANMM101AUM189N : aum1supply : monthly
CSCICP03AUM665S : auconsumersentiment : monthly
IR3TIB01AUM156N : auinterbank : monthly
AUSODCNPI03GPSAM : aunewbuildingpermits : monthly
LFUNTTTTAUM647N : auunemploy : monthly
IRLTLT01AUM156N : au10yrbench : monthly
CPMNACSCAB1GQFR : frgdp : quarterly
CPMNACSCAB1GQDE : germgdp : quarterly
CPMNACSCAB1GQIT : itgdp : quarterly
PSAVERT : ussavings : monthly

'''
dataseries = fred.get_series('PSAVERT')
table = dataseries.reset_index()
#see end of doc for explanation of line below
table.columns = ['Date','Value']
#optional line to clean up null values at head of table
#table = table.iloc[60:]
print(table.head())
table.to_sql("ussavings",engine)

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