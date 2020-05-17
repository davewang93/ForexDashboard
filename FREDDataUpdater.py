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

my_cursor.execute("SELECT DATE FROM usgdp ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
dataseries = fred.get_series('GDP',StartDate,)
table = dataseries.reset_index()
table.columns = ['Date','Value']
#this line recreates the table without the top row ([0]). This is necessary for some of these 
# because the query is inclusive of the immediate previous value and i can't bother to figure out why
table = table.iloc[1:]
print('usgdp')
print(table)
table.to_sql("usgdp",engine,if_exists='append')

my_cursor.execute("SELECT DATE FROM dollarindexbroad ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
dataseries = fred.get_series('DTWEXBGS',StartDate,)
table = dataseries.reset_index()
table.columns = ['Date','Value']
print('dollarindexbroad')
print(table)
table.to_sql("dollarindexbroad",engine,if_exists='append')

my_cursor.execute("SELECT DATE FROM dollarindexadvanced ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
dataseries = fred.get_series('DTWEXAFEGS',StartDate,)
table = dataseries.reset_index()
table.columns = ['Date','Value']
print('dollarindexadvanced')
print(table)
table.to_sql("dollarindexadvanced",engine,if_exists='append')

my_cursor.execute("SELECT DATE FROM corpprofittaxed ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
dataseries = fred.get_series('CP',StartDate,)
table = dataseries.reset_index()
table.columns = ['Date','Value']
table = table.iloc[1:]
print('corpprofittaxed')
print(table)
table.to_sql("corpprofittaxed",engine,if_exists='append')

my_cursor.execute("SELECT DATE FROM corpprofituntaxed ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
dataseries = fred.get_series('A446RC1Q027SBEA',StartDate,)
table = dataseries.reset_index()
table.columns = ['Date','Value']
table = table.iloc[1:]
print('corpprofituntaxed')
print(table)
table.to_sql("corpprofituntaxed",engine,if_exists='append')

my_cursor.execute("SELECT DATE FROM fredtotalassets ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
dataseries = fred.get_series('WALCL',StartDate,)
table = dataseries.reset_index()
table.columns = ['Date','Value']
print('fredtotalassets')
print(table)
table.to_sql("fredtotalassets",engine,if_exists='append')

my_cursor.execute("SELECT DATE FROM fedassetsrepos ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
dataseries = fred.get_series('WORAL',StartDate,)
table = dataseries.reset_index()
table.columns = ['Date','Value']
print('fedassetsrepos')
print(table)
table.to_sql("fedassetsrepos",engine,if_exists='append')

my_cursor.execute("SELECT DATE FROM fedassetscbswaps ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
dataseries = fred.get_series('SWPT',StartDate,)
table = dataseries.reset_index()
table.columns = ['Date','Value']
print('fedassetscbswaps')
print(table)
table.to_sql("fedassetscbswaps",engine,if_exists='append')

my_cursor.execute("SELECT DATE FROM fedassetsforeign ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
dataseries = fred.get_series('WFCDA',StartDate,)
table = dataseries.reset_index()
table.columns = ['Date','Value']
print('fedassetsforeign')
print(table)
table.to_sql("fedassetsforeign",engine,if_exists='append')

my_cursor.execute("SELECT DATE FROM fredassetsmbs ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
dataseries = fred.get_series('WSHOMCB',StartDate,)
table = dataseries.reset_index()
table.columns = ['Date','Value']
print('fredassetsmbs')
print(table)
table.to_sql("fredassetsmbs",engine,if_exists='append')

my_cursor.execute("SELECT DATE FROM fredassetstreas ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
dataseries = fred.get_series('TREAST',StartDate,)
table = dataseries.reset_index()
table.columns = ['Date','Value']
print('fredassetstreas')
print(table)
table.to_sql("fredassetstreas",engine,if_exists='append')

my_cursor.execute("SELECT DATE FROM m2 ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
dataseries = fred.get_series('M2',StartDate,)
table = dataseries.reset_index()
table.columns = ['Date','Value']
print('m2')
print(table)
table.to_sql("m2",engine,if_exists='append')

my_cursor.execute("SELECT DATE FROM usdebtgdp ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
dataseries = fred.get_series('GFDEGDQ188S',StartDate,)
table = dataseries.reset_index()
table.columns = ['Date','Value']
table = table.iloc[1:]
print('usdebtgdp')
print(table)
table.to_sql("usdebtgdp",engine,if_exists='append')

my_cursor.execute("SELECT DATE FROM federalbudget ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
dataseries = fred.get_series('MTSDS133FMS',StartDate,)
table = dataseries.reset_index()
table.columns = ['Date','Value']
table = table.iloc[1:]
print('federalbudget')
print(table)
table.to_sql("federalbudget",engine,if_exists='append')

my_cursor.execute("SELECT DATE FROM housingstarts ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
dataseries = fred.get_series('HOUST',StartDate,)
table = dataseries.reset_index()
table.columns = ['Date','Value']
table = table.iloc[1:]
print('housingstarts')
print(table)
table.to_sql("housingstarts",engine,if_exists='append')

my_cursor.execute("SELECT DATE FROM usrealestateloans ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
dataseries = fred.get_series('RELACBW027SBOG',StartDate,)
table = dataseries.reset_index()
table.columns = ['Date','Value']
print('usrealestateloans')
print(table)
table.to_sql("usrealestateloans",engine,if_exists='append')

my_cursor.execute("SELECT DATE FROM delinquencyrealestatesecured ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
dataseries = fred.get_series('DRSREACBS',StartDate,)
table = dataseries.reset_index()
table.columns = ['Date','Value']
table = table.iloc[1:]
print('delinquencyrealestatesecured')
print(table)
table.to_sql("delinquencyrealestatesecured",engine,if_exists='append')

my_cursor.execute("SELECT DATE FROM newbuildingpermits ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
dataseries = fred.get_series('PERMIT',StartDate,)
table = dataseries.reset_index()
table.columns = ['Date','Value']
table = table.iloc[1:]
print('newbuildingpermits')
print(table)
table.to_sql("newbuildingpermits",engine,if_exists='append')

my_cursor.execute("SELECT DATE FROM usurbancpi ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
dataseries = fred.get_series('CPIAUCSL',StartDate,)
table = dataseries.reset_index()
table.columns = ['Date','Value']
table = table.iloc[1:]
print('usurbancpi')
print(table)
table.to_sql("usurbancpi",engine,if_exists='append')

my_cursor.execute("SELECT DATE FROM 10yrtreasbenchmark ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
dataseries = fred.get_series('DGS10',StartDate,)
table = dataseries.reset_index()
table.columns = ['Date','Value']
print('10yrtreasbenchmark')
print(table)
table.to_sql("10yrtreasbenchmark",engine,if_exists='append')

my_cursor.execute("SELECT DATE FROM ukcpiall ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
dataseries = fred.get_series('CPALTT01GBM657N',StartDate,)
table = dataseries.reset_index()
table.columns = ['Date','Value']
table = table.iloc[1:]
print('ukcpiall')
print(table)
table.to_sql("ukcpiall",engine,if_exists='append')

my_cursor.execute("SELECT DATE FROM uk10yrgilt ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
dataseries = fred.get_series('IRLTLT01GBM156N',StartDate,)
table = dataseries.reset_index()
table.columns = ['Date','Value']
table = table.iloc[1:]
print('uk10yrgilt')
print(table)
table.to_sql("uk10yrgilt",engine,if_exists='append')

my_cursor.execute("SELECT DATE FROM ukunemploy ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
dataseries = fred.get_series('LMUNRRTTGBM156N',StartDate,)
table = dataseries.reset_index()
table.columns = ['Date','Value']
table = table.iloc[1:]
print('ukunemploy')
print(table)
table.to_sql("ukunemploy",engine,if_exists='append')

my_cursor.execute("SELECT DATE FROM ukforeignreserves ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
dataseries = fred.get_series('TRESEGGBM052N',StartDate,)
table = dataseries.reset_index()
table.columns = ['Date','Value']
table = table.iloc[1:]
print('ukforeignreserves')
print(table)
table.to_sql("ukforeignreserves",engine,if_exists='append')

my_cursor.execute("SELECT DATE FROM uk1mlibor ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
dataseries = fred.get_series('GBP1MTD156N',StartDate,)
table = dataseries.reset_index()
table.columns = ['Date','Value']
print('uk1mlibor')
print(table)
table.to_sql("uk1mlibor",engine,if_exists='append')

my_cursor.execute("SELECT DATE FROM ukgdp ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
dataseries = fred.get_series('CPMNACNSAB1GQUK',StartDate,)
table = dataseries.reset_index()
table = table.iloc[1:]
table.columns = ['Date','Value']
print('ukgdp')
print(table)
table.to_sql("ukgdp",engine,if_exists='append')

my_cursor.execute("SELECT DATE FROM ukfuturemanufacturingsurvey ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
dataseries = fred.get_series('BSPRFT02GBM460S',StartDate,)
table = dataseries.reset_index()
table = table.iloc[1:]
table.columns = ['Date','Value']
print('ukfuturemanufacturingsurvey')
print(table)
table.to_sql("ukfuturemanufacturingsurvey",engine,if_exists='append')

my_cursor.execute("SELECT DATE FROM uktotamanuoutput ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
dataseries = fred.get_series('GBRPROMANMISMEI',StartDate,)
table = dataseries.reset_index()
table = table.iloc[1:]
table.columns = ['Date','Value']
print('uktotamanuoutput')
print(table)
table.to_sql("uktotamanuoutput",engine,if_exists='append')

my_cursor.execute("SELECT DATE FROM eugdp27 ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
dataseries = fred.get_series('CPMEURNSAB1GQEU272020',StartDate,)
table = dataseries.reset_index()
table = table.iloc[1:]
table.columns = ['Date','Value']
print('eugdp27')
print(table)
table.to_sql("eugdp27",engine,if_exists='append')

my_cursor.execute("SELECT DATE FROM eucpiall ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
dataseries = fred.get_series('EU28CPALTT01GPM',StartDate,)
table = dataseries.reset_index()
table = table.iloc[1:]
table.columns = ['Date','Value']
print('eucpiall')
print(table)
table.to_sql("eucpiall",engine,if_exists='append')

my_cursor.execute("SELECT DATE FROM eucbassets ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
dataseries = fred.get_series('ECBASSETSW',StartDate,)
table = dataseries.reset_index()
table.columns = ['Date','Value']
print('eucbassets')
print(table)
table.to_sql("eucbassets",engine,if_exists='append')

my_cursor.execute("SELECT DATE FROM eumanuprod ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
dataseries = fred.get_series('PRMNTO01EUQ661S',StartDate,)
table = dataseries.reset_index()
table = table.iloc[1:]
table.columns = ['Date','Value']
print('eumanuprod')
print(table)
table.to_sql("eumanuprod",engine,if_exists='append')

my_cursor.execute("SELECT DATE FROM germcpiall ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
dataseries = fred.get_series('CPALTT01DEM657N',StartDate,)
table = dataseries.reset_index()
table = table.iloc[1:]
table.columns = ['Date','Value']
print('germcpiall')
print(table)
table.to_sql("germcpiall",engine,if_exists='append')

my_cursor.execute("SELECT DATE FROM frcpiall ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
dataseries = fred.get_series('CPALTT01FRM657N',StartDate,)
table = dataseries.reset_index()
table = table.iloc[1:]
table.columns = ['Date','Value']
print('frcpiall')
print(table)
table.to_sql("frcpiall",engine,if_exists='append')

my_cursor.execute("SELECT DATE FROM itcpiall ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
dataseries = fred.get_series('CPALTT01ITM657N',StartDate,)
table = dataseries.reset_index()
table = table.iloc[1:]
table.columns = ['Date','Value']
print('itcpiall')
print(table)
table.to_sql("itcpiall",engine,if_exists='append')

my_cursor.execute("SELECT DATE FROM fr10yrbench ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
dataseries = fred.get_series('IRLTLT01FRM156N',StartDate,)
table = dataseries.reset_index()
table = table.iloc[1:]
table.columns = ['Date','Value']
print('fr10yrbench')
print(table)
table.to_sql("fr10yrbench",engine,if_exists='append')

my_cursor.execute("SELECT DATE FROM it10yrbench ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
dataseries = fred.get_series('IRLTLT01ITM156N',StartDate,)
table = dataseries.reset_index()
table = table.iloc[1:]
table.columns = ['Date','Value']
print('it10yrbench')
print(table)
table.to_sql("it10yrbench",engine,if_exists='append')

my_cursor.execute("SELECT DATE FROM itmanusentiment ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
dataseries = fred.get_series('BSCICP03ITM665S',StartDate,)
table = dataseries.reset_index()
table = table.iloc[1:]
table.columns = ['Date','Value']
print('itmanusentiment')
print(table)
table.to_sql("itmanusentiment",engine,if_exists='append')

my_cursor.execute("SELECT DATE FROM germmanusentiment ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
dataseries = fred.get_series('BSCICP03DEM665S',StartDate,)
table = dataseries.reset_index()
table = table.iloc[1:]
table.columns = ['Date','Value']
print('germmanusentiment')
print(table)
table.to_sql("germmanusentiment",engine,if_exists='append')

my_cursor.execute("SELECT DATE FROM frmanusentiment ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
dataseries = fred.get_series('BSCICP03FRM665S',StartDate,)
table = dataseries.reset_index()
table = table.iloc[1:]
table.columns = ['Date','Value']
print('frmanusentiment')
print(table)
table.to_sql("frmanusentiment",engine,if_exists='append')

my_cursor.execute("SELECT DATE FROM germunemploy ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
dataseries = fred.get_series('LMUNRRTTDEM156N',StartDate,)
table = dataseries.reset_index()
table = table.iloc[1:]
table.columns = ['Date','Value']
print('germunemploy')
print(table)
table.to_sql("germunemploy",engine,if_exists='append')

my_cursor.execute("SELECT DATE FROM frunemploy ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
dataseries = fred.get_series('LMUNRLTTFRM647S',StartDate,)
table = dataseries.reset_index()
table = table.iloc[1:]
table.columns = ['Date','Value']
print('frunemploy')
print(table)
table.to_sql("frunemploy",engine,if_exists='append')

my_cursor.execute("SELECT DATE FROM itunemploy ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
dataseries = fred.get_series('LRUN64TTITQ156S',StartDate,)
table = dataseries.reset_index()
table = table.iloc[1:]
table.columns = ['Date','Value']
print('itunemploy')
print(table)
table.to_sql("itunemploy",engine,if_exists='append')

my_cursor.execute("SELECT DATE FROM jpgdp ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
dataseries = fred.get_series('JPNNGDP',StartDate,)
table = dataseries.reset_index()
table = table.iloc[1:]
table.columns = ['Date','Value']
print('jpgdp')
print(table)
table.to_sql("jpgdp",engine,if_exists='append')

my_cursor.execute("SELECT DATE FROM jpbankrate ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
dataseries = fred.get_series('IR3TIB01JPQ156N',StartDate,)
table = dataseries.reset_index()
table = table.iloc[1:]
table.columns = ['Date','Value']
print('jpbankrate')
print(table)
table.to_sql("jpbankrate",engine,if_exists='append')

my_cursor.execute("SELECT DATE FROM jpinduprod ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
dataseries = fred.get_series('JPNPROINDMISMEI',StartDate,)
table = dataseries.reset_index()
table = table.iloc[1:]
table.columns = ['Date','Value']
print('jpinduprod')
print(table)
table.to_sql("jpinduprod",engine,if_exists='append')

my_cursor.execute("SELECT DATE FROM jpunemploy ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
dataseries = fred.get_series('LRUNTTTTJPM156N',StartDate,)
table = dataseries.reset_index()
table = table.iloc[1:]
table.columns = ['Date','Value']
print('jpunemploy')
print(table)
table.to_sql("jpunemploy",engine,if_exists='append')

my_cursor.execute("SELECT DATE FROM jpforeignreserves ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
dataseries = fred.get_series('TRESEGJPM052N',StartDate,)
table = dataseries.reset_index()
table = table.iloc[1:]
table.columns = ['Date','Value']
print('jpforeignreserves')
print(table)
table.to_sql("jpforeignreserves",engine,if_exists='append')

my_cursor.execute("SELECT DATE FROM jp10yrbench ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
dataseries = fred.get_series('IRLTLT01JPQ156N',StartDate,)
table = dataseries.reset_index()
table = table.iloc[1:]
table.columns = ['Date','Value']
print('jp10yrbench')
print(table)
table.to_sql("jp10yrbench",engine,if_exists='append')

my_cursor.execute("SELECT DATE FROM jpcpiall ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
dataseries = fred.get_series('CPALCY01JPQ661N',StartDate,)
table = dataseries.reset_index()
table = table.iloc[1:]
table.columns = ['Date','Value']
print('jpcpiall')
print(table)
table.to_sql("jpcpiall",engine,if_exists='append')

my_cursor.execute("SELECT DATE FROM jpconsumersentiment ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
dataseries = fred.get_series('CSCICP03JPM665S',StartDate,)
table = dataseries.reset_index()
table = table.iloc[1:]
table.columns = ['Date','Value']
print('jpconsumersentiment')
print(table)
table.to_sql("jpconsumersentiment",engine,if_exists='append')

my_cursor.execute("SELECT DATE FROM jptotalassets ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
dataseries = fred.get_series('JPNASSETS',StartDate,)
table = dataseries.reset_index()
table = table.iloc[1:]
table.columns = ['Date','Value']
print('jptotalassets')
print(table)
table.to_sql("jptotalassets",engine,if_exists='append')

my_cursor.execute("SELECT DATE FROM aumanuprod ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
dataseries = fred.get_series('PRMNTO01AUQ156S',StartDate,)
table = dataseries.reset_index()
table = table.iloc[1:]
table.columns = ['Date','Value']
print('aumanuprod')
print(table)
table.to_sql("aumanuprod",engine,if_exists='append')

my_cursor.execute("SELECT DATE FROM augdp ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
dataseries = fred.get_series('AUSGDPRQDSMEI',StartDate,)
table = dataseries.reset_index()
table = table.iloc[1:]
table.columns = ['Date','Value']
print('augdp')
print(table)
table.to_sql("augdp",engine,if_exists='append')

my_cursor.execute("SELECT DATE FROM auforeignreserves ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
dataseries = fred.get_series('TRESEGAUM052N',StartDate,)
table = dataseries.reset_index()
table = table.iloc[1:]
table.columns = ['Date','Value']
print('auforeignreserves')
print(table)
table.to_sql("auforeignreserves",engine,if_exists='append')

my_cursor.execute("SELECT DATE FROM aucpiall ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
dataseries = fred.get_series('AUSCPIALLQINMEI',StartDate,)
table = dataseries.reset_index()
table = table.iloc[1:]
table.columns = ['Date','Value']
print('aucpiall')
print(table)
table.to_sql("aucpiall",engine,if_exists='append')

my_cursor.execute("SELECT DATE FROM aum1supply ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
dataseries = fred.get_series('MANMM101AUM189N',StartDate,)
table = dataseries.reset_index()
table = table.iloc[1:]
table.columns = ['Date','Value']
print('aum1supply')
print(table)
table.to_sql("aum1supply",engine,if_exists='append')

my_cursor.execute("SELECT DATE FROM auconsumersentiment ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
dataseries = fred.get_series('CSCICP03AUM665S',StartDate,)
table = dataseries.reset_index()
table = table.iloc[1:]
table.columns = ['Date','Value']
print('auconsumersentiment')
print(table)
table.to_sql("auconsumersentiment",engine,if_exists='append')

my_cursor.execute("SELECT DATE FROM auinterbank ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
dataseries = fred.get_series('IR3TIB01AUM156N',StartDate,)
table = dataseries.reset_index()
table = table.iloc[1:]
table.columns = ['Date','Value']
print('auinterbank')
print(table)
table.to_sql("auinterbank",engine,if_exists='append')

my_cursor.execute("SELECT DATE FROM au10yrbench ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
dataseries = fred.get_series('IRLTLT01AUM156N',StartDate,)
table = dataseries.reset_index()
table = table.iloc[1:]
table.columns = ['Date','Value']
print('au10yrbench')
print(table)
table.to_sql("au10yrbench",engine,if_exists='append')

my_cursor.execute("SELECT DATE FROM aunewbuildingpermits ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
dataseries = fred.get_series('AUSODCNPI03GPSAM',StartDate,)
table = dataseries.reset_index()
table = table.iloc[1:]
table.columns = ['Date','Value']
print('aunewbuildingpermits')
print(table)
table.to_sql("aunewbuildingpermits",engine,if_exists='append')

my_cursor.execute("SELECT DATE FROM auunemploy ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
dataseries = fred.get_series('LFUNTTTTAUM647N',StartDate,)
table = dataseries.reset_index()
table = table.iloc[1:]
table.columns = ['Date','Value']
print('auunemploy')
print(table)
table.to_sql("auunemploy",engine,if_exists='append')

my_cursor.execute("SELECT DATE FROM germgdp ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
dataseries = fred.get_series('CPMNACSCAB1GQDE',StartDate,)
table = dataseries.reset_index()
table = table.iloc[1:]
table.columns = ['Date','Value']
print('germgdp')
print(table)
table.to_sql("germgdp",engine,if_exists='append')

my_cursor.execute("SELECT DATE FROM frgdp ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
dataseries = fred.get_series('CPMNACSCAB1GQFR',StartDate,)
table = dataseries.reset_index()
table = table.iloc[1:]
table.columns = ['Date','Value']
print('frgdp')
print(table)
table.to_sql("frgdp",engine,if_exists='append')

my_cursor.execute("SELECT DATE FROM itgdp ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
dataseries = fred.get_series('CPMNACSCAB1GQIT',StartDate,)
table = dataseries.reset_index()
table = table.iloc[1:]
table.columns = ['Date','Value']
print('itgdp')
print(table)
table.to_sql("itgdp",engine,if_exists='append')