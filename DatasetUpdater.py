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

#quandl API Key
ql.ApiConfig.api_key = qlkey

#block below is used to update tables - should be turned into callable object to save on the copy paste

#create sql.connector cursor, often called "self"
my_cursor = mydb.cursor()
#call last record of sql table - notice it outputs as list in list even when select specific column, ie "Date"
my_cursor.execute("SELECT DATE FROM usgovtyieldcurve ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
#print(LastRecord)

#extract date from list in list
LastDate = LastRecord[0][0]


#Drop the time using .date() and add 1 day to get new start date
StartDate = LastDate.date() + timedelta(days=1)
#print(StartDate)

#pull new values going forward from start date
RawUpdateTable = ql.get("USTREASURY/YIELD", start_date=StartDate, paginate=True)

#print(GBPUSDUpdate)

#append into existing sql table
RawUpdateTable.to_sql("usgovtyieldcurve",engine,if_exists='append')

#check output
print('usgovtyieldcurve')
print(RawUpdateTable)

#below is the update code for each indicator

my_cursor.execute("SELECT DATE FROM usfedfundsrate ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
RawUpdateTable = ql.get("FRED/DFF", start_date=StartDate, paginate=True)
RawUpdateTable.to_sql("usfedfundsrate",engine,if_exists='append')
print('usfedfundsrate')
print(RawUpdateTable)

my_cursor.execute("SELECT DATE FROM 5yrgilt ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
RawUpdateTable = ql.get("BOE/IUDSNPY", start_date=StartDate, paginate=True)
RawUpdateTable.to_sql("5yrgilt",engine,if_exists='append')
print('5yrgilt')
print(RawUpdateTable)

my_cursor.execute("SELECT DATE FROM 10yrgilt ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
RawUpdateTable = ql.get("BOE/IUDMNPY", start_date=StartDate, paginate=True)
RawUpdateTable.to_sql("10yrgilt",engine,if_exists='append')
print('10yrgilt')
print(RawUpdateTable)

my_cursor.execute("SELECT DATE FROM 20yrgilt ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
RawUpdateTable = ql.get("BOE/IUDLNPY", start_date=StartDate, paginate=True)
RawUpdateTable.to_sql("20yrgilt",engine,if_exists='append')
print('20yrgilt')
print(RawUpdateTable)

my_cursor.execute("SELECT DATE FROM 10yrtips ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
RawUpdateTable = ql.get("FRED/DFII10", start_date=StartDate, paginate=True)
RawUpdateTable.to_sql("10yrtips",engine,if_exists='append')
print('10yrtips')
print(RawUpdateTable)

my_cursor.execute("SELECT DATE FROM intlagriidx ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
RawUpdateTable = ql.get("RICI/RICIA", start_date=StartDate, paginate=True)
RawUpdateTable.to_sql("intlagriidx",engine,if_exists='append')
print('intlagriidx')
print(RawUpdateTable)

my_cursor.execute("SELECT DATE FROM intlmetalsidx ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
RawUpdateTable = ql.get("RICI/RICIM", start_date=StartDate, paginate=True)
RawUpdateTable.to_sql("intlmetalsidx",engine,if_exists='append')
print('intlmetalsidx')
print(RawUpdateTable)

my_cursor.execute("SELECT DATE FROM intlenergyidx ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
RawUpdateTable = ql.get("RICI/RICIE", start_date=StartDate, paginate=True)
RawUpdateTable.to_sql("intlenergyidx",engine,if_exists='append')
print('intlenergyidx')
print(RawUpdateTable)

my_cursor.execute("SELECT DATE FROM opeccrude ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
RawUpdateTable = ql.get("OPEC/ORB", start_date=StartDate, paginate=True)
RawUpdateTable.to_sql("opeccrude",engine,if_exists='append')
print('opeccrude')
print(RawUpdateTable)

my_cursor.execute("SELECT DATE FROM usjoblessclaims ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
RawUpdateTable = ql.get("FRED/ICSA", start_date=StartDate, paginate=True)
RawUpdateTable.to_sql("usjoblessclaims",engine,if_exists='append')
print('usjoblessclaims')
print(RawUpdateTable)

my_cursor.execute("SELECT DATE FROM cotgpb ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
RawUpdateTable = ql.get("CFTC/096742_F_L_ALL", start_date=StartDate, paginate=True)
RawUpdateTable.to_sql("cotgpb",engine,if_exists='append')
print('cotgpb')
print(RawUpdateTable)

my_cursor.execute("SELECT DATE FROM cotusd ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
RawUpdateTable = ql.get("CFTC/098662_F_L_ALL", start_date=StartDate, paginate=True)
RawUpdateTable.to_sql("cotusd",engine,if_exists='append')
print('cotusd')
print(RawUpdateTable)

my_cursor.execute("SELECT DATE FROM cotaud ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
RawUpdateTable = ql.get("CFTC/232741_F_L_ALL", start_date=StartDate, paginate=True)
RawUpdateTable.to_sql("cotaud",engine,if_exists='append')
print('cotaud')
print(RawUpdateTable)

my_cursor.execute("SELECT DATE FROM cotyen ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
RawUpdateTable = ql.get("CFTC/097741_F_L_ALL", start_date=StartDate, paginate=True)
RawUpdateTable.to_sql("cotyen",engine,if_exists='append')
print('cotyen')
print(RawUpdateTable)

my_cursor.execute("SELECT DATE FROM coteur ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
RawUpdateTable = ql.get("CFTC/099741_F_L_ALL", start_date=StartDate, paginate=True)
RawUpdateTable.to_sql("coteur",engine,if_exists='append')
print('coteur')
print(RawUpdateTable)

my_cursor.execute("SELECT DATE FROM wheatfut ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
RawUpdateTable = ql.get("CHRIS/CME_W1", start_date=StartDate, paginate=True)
RawUpdateTable.to_sql("wheatfut",engine,if_exists='append')
print('wheatfut')
print(RawUpdateTable)

my_cursor.execute("SELECT DATE FROM cattlefut ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
RawUpdateTable = ql.get("CHRIS/CME_LC1", start_date=StartDate, paginate=True)
RawUpdateTable.to_sql("cattlefut",engine,if_exists='append')
print('cattlefut')
print(RawUpdateTable)

my_cursor.execute("SELECT DATE FROM soybeanfut ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
RawUpdateTable = ql.get("CHRIS/CME_S1", start_date=StartDate, paginate=True)
RawUpdateTable.to_sql("soybeanfut",engine,if_exists='append')
print('soybeanfut')
print(RawUpdateTable)

my_cursor.execute("SELECT DATE FROM nickelfut ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
RawUpdateTable = ql.get("CHRIS/MCX_NI1", start_date=StartDate, paginate=True)
RawUpdateTable.to_sql("nickelfut",engine,if_exists='append')
print('nickelfut')
print(RawUpdateTable)

my_cursor.execute("SELECT DATE FROM silverfut ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
RawUpdateTable = ql.get("CHRIS/CME_SI1", start_date=StartDate, paginate=True)
RawUpdateTable.to_sql("silverfut",engine,if_exists='append')
print('silverfut')
print(RawUpdateTable)

my_cursor.execute("SELECT DATE FROM zincfut ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
RawUpdateTable = ql.get("CHRIS/SHFE_ZN1", start_date=StartDate, paginate=True)
RawUpdateTable.to_sql("zincfut",engine,if_exists='append')
print('zincfut')
print(RawUpdateTable)

my_cursor.execute("SELECT DATE FROM jpngovtyields ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
RawUpdateTable = ql.get("MOFJ/INTEREST_RATE_JAPAN", start_date=StartDate, paginate=True)
RawUpdateTable.to_sql("jpngovtyields",engine,if_exists='append')
print('jpngovtyields')
print(RawUpdateTable)

my_cursor.execute("SELECT DATE FROM grmygovtyield30 ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
RawUpdateTable = ql.get("BUNDESBANK/BBK01_WT3030", start_date=StartDate, paginate=True)
RawUpdateTable.to_sql("grmygovtyield30",engine,if_exists='append')
print('grmygovtyield30')
print(RawUpdateTable)

my_cursor.execute("SELECT DATE FROM grmygovtyield10 ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
RawUpdateTable = ql.get("BUNDESBANK/BBK01_WT1010", start_date=StartDate, paginate=True)
RawUpdateTable.to_sql("grmygovtyield10",engine,if_exists='append')
print('grmygovtyield10')
print(RawUpdateTable)

my_cursor.execute("SELECT DATE FROM aaayieldidx ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
RawUpdateTable = ql.get("ML/AAAEY", start_date=StartDate, paginate=True)
RawUpdateTable.to_sql("aaayieldidx",engine,if_exists='append')
print('aaayieldidx')
print(RawUpdateTable)

my_cursor.execute("SELECT DATE FROM bbbyieldidx ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
RawUpdateTable = ql.get("ML/BBBEY", start_date=StartDate, paginate=True)
RawUpdateTable.to_sql("bbbyieldidx",engine,if_exists='append')
print('bbbyieldidx')
print(RawUpdateTable)

my_cursor.execute("SELECT DATE FROM eminvestmentidx ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
RawUpdateTable = ql.get("ML/EMHGY", start_date=StartDate, paginate=True)
RawUpdateTable.to_sql("eminvestmentidx",engine,if_exists='append')
print('eminvestmentidx')
print(RawUpdateTable)

my_cursor.execute("SELECT DATE FROM cccyieldidx ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
RawUpdateTable = ql.get("ML/CCCY", start_date=StartDate, paginate=True)
RawUpdateTable.to_sql("cccyieldidx",engine,if_exists='append')
print('cccyieldidx')
print(RawUpdateTable)

my_cursor.execute("SELECT DATE FROM aaoasidx ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
RawUpdateTable = ql.get("ML/AAOAS", start_date=StartDate, paginate=True)
RawUpdateTable.to_sql("aaoasidx",engine,if_exists='append')
print('aaoasidx')
print(RawUpdateTable)

my_cursor.execute("SELECT DATE FROM boasidx ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
RawUpdateTable = ql.get("ML/BOAS", start_date=StartDate, paginate=True)
RawUpdateTable.to_sql("boasidx",engine,if_exists='append')
print('boasidx')
print(RawUpdateTable)

my_cursor.execute("SELECT DATE FROM hyoasdidx ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
RawUpdateTable = ql.get("ML/HYOAS", start_date=StartDate, paginate=True)
RawUpdateTable.to_sql("hyoasdidx",engine,if_exists='append')
print('hyoasdidx')
print(RawUpdateTable)

my_cursor.execute("SELECT DATE FROM emoasidx ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
RawUpdateTable = ql.get("ML/EMCBI", start_date=StartDate, paginate=True)
RawUpdateTable.to_sql("emoasidx",engine,if_exists='append')
print('emoasidx')
print(RawUpdateTable)

my_cursor.execute("SELECT DATE FROM manupmi ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
RawUpdateTable = ql.get("ISM/MAN_PMI", start_date=StartDate, paginate=True)
RawUpdateTable.to_sql("manupmi",engine,if_exists='append')
print('manupmi')
print(RawUpdateTable)

my_cursor.execute("SELECT DATE FROM nonmanunmi ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
RawUpdateTable = ql.get("ISM/NONMAN_NMI", start_date=StartDate, paginate=True)
RawUpdateTable.to_sql("nonmanunmi",engine,if_exists='append')
print('nonmanunmi')
print(RawUpdateTable)

my_cursor.execute("SELECT DATE FROM umichcsi ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
RawUpdateTable = ql.get("UMICH/SOC1", start_date=StartDate, paginate=True)
RawUpdateTable.to_sql("umichcsi",engine,if_exists='append')
print('umichcsi')
print(RawUpdateTable)

my_cursor.execute("SELECT DATE FROM boebankrate ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=1)
RawUpdateTable = ql.get("BOE/IUDBEDR", start_date=StartDate, paginate=True)
RawUpdateTable.to_sql("boebankrate",engine,if_exists='append')
print('boebankrate')
print(RawUpdateTable)


'''
#Test Code to Print Latest Data from sql tables
my_cursor.execute("SELECT DATE FROM wtioil ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
print(LastDate)
'''