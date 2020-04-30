import sqlalchemy
from sqlalchemy import create_engine
from configparser import ConfigParser 

parser = ConfigParser()
parser.read('config.ini')
engine = parser.get('engines','macrodbengine')

engine = create_engine(engine)

print(engine)

