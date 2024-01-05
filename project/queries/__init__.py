import configparser
import os
from flask import Blueprint
from decouple import config

def get_db():
    app_config = configparser.ConfigParser()
    app_config.read('config.ini')
    return app_config[config('user')]['database_location']

def call_shutdown():
    print('Shutting down server.')
    os._exit(-1)


midas_blueprint = Blueprint('queries', __name__, template_folder='templates')

DB_LOCATION = get_db()
# if not os.path.isfile(DB_LOCATION):
#     print('The database listed in the config file (' + DB_LOCATION + ') does not exist.')
#     call_shutdown()

from . import routes
