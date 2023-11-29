from flask import Blueprint

midas_blueprint = Blueprint('queries', __name__, template_folder='templates')

from . import routes
