from decouple import config
from flask import Flask
from flasgger import Swagger
from werkzeug.middleware.proxy_fix import ProxyFix


'''
Tables
org_relations: orgid,rel_id,rel_type
p2au: paperid,authorid
p2org: paperid,orgid
p2s: paperid,Department,School,University,NotSpecified,Laboratory,Institute,Center,College,Division,GovernmentAgency,Hospital,Program
pcount: paperid,term,count,ngram,field
g2a: authorid, grantid
g2p: paperid, grantid
pdetails: paperid,title,abstract,year
odetails: orgid,org_name
adetails: authorid,author_name,orgid
gdetails: grantid,title,startdate,enddate
'''


def create_app():
    app = Flask(__name__)
    swagger = Swagger(app)

    register_blueprints(app)
    if (config('user') == "SERVER"):
        app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_prefix=1)
    return app

def register_blueprints(app):
    from webservice.queries import midas_blueprint

    app.register_blueprint(midas_blueprint)