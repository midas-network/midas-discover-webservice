from flask import Flask
from flasgger import Swagger


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
    return app

def register_blueprints(app):
    from project.queries import midas_blueprint

    app.register_blueprint(midas_blueprint)