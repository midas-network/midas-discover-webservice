from flask import Flask


'''
Tables
org_relations: orgid,rel_id,rel_type
p2au: paperid,authorid
p2org: paperid,orgid
p2s: paperid,Department,School,University,NotSpecified,Laboratory,Institute,Center,College,Division,GovernmentAgency,Hospital,Program
pcount: paperid,year,term,count,ngram,field
pdetails: paperid,title,abstract
g2a: authorid, grantid, startdate, enddate
g2p: paperid, grantid, startdate, enddate
'''

def create_app():
    app = Flask(__name__)

    register_blueprints(app)

    return app

def register_blueprints(app):
    from project.queries import midas_blueprint

    app.register_blueprint(midas_blueprint)