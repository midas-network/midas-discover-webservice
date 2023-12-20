from flask import jsonify, request, make_response
from flasgger import swag_from

import sqlite3
from . import midas_blueprint

author_prefix = 'https://midasnetwork.us/people/'
org_prefix = 'https://midasnetwork.us/organizations/'


def connect_to_db():
    db_file = '/Users/looseymoose/midasDB'
    try:
        conn = sqlite3.connect(db_file)
        conn.row_factory=sqlite3.Row
        print('connection successful')
    except sqlite3.Error as e:
        print(e)

    return conn

@midas_blueprint.route('/searchData/', methods=['GET'])
@swag_from('../swagger_docs/getSearchData.yml')
def get_search_data():
    searches = [x.lower() for x in request.json['categories']]
    if not set(searches).issubset(['all', 'papers', 'organizations', 'authors', 'grants', 'keywords']):
        return make_response("Invalid value in search requests", 400)
    
    conn = connect_to_db()
    cur = conn.cursor()
    response = {}

    if 'papers' in searches:
        response.update(get_full_paper_list(cur))
    if 'organizations' in searches:
        response.update(get_full_org_list(cur))
    if 'authors' in searches:
        response.update(get_full_author_list(cur))
    if 'grants' in searches:
        response.update(get_full_grant_list(cur))
    if 'keywords' in searches:
        response.update(get_full_term_list(cur))

    # response = {**papers, **orgs, **authors, **grants, **terms}
    return make_response(jsonify(response), 200)

def get_full_paper_list(cur):
    q = 'SELECT DISTINCT paperid, title FROM pdetails'
    cur.execute(q)
    rows = cur.fetchall()
    papers = {'papers': [{'id': x['paperid'], 'name': x['title']} for x in rows]}
    return papers


def get_full_org_list(cur):
    q = 'SELECT DISTINCT orgid, org_name FROM odetails'
    cur.execute(q)
    rows = cur.fetchall()
    orgs = {'orgs': [{'id': x['orgid'], 'name': x['org_name']} for x in rows]}
    return orgs


def get_full_author_list(cur):
    q = 'SELECT DISTINCT authorid, author_name FROM adetails'
    cur.execute(q)
    rows = cur.fetchall()
    authors = {'authors': [{'id': x['authorid'], 'name': x['author_name']} for x in rows]}
    return authors


def get_full_grant_list(cur):
    q = 'SELECT DISTINCT grantid, title FROM gdetails'
    cur.execute(q)
    rows = cur.fetchall()
    grants = {'grants': [{'id': x['grantid'], 'name': x['title']} for x in rows]}
    return grants


def get_full_keyword_list(cur):
    q = 'SELECT DISTINCT term FROM pcount'
    cur.execute(q)
    rows = cur.fetchall()
    terms = {'keywords': [x['term'] for x in rows]}
    return terms


@midas_blueprint.route('/intersection/papers/', methods=['GET'])
@swag_from('../swagger_docs/paperOverlap.yml')
def get_paper_list():
    conn = connect_to_db()
    cur = conn.cursor()

    withAuthors = False
    withOrgs = False
    withKeywords = False
    withGrants = False
    withDates = False

    if 'authors' in request.json.keys():
        withAuthors = True
    if 'orgs' in request.json.keys():
        withOrgs = True
    if 'keywords' in request.json.keys():
        withKeywords = True
    if 'grants' in request.json.keys():
        withGrants = True
    if 'dates' in request.json.keys():
        withDates = True

    q = ''
    formatted_ids = []
    if withAuthors:
        for author in request.json['authors']:
            if len(q) != 0:
                q += ' INTERSECT '
            q += 'SELECT DISTINCT paperid FROM p2au WHERE authorid=?'
            formatted_ids.append(author)

            if withDates:
                q += ' AND paperid in (SELECT DISTINCT paperid FROM pcount WHERE year BETWEEN ? AND ?)'
                formatted_ids.extend([request.json['dates']['start'],request.json['dates']['end']])
    if withOrgs:
        for org in request.json['orgs']:
            if len(q) != 0:
                q += ' INTERSECT '
            q += 'SELECT DISTINCT paperid FROM p2org WHERE orgid=?'
            formatted_ids.append(org)

            if withDates:
                q += ' AND paperid IN (SELECT DISTINCT paperid FROM pcount WHERE year BETWEEN ? AND ?)'
                formatted_ids.extend([request.json['dates']['start'],request.json['dates']['end']])
    if withKeywords:
        for term in request.json['keywords']:
            if len(q) != 0:
                q += ' INTERSECT '
            q += 'SELECT DISTINCT paperid FROM pcount WHERE term=?'
            formatted_ids.append(term)

            if withDates:
                q += ' AND paperid IN (SELECT DISTINCT paperid FROM pcount WHERE year BETWEEN ? AND ?)'
                formatted_ids.extend([request.json['dates']['start'],request.json['dates']['end']])
    if withGrants:
        for grant in request.json['grants']:
            if len(q) != 0:
                q += ' INTERSECT '
            q += 'SELECT DISTINCT paperid FROM g2p WHERE grantid=?'
            formatted_ids.append(grant)

            if withDates:
                q += ' AND paperid IN (SELECT DISTINCT paperid FROM pcount WHERE year BETWEEN ? AND ?)'
                formatted_ids.extend([request.json['dates']['start'],request.json['dates']['end']])
    
    cur.execute(q, tuple(formatted_ids))
    rows = cur.fetchall()
    papers = [x['paperid'] for x in rows]
    return make_response(jsonify(papers), 200) 

@midas_blueprint.route('/intersection/grants/', methods=['GET'])
@swag_from('../swagger_docs/grantOverlap.yml')
def get_grant_list():
    conn = connect_to_db()
    cur = conn.cursor()

    withAuthors = False
    withOrgs = False # Not in powerpoint but seems doable
    withKeywords = False
    withPapers = False
    withDates = False

    if 'authors' in request.json.keys():
        withAuthors = True
    if 'orgs' in request.json.keys():
        withOrgs = True
    if 'keywords' in request.json.keys():
        withKeywords = True
    if 'papers' in request.json.keys():
        withPapers = True
    if 'dates' in request.json.keys():
        withDates = True

    q = ''
    formatted_ids = []
    if withAuthors:
        for author in request.json['authors']:
            if len(q) != 0:
                q += ' INTERSECT '
            q += 'SELECT DISTINCT grantid FROM g2a WHERE grantid=?'
            formatted_ids.append(author)

            if withDates:
                q += ' AND grantid IN (SELECT grantid FROM gdetails WHERE startdate BETWEEN ? AND ? OR enddate BETWEEN ? AND ?)'
                formatted_ids.extend([request.json['dates']['start'],request.json['dates']['end'],
                                      request.json['dates']['start'],request.json['dates']['end']])
    if withOrgs:
        for org in request.json['orgs']:
            if len(q) != 0:
                q += ' INTERSECT '
            q += 'SELECT DISTINCT grantid FROM g2p a JOIN p2org b ON a.paperid=b.paperid WHERE orgid=?'
            formatted_ids.append(org)

            if withDates:
                q += ' AND grantid IN (SELECT grantid FROM gdetails WHERE startdate BETWEEN ? AND ? OR enddate BETWEEN ? AND ?)'
                formatted_ids.extend([request.json['dates']['start'],request.json['dates']['end'],
                                      request.json['dates']['start'],request.json['dates']['end']])
    if withKeywords:
        for term in request.json['keywords']:
            if len(q) != 0:
                q += ' INTERSECT '
            q += 'SELECT DISTINCT grantid FROM g2p a JOIN p2org b ON a.paperid=b.paperid WHERE term=?'
            formatted_ids.append(term)

            if withDates:
                q += ' AND grantid IN (SELECT grantid FROM gdetails WHERE startdate BETWEEN ? AND ? OR enddate BETWEEN ? AND ?)'
                formatted_ids.extend([request.json['dates']['start'],request.json['dates']['end'],
                                      request.json['dates']['start'],request.json['dates']['end']])
    if withPapers:
        for paper in request.json['papers']:
            if len(q) != 0:
                q += ' INTERSECT '
            q += 'SELECT DISTINCT grantid FROM g2p WHERE paperid=?'
            formatted_ids.append(paper)

            if withDates:
                q += ' AND grantid IN (SELECT grantid FROM gdetails WHERE startdate BETWEEN ? AND ? OR enddate BETWEEN ? AND ?)'
                formatted_ids.extend([request.json['dates']['start'],request.json['dates']['end'],
                                      request.json['dates']['start'],request.json['dates']['end']])
    
    cur.execute(q, tuple(formatted_ids))
    rows = cur.fetchall()
    grants = [x['grantid'] for x in rows]
    return make_response(jsonify(grants), 200) 

@midas_blueprint.route('/intersection/people/', methods=['GET'])
@swag_from('../swagger_docs/peopleOverlap.yml')
def get_poeple_list():
    conn = connect_to_db()
    cur = conn.cursor()

    withAuthors = False
    withPapers = False # Not in powerpoint but seems doable
    withOrgs = False
    withKeywords = False
    withGrants = False
    withDates = False

    if 'coauthors' in request.json.keys():
        withAuthors = True
    if 'papers' in request.json.keys():
        withPapers = True
    if 'orgs' in request.json.keys():
        withOrgs = True
    if 'keywords' in request.json.keys():
        withKeywords = True
    if 'grants' in request.json.keys():
        withGrants = True
    if 'dates' in request.json.keys():
        withDates = True

    q = ''
    formatted_ids = []
    if withAuthors:
        for author in request.json['coauthors']:
            if len(q) != 0:
                q += ' INTERSECT '

            if withDates:
                q += 'SELECT DISTINCT authorid FROM p2au WHERE paperid IN (SELECT DISTINCT paperid FROM p2au WHERE authorid=?) AND paperid IN (SELECT DISTINCT paperid FROM pcount WHERE year BETWEEN ? AND ?)'
                formatted_ids.extend([author, request.json['dates']['start'],request.json['dates']['end']])
            else:
                q += 'SELECT DISTINCT authorid FROM p2au WHERE paperid IN (SELECT DISTINCT paperid FROM p2au WHERE authorid=?)'
                formatted_ids.append(author)
    if withPapers:
        for paper in request.json['papers']:
            if len(q) != 0:
                q += ' INTERSECT '
            q += 'SELECT DISTINCT authorid FROM p2au WHERE paperid=?'
            formatted_ids.append(paper)

            if withDates:
                q += ' AND paperid in (SELECT DISTINCT paperid FROM pcount WHERE year BETWEEN ? AND ?)'
                formatted_ids.extend([request.json['dates']['start'],request.json['dates']['end']])
    if withOrgs:
        for org in request.json['orgs']:
            if len(q) != 0:
                q += ' INTERSECT '

            if withDates:
                q += 'SELECT DISTINCT authorid FROM p2au WHERE paperid IN (SELECT DISTINCT a.paperid FROM p2org a JOIN pcount b ON a.paperid=b.paperid WHERE orgid=? AND year BETWEEN ? AND ?)'
                formatted_ids.extend([org, request.json['dates']['start'],request.json['dates']['end']])
            else:
                q += 'SELECT DISTINCT authorid FROM p2au WHERE paperid IN (SELECT DISTINCT paperid FROM p2org WHERE orgid=?)'
                formatted_ids.append(org)
    if withKeywords:
        for term in request.json['keywords']:
            if len(q) != 0:
                q += ' INTERSECT '
            
            if withDates:
                q += 'SELECT DISTINCT authorid FROM p2au WHERE paperid IN (SELECT DISTINCT paperid FROM pcount WHERE term=? AND year BETWEEN ? AND ?)'
                formatted_ids.extend([term, request.json['dates']['start'],request.json['dates']['end']])
            else:
                q += 'SELECT DISTINCT authorid FROM p2au WHERE paperid IN (SELECT DISTINCT paperid FROM pcount WHERE term=?)'
                formatted_ids.append(term)
    if withGrants:
        for grant in request.json['grants']:
            if len(q) != 0:
                q += ' INTERSECT '
            q += 'SELECT DISTINCT authorid FROM g2a WHERE grantid=?'
            formatted_ids.append(grant)

            if withDates:
                q += ' AND grantid IN (SELECT grantid FROM gdetails WHERE startdate BETWEEN ? AND ? OR enddate BETWEEN ? AND ?)'
                formatted_ids.extend([request.json['dates']['start'],request.json['dates']['end'],
                                      request.json['dates']['start'],request.json['dates']['end']])
    
    print(q)
    cur.execute(q, tuple(formatted_ids))
    rows = cur.fetchall()
    people = [x['authorid'] for x in rows]
    return make_response(jsonify(people), 200) 

@midas_blueprint.route('/intersection/orgs/', methods=['GET'])
@swag_from('../swagger_docs/orgOverlap.yml')
def get_org_list():
    conn = connect_to_db()
    cur = conn.cursor()

    withAuthors = False
    withKeywords = False
    withGrants = False
    withDates = False

    if 'authors' in request.json.keys():
        withAuthors = True
    if 'keywords' in request.json.keys():
        withKeywords = True
    if 'grants' in request.json.keys():
        withGrants = True
    if 'dates' in request.json.keys():
        withDates = True

    q = ''
    formatted_ids = []
    if withAuthors:
        for author in request.json['authors']:
            if len(q) != 0:
                q += ' INTERSECT '
            if withDates:
                q += 'SELECT DISTINCT orgid FROM p2org WHERE paperid IN (SELECT DISTINCT paperid FROM p2au a JOIN pcount b ON a.paperid=b.paperid WHERE authorid=? AND year BETWEEN ? AND ?)'
                formatted_ids.extend([author, request.json['dates']['start'],request.json['dates']['end']])
            else:
                q += 'SELECT DISTINCT orgid FROM p2org WHERE paperid IN (SELECT DISTINCT paperid FROM p2au WHERE authorid=?)'
                formatted_ids.append(author)
    if withKeywords:
        for term in request.json['keywords']:
            if len(q) != 0:
                q += ' INTERSECT '
            if withDates:
                q += 'SELECT DISTINCT orgid FROM p2org WHERE paperid IN (SELECT DISTINCT paperid FROM pcount WHERE term=? AND year BETWEEN ? AND ?)'
                formatted_ids.extend([term, request.json['dates']['start'],request.json['dates']['end']])
            else:
                q += 'SELECT DISTINCT orgid FROM p2org WHERE paperid IN (SELECT DISTINCT paperid FROM pcount WHERE term=?)'
                formatted_ids.append(term)
    if withGrants:
        for grant in request.json['grants']:
            if len(q) != 0:
                q += ' INTERSECT '

            if withDates:
                q += 'SELECT DISTINCT orgid FROM p2org WHERE paperid IN (SELECT DISTINCT paperid FROM g2p WHERE grantid=? AND grantid IN (SELECT grantid FROM gdetails WHERE startdate BETWEEN ? AND ? OR enddate BETWEEN ? AND ?))'
                formatted_ids.extend(grant, [request.json['dates']['start'],request.json['dates']['end'],
                                      request.json['dates']['start'],request.json['dates']['end']])
            else:
                q += 'SELECT DISTINCT orgid FROM p2org WHERE paperid IN (SELECT DISTINCT paperid FROM g2p WHERE grantid=?)'
                formatted_ids.append(grant)
    
    cur.execute(q, tuple(formatted_ids))
    rows = cur.fetchall()
    orgs = [x['orgid'] for x in rows]
    return make_response(jsonify(orgs), 200) 