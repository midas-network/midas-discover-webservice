from flask import jsonify, request, make_response

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


@midas_blueprint.route('/papers/', methods=['GET'])
def get_full_paper_list(is_internal=False):
    conn = connect_to_db()
    cur = conn.cursor()

    q = 'SELECT DISTINCT paperid FROM pdetails'
    cur.execute(q)
    rows = cur.fetchall()
    papers = {'papers': [x['paperid'] for x in rows]}
    if is_internal:
        return papers
    else:
        return make_response(jsonify(papers), 200)


@midas_blueprint.route('/orgs/', methods=['GET'])
def get_full_org_list(is_internal=False):
    conn = connect_to_db()
    cur = conn.cursor()

    q = 'SELECT DISTINCT orgid FROM p2org'
    cur.execute(q)
    rows = cur.fetchall()
    orgs = {'orgs': [x['orgid'] for x in rows]}
    if is_internal:
        return orgs
    else:
        return make_response(jsonify(orgs), 200)


@midas_blueprint.route('/authors/', methods=['GET'])
def get_full_author_list(is_internal=False):
    conn = connect_to_db()
    cur = conn.cursor()

    q = 'SELECT DISTINCT authorid FROM p2au'
    cur.execute(q)
    rows = cur.fetchall()
    authors = {'authors': [x['authorid'] for x in rows]}
    if is_internal:
        return authors
    else:
        return make_response(jsonify(authors), 200)


@midas_blueprint.route('/grants/', methods=['GET'])
def get_full_grant_list(is_internal=False):
    conn = connect_to_db()
    cur = conn.cursor()

    q = 'SELECT DISTINCT grantid FROM g2p'
    cur.execute(q)
    rows = cur.fetchall()
    grants = {'grants': [x['grantid'] for x in rows]}
    if is_internal:
        return grants
    else:
        return make_response(jsonify(grants), 200)


@midas_blueprint.route('/terms/', methods=['GET'])
def get_full_term_list(is_internal=False):
    conn = connect_to_db()
    cur = conn.cursor()

    q = 'SELECT DISTINCT term FROM pcount'
    cur.execute(q)
    rows = cur.fetchall()
    terms = {'terms': [x['term'] for x in rows]}
    if is_internal:
        return terms
    else:
        return make_response(jsonify(terms), 200)


@midas_blueprint.rout('/getSearchData/', methods=['GET'])
def get_search_data():
    papers = get_full_paper_list(True)
    orgs = get_full_org_list(True)
    authors = get_full_author_list(True)
    grants = get_full_grant_list(True)
    terms = get_full_term_list(True)

    response = papers.update(orgs).update(authors).update(grants).update(terms)
    return make_response(jsonify(response), 200)


@midas_blueprint.route('/papers/overlap/', methods=['GET'])
def get_paper_list():
    conn = connect_to_db()
    cur = conn.cursor()

    withAuthors = False
    withOrgs = False
    withTerms = False
    withGrants = False
    withDates = False

    if 'authors' in request.json.keys():
        withAuthors = True
    if 'orgs' in request.json.keys():
        withOrgs = True
    if 'terms' in request.json.keys():
        withTerms = True
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
    if withTerms:
        for term in request.json['terms']:
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

@midas_blueprint.route('/grants/overlap/', methods=['GET'])
def get_grant_list():
    conn = connect_to_db()
    cur = conn.cursor()

    withAuthors = False
    withOrgs = False # Not in powerpoint but seems doable
    withTerms = False
    withPapers = False
    withDates = False

    if 'authors' in request.json.keys():
        withAuthors = True
    if 'orgs' in request.json.keys():
        withOrgs = True
    if 'terms' in request.json.keys():
        withTerms = True
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
                q += ' AND strftime("%Y", startdate) BETWEEN ? AND ? OR strftime("%Y", enddate) BETWEEN ? AND ?)'
                formatted_ids.extend([request.json['dates']['start'],request.json['dates']['end'],
                                      request.json['dates']['start'],request.json['dates']['end']])
    if withOrgs:
        for org in request.json['orgs']:
            if len(q) != 0:
                q += ' INTERSECT '
            q += 'SELECT DISTINCT grantid FROM g2p a JOIN p2org b ON a.paperid=b.paperid WHERE orgid=?'
            formatted_ids.append(org)

            if withDates:
                q += ' AND strftime("%Y", startdate) BETWEEN ? AND ? OR strftime("%Y", enddate) BETWEEN ? AND ?)'
                formatted_ids.extend([request.json['dates']['start'],request.json['dates']['end'],
                                      request.json['dates']['start'],request.json['dates']['end']])
    if withTerms:
        for term in request.json['terms']:
            if len(q) != 0:
                q += ' INTERSECT '
            q += 'SELECT DISTINCT grantid FROM g2p a JOIN p2org b ON a.paperid=b.paperid WHERE term=?'
            formatted_ids.append(term)

            if withDates:
                q += ' AND strftime("%Y", startdate) BETWEEN ? AND ? OR strftime("%Y", enddate) BETWEEN ? AND ?)'
                formatted_ids.extend([request.json['dates']['start'],request.json['dates']['end'],
                                      request.json['dates']['start'],request.json['dates']['end']])
    if withPapers:
        for paper in request.json['papers']:
            if len(q) != 0:
                q += ' INTERSECT '
            q += 'SELECT DISTINCT grantid FROM g2p WHERE paperid=?'
            formatted_ids.append(paper)

            if withDates:
                q += ' AND strftime("%Y", startdate) BETWEEN ? AND ? OR strftime("%Y", enddate) BETWEEN ? AND ?)'
                formatted_ids.extend([request.json['dates']['start'],request.json['dates']['end'],
                                      request.json['dates']['start'],request.json['dates']['end']])
    
    cur.execute(q, tuple(formatted_ids))
    rows = cur.fetchall()
    grants = [x['grantid'] for x in rows]
    return make_response(jsonify(grants), 200) 

@midas_blueprint.route('/authors/overlap/', methods=['GET'])
def get_author_list():
    conn = connect_to_db()
    cur = conn.cursor()

    withAuthors = False
    withPapers = False # Not in powerpoint but seems doable
    withOrgs = False
    withTerms = False
    withGrants = False
    withDates = False

    if 'authors' in request.json.keys():
        withAuthors = True
    if 'papers' in request.json.keys():
        withPapers = True
    if 'orgs' in request.json.keys():
        withOrgs = True
    if 'terms' in request.json.keys():
        withTerms = True
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
    if withTerms:
        for term in request.json['terms']:
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
                q += ' AND strftime("%Y", startdate) BETWEEN ? AND ? OR strftime("%Y", enddate) BETWEEN ? AND ?)'
                formatted_ids.extend([request.json['dates']['start'],request.json['dates']['end'],
                                      request.json['dates']['start'],request.json['dates']['end']])
    
    print(q)
    cur.execute(q, tuple(formatted_ids))
    rows = cur.fetchall()
    authors = [x['authorid'] for x in rows]
    return make_response(jsonify(authors), 200) 

@midas_blueprint.route('/orgs/overlap/', methods=['GET'])
def get_org_list():
    conn = connect_to_db()
    cur = conn.cursor()

    withAuthors = False
    withTerms = False
    withGrants = False
    withDates = False

    if 'authors' in request.json.keys():
        withAuthors = True
    if 'terms' in request.json.keys():
        withTerms = True
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
    if withTerms:
        for term in request.json['terms']:
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
                q += 'SELECT DISTINCT orgid FROM p2org WHERE paperid IN (SELECT DISTINCT paperid FROM g2p WHERE grantid=? AND strftime("%Y", startdate) BETWEEN ? AND ? OR strftime("%Y", enddate) BETWEEN ? AND ?))'
                formatted_ids.extend(grant, [request.json['dates']['start'],request.json['dates']['end'],
                                      request.json['dates']['start'],request.json['dates']['end']])
            else:
                q += 'SELECT DISTINCT orgid FROM p2org WHERE paperid IN (SELECT DISTINCT paperid FROM g2p WHERE grantid=?)'
                formatted_ids.append(grant)
    
    cur.execute(q, tuple(formatted_ids))
    rows = cur.fetchall()
    orgs = [x['orgid'] for x in rows]
    return make_response(jsonify(orgs), 200) 