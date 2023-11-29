from flask import jsonify, request, make_response

import sqlite3

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
                q += 'SELECT DISTINCT authorid FROM p2au WHERE paperid in (SELECT DISTINCT paperid FRON p2au WHERE authorid=?)'
                formatted_ids.extend([author, request.json['dates']['start'],request.json['dates']['end']])
            else:
                q += 'SELECT DISTINCT authorid FROM p2au WHERE paperid in (SELECT DISTINCT paperid FRON p2au WHERE authorid=?)'
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
                q += 'SELECT DISTINCT authorid FROM p2au WHERE paperid IN (SELECT DISTINCT paperid FROM p2org a JOIN pcount b ON a.paperid=b.paperid WHERE orgid=? AND year BETWEEN ? AND ?)'
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