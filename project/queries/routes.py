import sqlite3
import traceback

from flask import jsonify, request, make_response
from flasgger import swag_from

from . import midas_blueprint
from . import DB_LOCATION

GRANTS = 'grants'
KEYWORDS = 'keywords'
PAPERS = 'papers'
PEOPLE = 'people'
ORGANIZATIONS = 'organizations'
DATES = 'dates'
GRANT_DATE_RANGE = 'grantDateRange'
PUBLICATION_DATE_RANGE = 'publicationDateRange'
GRANT_LIST = 'grantList'
START = 'start'
END = 'end'

withPeople = 'withPeople'
withOrgs = 'withOrgs'
withKeywords = 'withKeywords'
withPapers = 'withPapers'
withDates = 'withDates'

# TODO: Better errors for identifying malformed queries


def connect_to_db():
    try:
        conn = sqlite3.connect(DB_LOCATION)
        conn.row_factory = sqlite3.Row
        print('connection successful')
        return conn
    except sqlite3.Error as e:
        tb = traceback.format_exc()
        print(e)
        print(tb)


def find_org_children(cur, org):
    q = 'SELECT DISTINCT orgid FROM org_relations WHERE rel_id=?'
    cur.execute(q, (org,))
    rows = cur.fetchall()
    orgs = [x['orgid'] for x in rows]

    return orgs


def get_categories_in_query(keys):
    result = {
        withPeople: False,
        withOrgs: False,  # Not in powerpoint but seems doable
        withKeywords: False,
        withPapers: False,
        withDates: False,
    }

    if PEOPLE in keys:
        result.withPeople = True
    if ORGANIZATIONS in keys:
        result.withOrgs = True
    if KEYWORDS in keys:
        result.withKeywords = True
    if PAPERS in keys:
        result.withPapers = True
    if GRANT_DATE_RANGE in keys:
        result.withDates = True

    return result


def check_payload(request_payload, options):
    if not isinstance(request_payload.json, dict):
        return make_response('Check structure of request', 400)
    if not set(request_payload.json.keys()).issubset(options):
        return make_response("Invalid value in search requests. Allowed options: " + " ".join(options), 400)
    return None


@midas_blueprint.after_request
def after_request(response):
    response.headers.add('Access-Control-Allow-Origin', '*')
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization')
    response.headers.add('Access-Control-Allow-Methods', 'POST')
    return response


@midas_blueprint.route('/searchCategories/', methods=['GET'])
def get_search_categories():
    response = [PAPERS, ORGANIZATIONS, PEOPLE, GRANTS, KEYWORDS]
    return make_response(jsonify(response), 200)


@midas_blueprint.route('/searchData/', methods=['POST'])
@swag_from('../swagger_docs/getSearchData.yml')
def get_search_data():
    search_options = [PAPERS, ORGANIZATIONS, PEOPLE, GRANTS, KEYWORDS]
    searches = [x.lower() for x in request.json['categories']]
    if not set(searches).issubset(search_options):
        return make_response('Invalid value in search requests. Options are: ' + ' '.join(search_options), 400)

    conn = connect_to_db()
    cur = conn.cursor()
    response = {}

    if PAPERS in searches:
        response.update(get_full_paper_list(cur))
    if ORGANIZATIONS in searches:
        response.update(get_full_org_list(cur))
    if PEOPLE in searches:
        response.update(get_full_people_list(cur))
    if GRANTS in searches:
        response.update(get_full_grant_list(cur))
    if KEYWORDS in searches:
        response.update(get_full_keyword_list(cur))

    # response = {**papers, **orgs, **authors, **grants, **terms}
    return make_response(jsonify(response), 200)


def get_full_paper_list(cur):
    q = 'SELECT DISTINCT paperid, title FROM pdetails'
    cur.execute(q)
    rows = cur.fetchall()
    papers = {PAPERS: [{'id': x['paperid'], 'name': x['title']} for x in rows]}
    return papers


def get_full_org_list(cur):
    q = 'SELECT DISTINCT orgid, org_name FROM odetails'
    cur.execute(q)
    rows = cur.fetchall()
    orgs = {ORGANIZATIONS: [{'id': x['orgid'], 'name': x['org_name']} for x in rows]}
    return orgs


def get_full_people_list(cur):
    q = 'SELECT DISTINCT authorid, author_name FROM adetails'
    cur.execute(q)
    rows = cur.fetchall()
    people = {PEOPLE: [{'id': x['authorid'], 'name': x['author_name']} for x in rows]}
    return people


def get_full_grant_list(cur):
    q = 'SELECT DISTINCT grantid, title FROM gdetails'
    cur.execute(q)
    rows = cur.fetchall()
    grants = {GRANTS: [{'id': x['grantid'], 'name': x['title']} for x in rows]}
    return grants


def get_full_keyword_list(cur):
    q = 'SELECT DISTINCT term FROM pcount'
    cur.execute(q)
    rows = cur.fetchall()
    terms = {KEYWORDS: [x['term'] for x in rows]}
    return terms


@midas_blueprint.route('/intersection/papers/', methods=['POST'])
@swag_from('../swagger_docs/paperOverlap.yml')
def get_paper_list():
    paper_options = [PEOPLE, GRANTS, KEYWORDS, ORGANIZATIONS, PUBLICATION_DATE_RANGE]
    errors = check_payload(request, paper_options)
    if errors is not None:
        return errors
    conn = connect_to_db()
    cur = conn.cursor()

    keys = get_categories_in_query(request.json.keys())

    q = ''
    formatted_ids = []
    if keys.withDates:
        q = 'SELECT DISTINCT paperid FROM pdetails WHERE '
        if START in request.json[PUBLICATION_DATE_RANGE].keys():
            if END in request.json[PUBLICATION_DATE_RANGE].keys():
                q += 'year BETWEEN ? AND ?'
                formatted_ids.extend(
                    [request.json[PUBLICATION_DATE_RANGE][START], request.json[PUBLICATION_DATE_RANGE][END]])
            else:
                q += 'year >= ?'
                formatted_ids.extend([request.json[PUBLICATION_DATE_RANGE][START]])
        elif END in request.json[PUBLICATION_DATE_RANGE].keys():
            q += 'year <= ?'
            formatted_ids.extend([request.json[PUBLICATION_DATE_RANGE][END]])
    if keys.withAuthors:
        for author in request.json[PEOPLE]:
            if len(q) != 0:
                q += ' INTERSECT '
            q += 'SELECT DISTINCT paperid FROM p2au WHERE authorid=?'
            formatted_ids.append(author)
    if keys.withOrgs:
        for org in request.json[ORGANIZATIONS]:
            orgs = find_org_children(cur, org)
            org_q = '(' + ('?, ' * len(orgs))[:-2] + ')'
            if len(q) != 0:
                q += ' INTERSECT '
            q += 'SELECT DISTINCT paperid FROM p2org WHERE orgid IN ' + org_q
            formatted_ids.extend(orgs)
    if keys.withKeywords:
        for term in request.json[KEYWORDS]:
            if len(q) != 0:
                q += ' INTERSECT '
            q += 'SELECT DISTINCT paperid FROM pcount WHERE lower(term)=?'
            formatted_ids.append(term.lower())
    if keys.withGrants:
        if not isinstance(request.json[GRANTS], dict):
            return make_response('Invalid value in search requests. Check grants.', 400)
        elif not set(request.json[GRANTS].keys()).issubset([DATES, GRANT_LIST]):
            return make_response('Invalid value in search requests. Check grants.', 400)

        if GRANT_LIST in request.json[GRANTS].keys():
            for grant in request.json[GRANTS][GRANT_LIST]:
                if len(q) != 0:
                    q += ' INTERSECT '
                q += 'SELECT DISTINCT paperid FROM g2p WHERE grantid=?'
                formatted_ids.append(grant)
        if DATES in request.json[GRANTS].keys():
            if len(q) != 0:
                q += ' INTERSECT '
            q += 'SELECT DISTINCT paperid FROM g2p WHERE grantid IN (SELECT DISTINCT grantid FROM gdetails WHERE '
            if START in request.json[GRANTS][DATES].keys():
                if END in request.json[GRANTS][DATES].keys():
                    q += '(startdate BETWEEN ? AND ?) OR (enddate BETWEEN ? AND ?)'
                    formatted_ids.extend([request.json[GRANTS][DATES][START], request.json[GRANTS][DATES][END],
                                          request.json[GRANTS][DATES][START], request.json[GRANTS][DATES][END]])
                else:
                    q += 'startdate >= ? OR enddate >= ?'
                    formatted_ids.extend([request.json[GRANTS][DATES][START], request.json[GRANTS][DATES][START]])
            elif END in request.json[GRANTS][DATES].keys():
                q += 'startdate <= ? OR enddate <= ?'
                formatted_ids.extend([request.json[GRANTS][DATES][END], request.json[GRANTS][DATES][END]])
            q += ')'

    final_q = 'SELECT DISTINCT paperid, title FROM pdetails WHERE paperid IN (' + q + ')'
    print(('=' * 5) + 'query' + ('=' * 5) + '\n' + q)
    cur.execute(final_q, tuple(formatted_ids))
    rows = cur.fetchall()
    papers = [{'id': x['paperid'], 'name': x['title']} for x in rows]
    return make_response(jsonify(papers), 200)


@midas_blueprint.route('/intersection/grants/', methods=['POST'])
@swag_from('../swagger_docs/grantOverlap.yml')
def get_grant_list():
    grant_options = [PEOPLE, PAPERS, KEYWORDS, ORGANIZATIONS, GRANT_DATE_RANGE]
    errors = check_payload(request, grant_options)
    if errors is not None:
        return errors
    conn = connect_to_db()
    cur = conn.cursor()

    keys = get_categories_in_query(request.json.keys())

    q = ''
    formatted_ids = []
    if keys.withDates:
        q = 'SELECT DISTINCT grantid FROM gdetails WHERE '
        if START in request.json[GRANT_DATE_RANGE].keys():
            if END in request.json[GRANT_DATE_RANGE].keys():
                q += '(startdate BETWEEN ? AND ?) OR (enddate BETWEEN ? AND ?)'
                formatted_ids.extend([request.json[GRANT_DATE_RANGE][START], request.json[GRANT_DATE_RANGE][END],
                                      request.json[GRANT_DATE_RANGE][START], request.json[GRANT_DATE_RANGE][END]])
            else:
                q += 'startdate >= ? OR enddate >= ?'
                formatted_ids.extend([request.json[GRANT_DATE_RANGE][START], request.json[GRANT_DATE_RANGE][START]])
        elif END in request.json[GRANT_DATE_RANGE].keys():
            q += 'startdate <= ? OR enddate <= ?'
            formatted_ids.extend([request.json[GRANT_DATE_RANGE][END], request.json[GRANT_DATE_RANGE][END]])
    if keys.withPeople:
        for author in request.json[PEOPLE]:
            if len(q) != 0:
                q += ' INTERSECT '
            q += 'SELECT DISTINCT grantid FROM g2a WHERE authorid=?'
            formatted_ids.append(author)
    if keys.withOrgs:
        for org in request.json[ORGANIZATIONS]:
            orgs = find_org_children(cur, org)
            org_q = '(' + ('?, ' * len(orgs))[:-2] + ')'
            if len(q) != 0:
                q += ' INTERSECT '
            q += 'SELECT DISTINCT grantid FROM g2a a JOIN adetails b ON a.authorid=b.authorid WHERE orgid IN ' + org_q
            formatted_ids.extend(orgs)
    if keys.withKeywords:
        for term in request.json[KEYWORDS]:
            if len(q) != 0:
                q += ' INTERSECT '
            q += 'SELECT DISTINCT grantid FROM g2p a JOIN pcount b ON a.paperid=b.paperid WHERE lower(term)=?'
            formatted_ids.append(term.lower())
    if keys.withPapers:
        if not isinstance(request.json[PAPERS], dict):
            return make_response('Invalid value in search requests. Check papers.', 400)
        elif not set(request.json[PAPERS].keys()).issubset([DATES, 'paperList']):
            return make_response('Invalid value in search requests. Check papers.', 400)

        if 'paperList' in request.json[PAPERS].keys():
            for paper in request.json[PAPERS]['paperList']:
                if len(q) != 0:
                    q += ' INTERSECT '
                q += 'SELECT DISTINCT grantid FROM g2p WHERE paperid=?'
                formatted_ids.append(paper)
        if DATES in request.json[PAPERS].keys():
            if len(q) != 0:
                q += ' INTERSECT '
            q += 'SELECT DISTINCT grantid FROM g2p WHERE paperid IN (SELECT paperid FROM pdetails WHERE '
            if START in request.json[PAPERS][DATES].keys():
                if END in request.json[PAPERS][DATES].keys():
                    q += 'year BETWEEN ? AND ?'
                    formatted_ids.extend([request.json[PAPERS][DATES][START], request.json[PAPERS][DATES][END]])
                else:
                    q += 'year >= ?'
                    formatted_ids.extend([request.json[PAPERS][DATES][START]])
            elif END in request.json[PAPERS][DATES].keys():
                q += 'year <= ?'
                formatted_ids.extend([request.json[PAPERS][DATES][END]])
            q += ')'

    final_q = 'SELECT DISTINCT grantid, title FROM gdetails WHERE grantid IN (' + q + ')'
    print(('=' * 5) + 'query' + ('=' * 5) + '\n' + q)
    cur.execute(final_q, tuple(formatted_ids))
    rows = cur.fetchall()
    grants = [{'id': x['grantid'], 'name': x['title']} for x in rows]
    return make_response(jsonify(grants), 200)


@midas_blueprint.route('/intersection/people/', methods=['POST'])
@swag_from('../swagger_docs/peopleOverlap.yml')
def get_people_list():
    people_options = [PEOPLE, GRANTS, KEYWORDS, ORGANIZATIONS, PAPERS]
    errors = check_payload(request, people_options)
    if errors is not None:
        return errors
    print('##' * 20)
    conn = connect_to_db()
    cur = conn.cursor()

    keys = get_categories_in_query(request.json.keys())

    q = ''
    formatted_ids = []
    if keys.withAuthors:
        for author in request.json[PEOPLE]:
            if len(q) != 0:
                q += ' INTERSECT '
            q += 'SELECT DISTINCT authorid FROM p2au WHERE paperid IN (SELECT DISTINCT paperid FROM p2au WHERE authorid=?)'
            formatted_ids.append(author)
    if keys.withPapers:
        if not isinstance(request.json[PAPERS], dict):
            return make_response('Invalid value in search requests. Check papers.', 400)
        elif not set(request.json[PAPERS].keys()).issubset([DATES, 'paperList']):
            return make_response('Invalid value in search requests. Check papers.', 400)

        if 'paperList' in request.json[PAPERS].keys():
            for paper in request.json[PAPERS]['paperList']:
                if len(q) != 0:
                    q += ' INTERSECT '
                q += 'SELECT DISTINCT authorid FROM p2au WHERE paperid=?'
                formatted_ids.append(paper)
        if DATES in request.json[PAPERS].keys():
            if len(q) != 0:
                q += ' INTERSECT '
            q += 'SELECT DISTINCT authorid FROM p2au WHERE paperid IN (SELECT paperid FROM pdetails WHERE '
            if START in request.json[PAPERS][DATES].keys():
                if END in request.json[PAPERS][DATES].keys():
                    q += 'year BETWEEN ? AND ?'
                    formatted_ids.extend([request.json[PAPERS][DATES][START], request.json[PAPERS][DATES][END]])
                else:
                    q += 'year >= ?'
                    formatted_ids.extend([request.json[PAPERS][DATES][START]])
            elif END in request.json[PAPERS][DATES].keys():
                q += 'year <= ?'
                formatted_ids.extend([request.json[PAPERS][DATES][END]])
            q += ')'
    if keys.withOrg:
        orgs = find_org_children(cur, request.json[ORGANIZATIONS])
        org_q = '(' + ('?, ' * len(orgs))[:-2] + ')'
        if len(q) != 0:
            q += ' INTERSECT '
        q += 'SELECT DISTINCT authorid FROM adetails WHERE orgid IN ' + org_q
        formatted_ids.extend(orgs)
    if keys.withKeywords:
        for term in request.json[KEYWORDS]:
            if len(q) != 0:
                q += ' INTERSECT '
            q += 'SELECT DISTINCT authorid FROM p2au WHERE paperid IN (SELECT DISTINCT paperid FROM pcount WHERE lower(term)=?)'
            formatted_ids.append(term.lower())
    if keys.withGrants:
        if not isinstance(request.json[GRANTS], dict):
            return make_response('Invalid value in search requests. Check grants.', 400)
        elif not set(request.json[GRANTS].keys()).issubset([DATES, GRANT_LIST]):
            return make_response('Invalid value in search requests. Check grants.', 400)

        if GRANT_LIST in request.json[GRANTS].keys():
            for grant in request.json[GRANTS][GRANT_LIST]:
                if len(q) != 0:
                    q += ' INTERSECT '
                q += 'SELECT DISTINCT authorid FROM g2a WHERE grantid=?'
                formatted_ids.append(grant)
        if DATES in request.json[GRANTS].keys():
            if len(q) != 0:
                q += ' INTERSECT '
            q += 'SELECT DISTINCT authorid FROM g2a WHERE grantid IN (SELECT DISTINCT grantid FROM gdetails WHERE '
            if START in request.json[GRANTS][DATES].keys():
                if END in request.json[GRANTS][DATES].keys():
                    q += '(startdate BETWEEN ? AND ?) OR (enddate BETWEEN ? AND ?)'
                    formatted_ids.extend([request.json[GRANTS][DATES][START], request.json[GRANTS][DATES][END],
                                          request.json[GRANTS][DATES][START], request.json[GRANTS][DATES][END]])
                else:
                    q += 'startdate >= ? OR enddate >= ?'
                    formatted_ids.extend([request.json[GRANTS][DATES][START], request.json[GRANTS][DATES][START]])
            elif END in request.json[GRANTS][DATES].keys():
                q += 'startdate <= ? OR enddate <= ?'
                formatted_ids.extend([request.json[GRANTS][DATES][END], request.json[GRANTS][DATES][END]])
            q += ')'

    final_q = 'SELECT DISTINCT authorid, author_name FROM adetails WHERE authorid IN (' + q + ')'
    print(('=' * 5) + 'query' + ('=' * 5) + '\n' + q)
    cur.execute(final_q, tuple(formatted_ids))
    rows = cur.fetchall()
    people = [{'id': x['authorid'], 'name': x['author_name']} for x in rows]
    return make_response(jsonify(people), 200)


@midas_blueprint.route('/intersection/organizations/', methods=['POST'])
@swag_from('../swagger_docs/orgOverlap.yml')
def get_org_list():
    org_options = [PEOPLE, GRANTS, KEYWORDS, PAPERS]
    errors = check_payload(request, org_options)
    if errors is not None:
        return errors

    conn = connect_to_db()
    cur = conn.cursor()

    keys = get_categories_in_query(request.json.keys())
    q = ''
    formatted_ids = []
    if keys.withPerson:
        q += 'SELECT DISTINCT orgid FROM adetails WHERE authorid=?'
        formatted_ids.append(request.json[PEOPLE])
    if keys.withKeywords:
        for term in request.json[KEYWORDS]:
            if len(q) != 0:
                q += ' INTERSECT '
            q += 'SELECT DISTINCT orgid FROM p2org WHERE paperid IN (SELECT DISTINCT paperid FROM pcount WHERE lower(term)=?)'
            formatted_ids.append(term.lower())
    if keys.withGrants:
        if not isinstance(request.json[GRANTS], dict):
            return make_response('Invalid value in search requests. Check grants.', 400)
        elif not set(request.json[GRANTS].keys()).issubset([DATES, GRANT_LIST]):
            return make_response('Invalid value in search requests. Check grants.', 400)

        if GRANT_LIST in request.json[GRANTS].keys():
            for grant in request.json[GRANTS][GRANT_LIST]:
                if len(q) != 0:
                    q += ' INTERSECT '
                q += 'SELECT DISTINCT orgid FROM adetails WHERE authorid IN (SELECT DISTINCT authorid FROM g2a WHERE grantid=?)'
                formatted_ids.append(grant)
        if DATES in request.json[GRANTS].keys():
            if len(q) != 0:
                q += ' INTERSECT '
            q += 'SELECT DISTINCT orgid FROM adetails WHERE authorid IN (SELECT DISTINCT authorid FROM g2a WHERE grantid IN (SELECT DISTINCT grantid FROM gdetails WHERE '
            if START in request.json[GRANTS][DATES].keys():
                if END in request.json[GRANTS][DATES].keys():
                    q += '(startdate BETWEEN ? AND ?) OR (enddate BETWEEN ? AND ?)'
                    formatted_ids.extend([request.json[GRANTS][DATES][START], request.json[GRANTS][DATES][END],
                                          request.json[GRANTS][DATES][START], request.json[GRANTS][DATES][END]])
                else:
                    q += 'startdate >= ? OR enddate >= ?'
                    formatted_ids.extend([request.json[GRANTS][DATES][START], request.json[GRANTS][DATES][START]])
            elif END in request.json[GRANTS][DATES].keys():
                q += 'startdate <= ? OR enddate <= ?'
                formatted_ids.extend([request.json[GRANTS][DATES][END], request.json[GRANTS][DATES][END]])
            q += '))'
    if keys.withPapers:
        if not isinstance(request.json[PAPERS], dict):
            return make_response('Invalid value in search requests. Check papers.', 400)
        elif not set(request.json[PAPERS].keys()).issubset([DATES, 'paperList']):
            return make_response('Invalid value in search requests. Check papers.', 400)

        if 'paperList' in request.json[PAPERS].keys():
            for paper in request.json[PAPERS]['paperList']:
                if len(q) != 0:
                    q += ' INTERSECT '
                q += 'SELECT DISTINCT orgid FROM p2org WHERE paperid=?'
                formatted_ids.append(paper)
        if DATES in request.json[PAPERS].keys():
            if len(q) != 0:
                q += ' INTERSECT '
            q += 'SELECT DISTINCT orgid FROM p2org WHERE paperid IN (SELECT paperid FROM pdetails WHERE '
            if START in request.json[PAPERS][DATES].keys():
                if END in request.json[PAPERS][DATES].keys():
                    q += 'year BETWEEN ? AND ?'
                    formatted_ids.extend([request.json[PAPERS][DATES][START], request.json[PAPERS][DATES][END]])
                else:
                    q += 'year >= ?'
                    formatted_ids.extend([request.json[PAPERS][DATES][START]])
            elif END in request.json[PAPERS][DATES].keys():
                q += 'year <= ?'
                formatted_ids.extend([request.json[PAPERS][DATES][END]])
            q += ')'

    parent_q = 'SELECT DISTINCT rel_id FROM org_relations WHERE orgid IN (' + q + ')'
    final_q = 'SELECT DISTINCT orgid, org_name FROM odetails WHERE orgid IN (' + parent_q + ')'
    print(('=' * 5) + 'query' + ('=' * 5) + '\n' + q)
    cur.execute(final_q, tuple(formatted_ids))
    rows = cur.fetchall()
    orgs = [{'id': x['orgid'], 'name': x['org_name']} for x in rows]
    return make_response(jsonify(orgs), 200)
