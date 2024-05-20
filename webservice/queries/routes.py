from flasgger import swag_from
from flask import jsonify, request, make_response
from . import midas_blueprint
from .constants import withPeople, withOrgs, withKeywords, withPapers, withDates, withGrants, PEOPLE, ORGANIZATIONS, \
    KEYWORDS, PAPERS, GRANT_DATE_RANGE, GRANTS, PUBLICATION_DATE_RANGE, START, END, GRANT_LIST, DATES, PAPER_LIST
from .errorchecking import check_payload
from .utils import connect_to_db, get_full_paper_list, get_full_org_list, get_full_people_list, get_full_grant_list, \
    get_full_keyword_list, find_org_children, handle_grants_dates, init_endpoint


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


@midas_blueprint.route('/orgHierarchy/', methods=['GET'])
def get_parent_orgs():
    conn = connect_to_db()
    cur = conn.cursor()
    
    q = 'SELECT DISTINCT A.orgid, A.top_level, B.org_name FROM odetails A JOIN odetails B ON A.top_level=B.orgid'
    cur.execute(q)
    rows = cur.fetchall()
    org_parents = {x['orgid']: {'parent': x['top_level'], 'parent_name': x['org_name']} for x in rows}
    return make_response(jsonify(org_parents), 200)


@midas_blueprint.route('/intersection/papers/', methods=['POST'])
@swag_from('../swagger_docs/paperOverlap.yml')
def get_paper_list():
    paper_options = [PEOPLE, GRANTS, KEYWORDS, ORGANIZATIONS, PAPERS, PUBLICATION_DATE_RANGE]
    [q, formatted_ids, cur, keys, errors] = init_endpoint(request, paper_options, None, None)
    if errors is not None:
        return errors

    if keys[withDates]:
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
    if keys[withPapers]:
        check_payload(request, None, PAPERS, PAPER_LIST)
        if PAPER_LIST in request.json[PAPERS].keys():
            for paper in request.json[PAPERS][PAPER_LIST]:
                if len(q) != 0:
                    q += ' INTERSECT '
                q += 'SELECT DISTINCT paperid from pdetails WHERE paperid=?'
                formatted_ids.append(paper) 
    if keys[withPeople]:
        for author in request.json[PEOPLE]:
            if len(q) != 0:
                q += ' INTERSECT '
            q += 'SELECT DISTINCT paperid FROM p2au WHERE authorid=?'
            formatted_ids.append(author)
    if keys[withOrgs]:
        for org in request.json[ORGANIZATIONS]:
            orgs = find_org_children(cur, org)
            org_q = '(' + ('?, ' * len(orgs))[:-2] + ')'
            if len(q) != 0:
                q += ' INTERSECT '
            q += 'SELECT DISTINCT paperid FROM p2org WHERE orgid IN ' + org_q
            formatted_ids.extend(orgs)
    if keys[withKeywords]:
        for term in request.json[KEYWORDS]:
            if len(q) != 0:
                q += ' INTERSECT '
            q += 'SELECT DISTINCT paperid FROM pcount WHERE lower(term)=?'
            formatted_ids.append(term.lower())
    if keys[withGrants]:
        check_payload(request, None, GRANTS, GRANT_LIST)

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
            q = handle_grants_dates(request, q, formatted_ids)
            q += ')'

    final_q = 'SELECT DISTINCT paperid, title FROM pdetails WHERE paperid IN (' + q + ')'
    print(('=' * 5) + 'query' + ('=' * 5) + '\n' + q)
    cur.execute(final_q, tuple(formatted_ids))
    rows = cur.fetchall()
    papers = [{'id': x['paperid'], 'name': x['title']} for x in rows]
    return make_response(jsonify(papers), 200)


def handle_papers_dates(request_payload, q, formatted_ids):
    if START in request_payload.json[PAPERS][DATES].keys():
        if END in request.json[PAPERS][DATES].keys():
            q += 'year BETWEEN ? AND ?'
            formatted_ids.extend([request.json[PAPERS][DATES][START], request.json[PAPERS][DATES][END]])
        else:
            q += 'year >= ?'
            formatted_ids.extend([request.json[PAPERS][DATES][START]])
    elif END in request.json[PAPERS][DATES].keys():
        q += 'year <= ?'
        formatted_ids.extend([request.json[PAPERS][DATES][END]])
    return q


@midas_blueprint.route('/intersection/grants/', methods=['POST'])
@swag_from('../swagger_docs/grantOverlap.yml')
def get_grant_list():
    grant_options = [PEOPLE, PAPERS, KEYWORDS, ORGANIZATIONS, GRANT_DATE_RANGE]
    [q, formatted_ids, cur, keys, errors] = init_endpoint(request, grant_options, None, None)
    if errors is not None:
        return errors
    if keys[withDates]:
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
    if keys[withPeople]:
        for author in request.json[PEOPLE]:
            if len(q) != 0:
                q += ' INTERSECT '
            q += 'SELECT DISTINCT grantid FROM g2a WHERE authorid=?'
            formatted_ids.append(author)
    if keys[withOrgs]:
        for org in request.json[ORGANIZATIONS]:
            orgs = find_org_children(cur, org)
            org_q = '(' + ('?, ' * len(orgs))[:-2] + ')'
            if len(q) != 0:
                q += ' INTERSECT '
            q += 'SELECT DISTINCT grantid FROM g2a a JOIN adetails b ON a.authorid=b.authorid WHERE orgid IN ' + org_q
            formatted_ids.extend(orgs)
    if keys[withKeywords]:
        for term in request.json[KEYWORDS]:
            if len(q) != 0:
                q += ' INTERSECT '
            q += 'SELECT DISTINCT grantid FROM g2p a JOIN pcount b ON a.paperid=b.paperid WHERE lower(term)=?'
            formatted_ids.append(term.lower())
    if keys[withPapers]:
        check_payload(request, None, PAPERS, PAPER_LIST)
        if PAPER_LIST in request.json[PAPERS].keys():
            for paper in request.json[PAPERS][PAPER_LIST]:
                if len(q) != 0:
                    q += ' INTERSECT '
                q += 'SELECT DISTINCT grantid FROM g2p WHERE paperid=?'
                formatted_ids.append(paper)
        if DATES in request.json[PAPERS].keys():
            if len(q) != 0:
                q += ' INTERSECT '
            q += 'SELECT DISTINCT grantid FROM g2p WHERE paperid IN (SELECT paperid FROM pdetails WHERE '
            q = handle_papers_dates(request, q, formatted_ids)
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
    [q, formatted_ids, cur, keys, errors] = init_endpoint(request, people_options, None, None)
    if errors is not None:
        return errors
    if ORGANIZATIONS in request.json and len(request.json[ORGANIZATIONS]) > 1:
        return make_response("There can only be one organization listed in the body of this request: ", 400)
    if keys[withPeople]:
        for author in request.json[PEOPLE]:
            if len(q) != 0:
                q += ' INTERSECT '
            q += ('SELECT DISTINCT authorid FROM p2au WHERE paperid IN  (SELECT DISTINCT paperid FROM p2au WHERE '
                  'authorid=?)')
            formatted_ids.append(author)
    if keys[withPapers]:
        check_payload(request, None, PAPERS, PAPER_LIST)
        if PAPER_LIST in request.json[PAPERS].keys():
            for paper in request.json[PAPERS][PAPER_LIST]:
                if len(q) != 0:
                    q += ' INTERSECT '
                q += 'SELECT DISTINCT authorid FROM p2au WHERE paperid=?'
                formatted_ids.append(paper)
        if DATES in request.json[PAPERS].keys():
            if len(q) != 0:
                q += ' INTERSECT '
            q += 'SELECT DISTINCT authorid FROM p2au WHERE paperid IN (SELECT paperid FROM pdetails WHERE '
            q = handle_papers_dates(request, q, formatted_ids)
            q += ')'
    if keys[withOrgs]:
        for org in request.json[ORGANIZATIONS]:
            orgs = find_org_children(cur, org)
            org_q = '(' + ('?, ' * len(orgs))[:-2] + ')'
            if len(q) != 0:
                q += ' INTERSECT '
            q += 'SELECT DISTINCT authorid FROM adetails WHERE orgid IN ' + org_q
            formatted_ids.extend(orgs)
    if keys[withKeywords]:
        for term in request.json[KEYWORDS]:
            if len(q) != 0:
                q += ' INTERSECT '
            q += ('SELECT DISTINCT authorid FROM p2au WHERE paperid IN (SELECT DISTINCT paperid FROM pcount WHERE '
                  'lower(term)=?)')
            formatted_ids.append(term.lower())
    if keys[withGrants]:
        check_payload(request, None, GRANTS, GRANT_LIST)
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
            q = handle_grants_dates(request, q, formatted_ids)
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
    [q, formatted_ids, cur, keys, errors] = init_endpoint(request, org_options, None, None)
    if errors is not None:
        return errors
    if keys[withPeople]:
        # TODO: fix
        people_arr = request.json[PEOPLE]
        people_arr_w_quotes = []
        for person in people_arr:
            people_arr_w_quotes.append("'" + person + "'")
        q += 'SELECT DISTINCT orgid FROM adetails WHERE authorid IN (' + ','.join(people_arr_w_quotes) + ')'
        print("query is" + q)
    if keys[withKeywords]:
        for term in request.json[KEYWORDS]:
            if len(q) != 0:
                q += ' INTERSECT '
            q += ('SELECT DISTINCT orgid FROM p2org WHERE paperid IN (SELECT DISTINCT paperid FROM pcount WHERE lower('
                  'term)=?)')
            formatted_ids.append(term.lower())
    if keys[withGrants]:
        check_payload(request, None, GRANTS, GRANT_LIST)

        if GRANT_LIST in request.json[GRANTS].keys():
            for grant in request.json[GRANTS][GRANT_LIST]:
                if len(q) != 0:
                    q += ' INTERSECT '
                q += ('SELECT DISTINCT orgid FROM adetails WHERE authorid IN (SELECT DISTINCT authorid FROM g2a WHERE '
                      'grantid=?)')
                formatted_ids.append(grant)
        if DATES in request.json[GRANTS].keys():
            if len(q) != 0:
                q += ' INTERSECT '
            q += ('SELECT DISTINCT orgid FROM adetails WHERE authorid IN (SELECT DISTINCT authorid FROM g2a WHERE '
                  'grantid IN (SELECT DISTINCT grantid FROM gdetails WHERE')
            q = handle_grants_dates(request, q, formatted_ids)
            q += '))'
    if keys[withPapers]:
        check_payload(request, None, PAPERS, PAPER_LIST)
        if PAPER_LIST in request.json[PAPERS].keys():
            for paper in request.json[PAPERS][PAPER_LIST]:
                if len(q) != 0:
                    q += ' INTERSECT '
                q += 'SELECT DISTINCT orgid FROM p2org WHERE paperid=?'
                formatted_ids.append(paper)
        if DATES in request.json[PAPERS].keys():
            if len(q) != 0:
                q += ' INTERSECT '
            q += 'SELECT DISTINCT orgid FROM p2org WHERE paperid IN (SELECT paperid FROM pdetails WHERE '
            q = handle_papers_dates(request, q, formatted_ids)
            q += ')'

    parent_q = 'SELECT DISTINCT rel_id FROM org_relations WHERE orgid IN (' + q + ')'
    final_q = 'SELECT DISTINCT orgid, org_name FROM odetails WHERE orgid IN (' + parent_q + ')'
    print(('=' * 5) + 'query' + ('=' * 5) + '\n' + q)
    cur.execute(final_q, tuple(formatted_ids))
    rows = cur.fetchall()
    orgs = [{'id': x['orgid'], 'name': x['org_name']} for x in rows]
    return make_response(jsonify(orgs), 200)

@midas_blueprint.route('/intersection/keywords/', methods=['POST'])
@swag_from('../swagger_docs/keywordOverlap.yml')
def get_keyword_list():
    keyword_options = [PEOPLE, GRANTS, ORGANIZATIONS, PAPERS]
    [q, formatted_ids, cur, keys, errors] = init_endpoint(request, keyword_options, None, None)
    if errors is not None:
        return errors
    if keys[withPeople]:
        for person in request.json[PEOPLE]:
            if len(q) != 0:
                q += ' INTERSECT '
            q += 'SELECT DISTINCT term FROM pcount WHERE paperid IN (SELECT DISTINCT paperid FROM p2au WHERE authorid=?)'
            formatted_ids.append(person)
    if keys[withGrants]:
        check_payload(request, None, GRANTS, GRANT_LIST)
        if GRANT_LIST in request.json[GRANTS].keys():
            for grant in request.json[GRANTS][GRANT_LIST]:
                if len(q) != 0:
                    q += ' INTERSECT '
                q += ('SELECT DISTINCT term FROM pcount WHERE paperid IN (SELECT DISTINCT paperid FROM g2p WHERE '
                      'grantid=?)')
                formatted_ids.append(grant)
        if DATES in request.json[GRANTS].keys():
            if len(q) != 0:
                q += ' INTERSECT '
            q += ('SELECT DISTINCT term FROM pcount WHERE paperid IN (SELECT DISTINCT paperid FROM g2p WHERE '
                  'grantid IN (SELECT DISTINCT grantid FROM gdetails WHERE')
            q = handle_grants_dates(request, q, formatted_ids)
            q += '))'
    if keys[withPapers]:
        check_payload(request, None, PAPERS, PAPER_LIST)
        if PAPER_LIST in request.json[PAPERS].keys():
            for paper in request.json[PAPERS][PAPER_LIST]:
                if len(q) != 0:
                    q += ' INTERSECT '
                q += 'SELECT DISTINCT term FROM pcount WHERE paperid=?'
                formatted_ids.append(paper)
        if DATES in request.json[PAPERS].keys():
            if len(q) != 0:
                q += ' INTERSECT '
            q += 'SELECT DISTINCT term FROM pcount WHERE paperid IN (SELECT paperid FROM pdetails WHERE '
            q = handle_papers_dates(request, q, formatted_ids)
            q += ')'

    print(('=' * 5) + 'query' + ('=' * 5) + '\n' + q)
    cur.execute(q, tuple(formatted_ids))
    rows = cur.fetchall()
    terms = [x['term'] for x in rows]
    return make_response(terms, 200)
