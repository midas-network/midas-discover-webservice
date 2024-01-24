import sqlite3
import traceback

from flask import request

from project.queries import DB_LOCATION
from .constants import withPeople, withOrgs, withKeywords, withPapers, withDates, withGrants, PEOPLE, ORGANIZATIONS, \
    KEYWORDS, PAPERS, GRANT_DATE_RANGE, GRANTS, START, END, DATES
from .errorchecking import check_payload


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
        withOrgs: False,  # Not in PowerPoint but seems doable
        withKeywords: False,
        withPapers: False,
        withDates: False,
        withGrants: False,
    }

    if PEOPLE in keys:
        result[withPeople] = True
    if ORGANIZATIONS in keys:
        result[withOrgs] = True
    if KEYWORDS in keys:
        result[withKeywords] = True
    if PAPERS in keys:
        result[withPapers] = True
    if GRANT_DATE_RANGE in keys:
        result[withDates] = True
    if GRANTS in keys:
        result[withGrants] = True

    return result


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


def handle_grants_dates(request_payload, q, formatted_ids):
    if START in request_payload.json[GRANTS][DATES].keys():
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
    return q


def init_endpoint(request_payload, options, category, sub_list):
    errors = check_payload(request_payload, options, category, sub_list)
    conn = connect_to_db()
    cur = conn.cursor()
    keys = get_categories_in_query(request.json.keys())
    q = ''
    formatted_ids = []
    return [q, formatted_ids, cur, keys, errors]
