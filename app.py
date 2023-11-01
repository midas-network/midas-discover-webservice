from flask import Flask
from flask import jsonify, request, make_response

import sqlite3

'''
Tables
org_relations: orgid,rel_id,rel_type
p2au: paperid,authorid
p2org: paperid,orgid
p2s: paperid,Department,School,University,NotSpecified,Laboratory,Institute,Center,College,Division,GovernmentAgency,Hospital,Program
pcount: paperid,year,term,count,ngram,field
pdetails: paperid,title,abstract
'''

app = Flask(__name__)

## TODO: Make keys in response smaller (e.g. title to t)/use constants in River

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

def get_paper_list(request, conn):
    cur = conn.cursor()

    withAuthors = False
    withOrgs = False
    withTerms = False
    withDates = False

    if 'authors' in request.json.keys():
        withAuthors = True
    if 'orgs' in request.json.keys():
        withOrgs = True
    if 'terms' in request.json.keys():
        withTerms = True
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
                q += ' AND paperid IN (SELECT DISTINCT paperid FROM pcount WHERE year BETWEEN ? AND ?)'
                formatted_ids.append(request.json['dates']['start'])
                formatted_ids.append(request.json['dates']['end'])
    if withOrgs:
        for org in request.json['orgs']:
            if len(q) != 0:
                q += ' INTERSECT '
            q += 'SELECT DISTINCT paperid FROM p2org WHERE orgid=?'
            formatted_ids.append(org)

            if withDates:
                q += ' AND paperid IN (SELECT DISTINCT paperid FROM pcount WHERE year BETWEEN ? AND ?)'
                formatted_ids.append(request.json['dates']['start'])
                formatted_ids.append(request.json['dates']['end'])
    if withTerms:
        for term in request.json['terms']:
            if len(q) != 0:
                q += ' INTERSECT '
            q += 'SELECT DISTINCT paperid FROM pcount WHERE term=?'
            formatted_ids.append(term)

            if withDates:
                q += ' AND paperid IN (SELECT DISTINCT paperid FROM pcount WHERE year BETWEEN ? AND ?)'
                formatted_ids.append(request.json['dates']['start'])
                formatted_ids.append(request.json['dates']['end'])
    
    cur.execute(q, tuple(formatted_ids))
    rows = cur.fetchall()
    papers = [x['paperid'] for x in rows]
    return papers

    
def get_paper_response(papers, conn):
    curs = conn.cursor()
    q = 'SELECT DISTINCT paperid, abstract, title from pdetails WHERE paperid in ({ppr_seq})'.format(ppr_seq=','.join(['?']*len(papers)))
    curs.execute(q, papers)
    rows = curs.fetchall()

    res = [{'title': row['title'], 'uri': row['paperid'], 'abstract': row['abstract']} for row in rows]
    return res

# How to handle ngrams- only one service that can take specific ngram count or return all for term? -- Optional argument
@app.route('/termlist/<string:term_type>/', methods=['GET'])
def get_terms(term_type):
    conn = connect_to_db()
    cur = conn.cursor()

    q = "SELECT DISTINCT term FROM pcount WHERE field=?"
    cur.execute(q, (term_type,))
    rows = cur.fetchall()
    res = [x['term'] for x in rows]

    return make_response(jsonify(res), 200)


@app.route('/getpapers/overlap/', methods=['GET'])
def get_paper():
    conn = connect_to_db()

    papers = get_paper_list(request, conn)
    res = get_paper_response(papers, conn)
    return make_response(jsonify(res), 200)

@app.route('/getauthors/overlap/', methods=['GET'])
def get_authors():
    conn = connect_to_db()
    cur = conn.cursor()

    papers = get_paper_list(request, conn)

    q2 = 'SELECT DISTINCT authorid FROM p2au WHERE paperid IN ({ppr_seq})'.format(ppr_seq=','.join(['?']*len(papers)))
    cur.execute(q2, papers)
    rows = cur.fetchall()
    authors = [x['authorid'] for x in rows]
    
    return make_response(jsonify(authors), 200)

@app.route('/getorgs/overlap/', methods=['GET'])
def get_orgs():
    conn = connect_to_db()
    cur = conn.cursor()

    papers = get_paper_list(request, conn)

    q2 = 'SELECT DISTINCT orgid FROM p2org WHERE paperid IN ({ppr_seq})'.format(ppr_seq=','.join(['?']*len(papers)))
    cur.execute(q2, papers)
    rows = cur.fetchall()
    orgs = [x['orgid'] for x in rows]
    
    return make_response(jsonify(orgs), 200)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=105)



# ########################################################################################################


# @app.route('/author/<string:author>/', methods=['GET'])
# def get_auth_papers(author):
#     conn = connect_to_db()
#     cur = conn.cursor()

#     import pdb;pdb.set_trace()
#     q = "SELECT DISTINCT a.paperid, a.abstract, a.title FROM pdetails a, p2au b WHERE a.paperid=b.paperid AND b.authorid=?"
#     cur.execute(q,(author_prefix + author + '/',))
#     rows = cur.fetchall()
#     res = [{'title': row['title'], 'uri': row['paperid'], 'abstract': row['abstract']} for row in rows]

#     return make_response(jsonify(res), 200)

# @app.route('/org/<string:org>/', methods=['GET'])
# def get_org_papers(org):
#     conn = connect_to_db()
#     cur = conn.cursor()

#     q1 = "SELECT rel_type FROM org_relations WHERE rel_id=? LIMIT 1"
#     cur.execute(q1,(org_prefix + org,))
#     rows1 = cur.fetchall()
#     rel_type = rows1[0]['rel_type'].split('#')[1]
#     if rel_type in ('Department','School','University','NotSpecified','Laboratory','Institute','Center','College','Division','GovernmentAgency','Hospital','Program'):
#         q2 = f"SELECT DISTINCT a.paperid, a.abstract, a.title FROM pdetails a, p2s b WHERE a.paperid=b.paperid AND b.{rel_type}=?"
#         cur.execute(q2,(org_prefix + org,))
#         rows2 = cur.fetchall()
#         res = [{'title': row['title'], 'uri': row['paperid'], 'abstract': row['abstract']} for row in rows2]

#         res = make_response(jsonify(res), 200)
#     else:
#         res = make_response('Organization type not found. This might mean the organization is not in the database.', 400)
    
#     return res

# # termoverlap?term=<term1>&term=<term2>
# @app.route('/termoverlap', methods=['GET'])
# def get_term_overlap():
#     conn = connect_to_db()
#     cur = conn.cursor()

#     # search_terms = tuple(request.args.getlist('term'))
#     search_terms = tuple(request.json('terms'))
#     q_papers = f"SELECT DISTINCT paperid FROM pcount WHERE term IN {format(search_terms)}"
#     cur.execute(q_papers)
#     rows = cur.fetchall()
#     papers = [x['paperid'] for x in rows]

#     q_authors = f"SELECT DISTINCT authorid FROM p2au WHERE paperid IN {format(tuple(papers))}"
#     cur.execute(q_authors)
#     rows = cur.fetchall()
#     authors = [x['authorid'] for x in rows]

#     ## How to handle orgs? Do we need a p2au equivalent for orgs or do we check every possile column in p2s?

#     return make_response(jsonify({'papers': papers, 'authors': authors, 'orgs': ''}), 200)

# # authoroverlap?author=<author1>&author=<author2>
# @app.route('/authoroverlap', methods=['GET'])
# def get_author_overlap():
#     conn = connect_to_db()
#     cur = conn.cursor()

#     search_authors = request.args.getlist('author')

#     # get paper_id where author_id = ? and paper_id in (select paper_id where author_id=?2)

# ########################################################################################################

# def get_paper_response(papers, conn):
#     curs = conn.cursor()
#     paper_response = []
#     for paper in papers:
#         q = "SELECT DISTINCT a.abstract, b.paper_title FROM p2ab a JOIN pcount b ON a.paperid=b.paper_uri WHERE a.paperid='" + paper + "'"
#         curs.execute(q)
#         rows = curs.fetchall()
#         try:
#             paper_response.append({'title': rows[0][1], 'uri': paper, 'abstract': rows[0][0]})
#         except IndexError:
#             pass
#     return paper_response

# @app.route('/words/<string:term_type>/<int:ngram>/')
# def get_words(term_type, ngram):
#     return 'type: ' + term_type + '\nngram: ' + str(ngram)

# @app.route('/auth/<string:author>/', methods=['GET'])
# def get_auth_papes(author):
#     conn = connect_to_db()
#     cur = conn.cursor()
#     q = "SELECT * FROM p2au where authorid='https://midasnetwork.us/people/" + author + "/'"
#     cur.execute(q)
#     # q = "SELECT DISTINCT a.paperid, a.abstract, a.paper_title FROM p2ab a, p2au b WHERE a.paperid=b.paperid AND b.authorid='?'"
#     # cur.execute(q,author)
#     ## If we need multiple variables, use cur.executemany()
#     print(q)
#     rows = cur.fetchall()
#     import pdb;pdb.set_trace()
#     papers = [paper['paperid'] for paper in rows]
#     paper_response = get_paper_response(papers, conn)
#     return jsonify(paper_response)

# @app.route('/')
# def search_terms():
#     terms = request.args.getlist("terms")
#     return 'test \n' + ' '.join(terms)

# if __name__ == '__main__':
#     app.run(host='0.0.0.0', port=105)



#     # org1: pap1, pap2, pap4, pap5
#     # org2: pap1, pap3, pap4, pap7

#     # union: 1,2,3,4,5,7
#     # get paperid where orgid in (org1, org2)

#     # orgs = request.args.getlist('org')
#     # q = f"SELECT paperid FROM p2org WHERE orgid IN ({', '.join(['?']*len(apps))})", orgs





#     # overlap: 1,4
#     # get paperid where orgid=1 and paperid in (get paperid where orgid=2)
#     # select p1.paperid from p2org p1, p2org p2 where p1.paperid=p2.paperid and p1.orgid != p2.orgid and p1.orgid in (org_list) and p2.orgid in (org_list)
#     # SELECT paperid FROM p2org WHERE orgid=org1 INTERSECT SELECT paperid FROM p2org WHERE orgid=org2

