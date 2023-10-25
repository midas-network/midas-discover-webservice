from flask import Flask
from flask import jsonify
from flask import request

import sqlite3

'''
Tables
org_relations: orgid,rel_id,rel_type
p2au: paperid,authorid
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

# How to handle ngrams- only one service that can take specific ngram count or return all for term?
@app.route('/termlist/<string:term_type>/', methods=['GET'])
def get_terms(term_type):
    conn = connect_to_db()
    cur = conn.cursor()

    q = "SELECT DISTINCT term FROM pcount WHERE field=?"
    cur.execute(q, (term_type,))
    rows = cur.fetchall()
    res = [x['term'] for x in rows]

    return jsonify(res)

@app.route('/author/<string:author>/', methods=['GET'])
def get_auth_papers(author):
    conn = connect_to_db()
    cur = conn.cursor()

    q = "SELECT DISTINCT a.paperid, a.abstract, a.title FROM pdetails a, p2au b WHERE a.paperid=b.paperid AND b.authorid=?"
    cur.execute(q,(author_prefix + author + '/',))
    rows = cur.fetchall()
    res = [{'title': row['title'], 'uri': row['paperid'], 'abstract': row['abstract']} for row in rows]

    return jsonify(res)

@app.route('/org/<string:org>/', methods=['GET'])
def get_org_papers(org):
    conn = connect_to_db()
    cur = conn.cursor()

    q1 = "SELECT rel_type FROM org_relations WHERE rel_id=? LIMIT 1"
    cur.execute(q1,(org_prefix + org,))
    rows1 = cur.fetchall()
    rel_type = rows1[0]['rel_type'].split('#')[1]
    # if rel_type in ('Department','School','University','NotSpecified','Laboratory','Institute','Center','College','Division','GovernmentAgency','Hospital','Program'):
    q2 = f"SELECT DISTINCT a.paperid, a.abstract, a.title FROM pdetails a, p2s b WHERE a.paperid=b.paperid AND b.{rel_type}=?"
    cur.execute(q2,(org_prefix + org,))
    rows2 = cur.fetchall()
    res = [{'title': row['title'], 'uri': row['paperid'], 'abstract': row['abstract']} for row in rows2]

    return jsonify(res)

# termoverlap?term=<term1>&term=<term2>
@app.route('/termoverlap', methods=['GET'])
def get_term_overlap():
    conn = connect_to_db()
    cur = conn.cursor()

    search_terms = tuple(request.args.getlist('term'))
    q_papers = f"SELECT DISTINCT paperid FROM pcount WHERE term IN {format(search_terms)}"
    cur.execute(q_papers)
    rows = cur.fetchall()
    papers = tuple([x['paperid'] for x in rows])

    q_authors = f"SELECT DISTINCT authorid FROM p2au WHERE paperid IN {format(papers)}"
    cur.execute(q_authors)
    rows = cur.fetchall()
    authors = [x['authorid'] for x in rows]

    ## How to handle orgs? Do we need a p2au equivalent for orgs or do we check every possile column in p2s?

    return jsonify({'authors': authors, 'orgs': ''})










def get_paper_response(papers, conn):
    curs = conn.cursor()
    paper_response = []
    for paper in papers:
        q = "SELECT DISTINCT a.abstract, b.paper_title FROM p2ab a JOIN pcount b ON a.paperid=b.paper_uri WHERE a.paperid='" + paper + "'"
        curs.execute(q)
        rows = curs.fetchall()
        try:
            paper_response.append({'title': rows[0][1], 'uri': paper, 'abstract': rows[0][0]})
        except IndexError:
            pass
    return paper_response

@app.route('/words/<string:term_type>/<int:ngram>/')
def get_words(term_type, ngram):
    return 'type: ' + term_type + '\nngram: ' + str(ngram)

@app.route('/auth/<string:author>/', methods=['GET'])
def get_auth_papes(author):
    conn = connect_to_db()
    cur = conn.cursor()
    q = "SELECT * FROM p2au where authorid='https://midasnetwork.us/people/" + author + "/'"
    cur.execute(q)
    # q = "SELECT DISTINCT a.paperid, a.abstract, a.paper_title FROM p2ab a, p2au b WHERE a.paperid=b.paperid AND b.authorid='?'"
    # cur.execute(q,author)
    ## If we need multiple variables, use cur.executemany()
    print(q)
    rows = cur.fetchall()
    import pdb;pdb.set_trace()
    papers = [paper['paperid'] for paper in rows]
    paper_response = get_paper_response(papers, conn)
    return jsonify(paper_response)

@app.route('/')
def search_terms():
    terms = request.args.getlist("terms")
    return 'test \n' + ' '.join(terms)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=105)