from datetime import datetime
import os
import sqlite3
import time

import configparser
from decouple import config
import numpy as np
import pandas as pd

from riverobjs.fields import Fields

"""
Process data from midas-api-data and midas-viz-data folders for webservice database.
    Run update_midas_data.py before this program.
    
    Then upload midasDB to GitHub @ midas-network/midas-discover-webservice
"""

MIN_YEAR = 2012
MAX_YEAR = 2022
INPUT_DIR = 'midas-api-data'
# OUTPUT_DIR = 'midas-db-files'

df_map = {
    'grant_to_paper_df': 'g2p',
    'grant_to_person_df': 'g2a',
    'org_relations': 'org_relations',
    'paper_to_author_df': 'p2au',
    'paper_details': 'pdetails',
    'org_details': 'odetails',
    'grant_details': 'gdetails',
    'author_details': 'adetails',
    'paper_to_org_df': 'p2org',
    'paper_to_source_df': 'p2s',
    'paper_counts_df': 'pcount'
}


def print_time(start_time, what):
    stop_time = time.perf_counter()
    print(f'{what} took {stop_time - start_time:0.4f} seconds')
    return stop_time


def check_date(date):
    if type(date) == float:
        return date
    else:
        return datetime.strptime(date, '%m/%d/%Y').strftime('%Y')


def get_db():
    app_config = configparser.ConfigParser()
    app_config.read('config.ini')
    return app_config[config('user')]['database_location']


def make_grants_dfs():
    grants = pd.read_json(os.path.join(INPUT_DIR, 'GRANTS_ENDPOINT.json'))

    grant_to_person_df = pd.DataFrame(columns=['authorid', 'grantid'])
    grant_to_paper_df = pd.DataFrame(columns=['paperid', 'grantid'])
    grant_details = pd.DataFrame(columns=['grantid', 'title', 'startdate', 'enddate'])

    for grant_idx in range(0, len(grants)):
        grantid = grants.iloc[grant_idx]['grantID']
        grant_title = grants.iloc[grant_idx]['title']
        startdate = check_date(grants.iloc[grant_idx]['startDate'])
        enddate = check_date(grants.iloc[grant_idx]['endDate'])
        grant_details = pd.concat([grant_details, pd.DataFrame.from_records(
            {'grantid': grantid, 'title': grant_title, 'startdate': startdate, 'enddate': enddate}, index=[0])],
                                  ignore_index=True)
        affiliated_people = grants.iloc[grant_idx]['peopleAffiliatedWithGrant'] + list(
            set(grants.iloc[grant_idx]['grantPrincipalInvestigators']) - set(
                grants.iloc[grant_idx]['peopleAffiliatedWithGrant']))
        for person in affiliated_people:
            grant_to_person_df = pd.concat(
                [grant_to_person_df, pd.DataFrame.from_records({'authorid': person, 'grantid': grantid}, index=[0])],
                ignore_index=True)
        for paper in grants.iloc[grant_idx]['papersAffiliatedWithGrant']:
            grant_to_paper_df = pd.concat(
                [grant_to_paper_df, pd.DataFrame.from_records({'paperid': paper, 'grantid': grantid}, index=[0])],
                ignore_index=True)

    return (grant_to_paper_df, grant_to_person_df, grant_details)


def make_org_link_dfs():
    org_dicts = []
    org_info = []

    orgs = pd.read_json(os.path.join(INPUT_DIR, 'ORGANIZATIONS_ENDPOINT.json'))
    orgs.loc[orgs['type'] == '', 'type'] = '#NotSpecified'

    for org_idx, org in orgs.iterrows():
        uri = org['uri']
        name = org['name']
        otype = org['type']
        parent = org['parent']

        org_dicts.append({'orgid': uri, 'rel_id': uri, 'rel_type': otype})

        top_level = uri
        while parent != '':
            ptype = orgs[(orgs['uri'] == parent)]['type'].to_string(index=False, na_rep='')
            pdict = {'orgid': uri, 'rel_id': parent, 'rel_type': ptype}
            org_dicts.append(pdict)
            top_level = parent
            parent = orgs[(orgs['uri'] == parent)]['parent'].to_string(index=False, na_rep='')

        org_info.append({'orgid': uri, 'org_name': name, 'top_level': top_level})

    org_relations = pd.DataFrame.from_records(org_dicts)
    org_details = pd.DataFrame.from_records(org_info)

    return (org_relations, org_details)


def connect_papers(org_relations):
    missing_orgs = []
    author_info = set()
    auth_inserts = []
    org_inserts = []
    org_all_inserts = []

    paper_to_source_df = pd.DataFrame(
        columns=['paperid', 'Division', 'Department', 'College', 'University', 'Hospital', 'School', 'Program',
                 'Laboratory', 'Institute', 'Center', 'GovernmentAgency', 'NotSpecified'])
    paper_to_author_df = pd.DataFrame(columns=['paperid', 'authorid'])
    paper_to_org_df = pd.DataFrame(columns=['paperid', 'orgid'])
    author_details = pd.DataFrame(columns=['authorid', 'author_name', 'orgid'])

    people = pd.read_json(os.path.join(INPUT_DIR, 'PEOPLE_ENDPOINT.json'))

    for person_idx, person in people.iterrows():
        good_org = True

        org = person['parent']
        if not (org_relations['orgid'] == org).any():
            missing_orgs.append(org)
            good_org = False
            org = ''
        else:
            org_type = org_relations.loc[
                np.logical_and(org_relations['orgid'] == org, org_relations['rel_id'] == org), ['rel_type']]
            otype = org_type.to_string(index=False, na_rep='').split('#')[1]

            parents = org_relations.loc[org_relations['orgid'] == org, ['rel_id', 'rel_type']]

            org_hierarchy = {}
            for parent_idx, parent in parents.iterrows():
                orgid = parent.get('rel_id')
                org_type = parent.get('rel_type').split('#')[1]
                org_hierarchy[org_type] = orgid

        author = person['uri']
        author_name = person['name']
        author_info.add(author + '===' + author_name + '===' + org)

        for pub in person['publications']:
            auth_inserts.append({'paperid': pub, 'authorid': author})

            if good_org:
                org_insert = {'paperid': pub}
                org_insert.update(org_hierarchy)
                org_inserts.append(org_insert)
                for org in org_hierarchy.values():
                    org_all_inserts.append({'paperid': pub, 'orgid': org})

    author_info_df = []
    for person in author_info:
        author_info_df.append({'authorid': person.split('===')[0], 'author_name': person.split('===')[1],
                               'orgid': person.split('===')[2]})

    paper_to_author_df = pd.DataFrame.from_records(auth_inserts)
    paper_to_source_df = pd.DataFrame.from_records(org_inserts)
    paper_to_org_df = pd.DataFrame.from_records(org_all_inserts)
    author_details = pd.DataFrame.from_records(author_info_df)

    return (paper_to_author_df, paper_to_source_df, paper_to_org_df, author_details)


def fill_words(paper_counts_df, paper_details, min_year, max_year):
    papers = pd.read_json(os.path.join(INPUT_DIR, 'PAPERS_ENDPOINT.json'))

    pprdicts = []
    for paper_idx, paper in papers.iterrows():
        try:
            year = int(paper['datePublished'][-4:])
        except TypeError:
            try:
                year = int(paper['articleDate'][-4:])
            except TypeError:
                continue
        # if year < min_year or year > max_year:
        #     continue

        uri = paper['uri']
        title = paper['title']
        abstract = paper['paperAbstract']
        if type(abstract) != str:
            abstract = ''
        mesh_content = paper[Fields.MESH_TERM.value]
        pubmed_kw_content = paper[Fields.PUBMED_KEYWORD.value]

        goodMesh = True
        goodPubmed = True
        if isinstance(mesh_content, float):
            goodMesh = False
        if isinstance(pubmed_kw_content, float):
            goodPubmed = False

        paper_details = pd.concat([paper_details, pd.DataFrame.from_records(
            {'paperid': uri, 'title': title, 'abstract': abstract, 'year': year}, index=[0])], ignore_index=True)

        if goodMesh:
            for term in mesh_content:
                pprdicts.append({'paperid': uri, 'term': term, 'count': 1, 'ngram': 1, 'field': Fields.MESH_TERM.value})
        if goodPubmed:
            for term in pubmed_kw_content:
                pprdicts.append(
                    {'paperid': uri, 'term': term, 'count': 1, 'ngram': 1, 'field': Fields.PUBMED_KEYWORD.value})

    paper_counts_df = pd.concat([paper_counts_df, pd.DataFrame.from_records(pprdicts)], ignore_index=True)

    return (paper_counts_df, paper_details)


def make_table(conn, table_name, table_df):
    cur = conn.cursor()

    drop_q = 'DROP TABLE IF EXISTS ' + table_name
    cur.execute(drop_q)

    table_df.to_sql(table_name, conn, index=False)


def main():
    start_time = time.perf_counter()

    grant_to_paper_df, grant_to_person_df, grant_details = make_grants_dfs()
    start_time = print_time(start_time, "make_grants_dfs")

    org_relations, org_details = make_org_link_dfs()
    start_time = print_time(start_time, "make_org_link_dfs")

    (paper_to_author_df, paper_to_source_df, paper_to_org_df, author_details) = connect_papers(org_relations)
    start_time = print_time(start_time, "connect_papers")

    paper_counts_df = pd.DataFrame(columns=['paperid', 'term', 'count', 'ngram', 'field'])
    paper_details = pd.DataFrame(columns=['paperid', 'title', 'abstract', 'year'])
    (paper_counts_df, paper_details) = fill_words(paper_counts_df, paper_details, MIN_YEAR, MAX_YEAR)
    start_time = print_time(start_time, "fill_words")

    db_loc = get_db()
    conn = sqlite3.connect(db_loc)
    for df, table in df_map.items():
        if len(eval(df)) > 0:
            make_table(conn, table, eval(df))
        else:
            print("Error creating " + table + " table.")
    print_time(start_time, "Saving the tables")


if __name__ == '__main__':
    main()
