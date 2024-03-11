from datetime import datetime
import itertools
import os
import time

import numpy as np
import pandas as pd

from fields import Fields


"""Process data from MIDAS for webservice database.
    Run update_midas_data.py before this program.

    Produces several csvs to be uploaded to the midas database.
    grant_2_paper.csv    =>  g2p
    grant_2_author.csv   =>  g2a
    org_relations.csv    =>  org_relations
    paper_2_abstract.csv => p2a
    paper_details.csv    => pdetails
    org_details.csv      => odetails
    grant_details.csv    => gdetails
    author_details.csv   => adetails
    paper_2_author.csv   => p2au
    paper_2_org.csv      => p2org
    paper_2_source.csv   => p2s
    paper_count.csv      => pcount 
"""

MIN_YEAR = 2012
MAX_YEAR = 2022
INPUT_DIR = 'midas-api-data'
OUTPUT_DIR = 'midas-db-files'


def print_time(start_time):
    stop_time = time.perf_counter()
    print(f"Took {stop_time-start_time:0.4f} seconds")
    return stop_time

def check_date(date):
    if type(date) == float:
        return date
    else:
        return datetime.strptime(date,'%m/%d/%Y').strftime('%Y')

def make_grants_dfs():
    grants = pd.read_json(os.path.join(INPUT_DIR, 'GRANTS_ENDPOINT.json'))
    
    grant_to_person_df = pd.DataFrame(columns=['authorid', 'grantid'])
    grant_to_paper_df = pd.DataFrame(columns=['paperid', 'grantid'])
    grant_details = pd.DataFrame(columns=['grantid', 'title', 'startdate', 'enddate'])


    for grant_idx in range(0,len(grants)):
        grantid = grants.iloc[grant_idx]["grantID"]
        grant_title = grants.iloc[grant_idx]["title"]
        startdate = check_date(grants.iloc[grant_idx]['startDate'])
        enddate = check_date(grants.iloc[grant_idx]['endDate'])
        grant_details = pd.concat([grant_details, pd.DataFrame.from_records({'grantid': grantid, 'title': grant_title, 'startdate': startdate, 'enddate': enddate}, index=[0])], ignore_index=True)
        affiliated_people = grants.iloc[grant_idx]['peopleAffiliatedWithGrant'] + list(set(grants.iloc[grant_idx]["grantPrincipalInvestigators"]) - set(grants.iloc[grant_idx]['peopleAffiliatedWithGrant']))
        for person in affiliated_people:
            grant_to_person_df = pd.concat([grant_to_person_df, pd.DataFrame.from_records({'authorid': person, 'grantid': grantid}, index=[0])], ignore_index=True)
        for paper in grants.iloc[grant_idx]['papersAffiliatedWithGrant']:
            grant_to_paper_df = pd.concat([grant_to_paper_df, pd.DataFrame.from_records({'paperid': paper, 'grantid': grantid}, index=[0])], ignore_index=True)
    
    return (grant_to_paper_df, grant_to_person_df, grant_details)


def main():
    start_time = time.perf_counter()
    start_time = print_time(start_time)
    
    grant_to_paper_df, grant_to_person_df, grant_details = make_grants_dfs()
    start_time = print_time(start_time)


if __name__ == "__main__":
    main()