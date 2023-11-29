import project.queries.routes as routes
from flask import jsonify

import json

# def test_bad_test():
#     assert 1+1 == 3

def test_author_pull(test_client):
    query_params= { "authors": ["https://midasnetwork.us/people/logan-brooks/", "https://midasnetwork.us/people/joseph-lemaitre/"],
                    "orgs": ["https://midasnetwork.us/organizations/17"],
                    "dates": {"start": 2021, "end": 2023}}

    expected_result = ["https://midasnetwork.us/people/ajitesh-srivastava/",
                       "https://midasnetwork.us/people/alessandro-vespignani/",
                       "https://midasnetwork.us/people/alex-perkins/",
                       "https://midasnetwork.us/people/alison-hill/",
                       "https://midasnetwork.us/people/alvaro-castro-rivadeneira/",
                       "https://midasnetwork.us/people/ana-pastore-y-piontti/",
                       "https://midasnetwork.us/people/ariane-stark/",
                       "https://midasnetwork.us/people/b-aditya-prakash/",
                       "https://midasnetwork.us/people/bijaya-adhikari/",
                       "https://midasnetwork.us/people/eamon-odea/",
                       "https://midasnetwork.us/people/elizabeth-lee/",
                       "https://midasnetwork.us/people/estee-cramer/",
                       "https://midasnetwork.us/people/evan-ray/",
                       "https://midasnetwork.us/people/geoffrey-fairchild/",
                       "https://midasnetwork.us/people/graham-gibson/",
                       "https://midasnetwork.us/people/guido-espana/",
                       "https://midasnetwork.us/people/james-turtle/",
                       "https://midasnetwork.us/people/jeffrey-shaman/",
                       "https://midasnetwork.us/people/jiaming-cui/",
                       "https://midasnetwork.us/people/john-drake/",
                       "https://midasnetwork.us/people/joseph-lemaitre/",
                       "https://midasnetwork.us/people/justin-lessler/",
                       "https://midasnetwork.us/people/lauren-gardner/",
                       "https://midasnetwork.us/people/lauren-meyers/",
                       "https://midasnetwork.us/people/lindsay-keegan/",
                       "https://midasnetwork.us/people/logan-brooks/",
                       "https://midasnetwork.us/people/marisa-eisenberg/",
                       "https://midasnetwork.us/people/matteo-chinazzi/",
                       "https://midasnetwork.us/people/matthew-biggerstaff/",
                       "https://midasnetwork.us/people/michael-johansson/",
                       "https://midasnetwork.us/people/michael-lachmann/",
                       "https://midasnetwork.us/people/michal-ben-nun/",
                       "https://midasnetwork.us/people/nicholas-reich/",
                       "https://midasnetwork.us/people/nutcha-wattanachit/",
                       "https://midasnetwork.us/people/quanquan-gu/",
                       "https://midasnetwork.us/people/rachel-jayne-oidtman/",
                       "https://midasnetwork.us/people/rachel-slayton/",
                       "https://midasnetwork.us/people/sean-cavany/",
                       "https://midasnetwork.us/people/sean-moore/",
                       "https://midasnetwork.us/people/sebastian-funk/",
                       "https://midasnetwork.us/people/sen-pei/",
                       "https://midasnetwork.us/people/shaun-truelove/",
                       "https://midasnetwork.us/people/spencer-fox/",
                       "https://midasnetwork.us/people/spencer-woody/",
                       "https://midasnetwork.us/people/stephen-lauer/",
                       "https://midasnetwork.us/people/stephen-turner/",
                       "https://midasnetwork.us/people/steven-riley/",
                       "https://midasnetwork.us/people/teresa-yamana/",
                       "https://midasnetwork.us/people/yangquan-chen/",
                       "https://midasnetwork.us/people/yanting-zhao/",
                       "https://midasnetwork.us/people/yuxin-huang/"
                   ]
    

    response = test_client.get('/authors/overlap/',
                     json=query_params)
    assert response.status_code == 200
    assert json.loads(response.data.decode('utf-8')) == expected_result

