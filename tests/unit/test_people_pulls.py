import project.queries.routes as routes

import json


def run_people_pull(test_client, query_params):
    response = test_client.post('/intersection/people/',
                    json=query_params)
    return response
    
def test_people_coauthor(test_client):
    query_params = {"people": ["https://midasnetwork.us/people/angkana-huang/",
                                  "https://midasnetwork.us/people/donald-burke/"]}
    expected_result = [
            {
                "id": "https://midasnetwork.us/people/angkana-huang/",
                "name": "Angkana Huang"
            },
            {
                "id": "https://midasnetwork.us/people/derek-cummings/",
                "name": "Derek A.T. Cummings"
            },
            {
                "id": "https://midasnetwork.us/people/isabel-rodriguez-barraquer/",
                "name": "Isabel Rodriguez-Barraquer"
            },
            {
                "id": "https://midasnetwork.us/people/henrik-salje/",
                "name": "Henrik Salje"
            },
            {
                "id": "https://midasnetwork.us/people/amy-wesolowski/",
                "name": "Amy Wesolowski"
            },
            {
                "id": "https://midasnetwork.us/people/justin-lessler/",
                "name": "Justin Lessler"
            },
            {
                "id": "https://midasnetwork.us/people/donald-burke/",
                "name": "Donald S Burke"
            }
        ]

    response = run_people_pull(test_client, query_params)
    assert response.status_code == 200
    assert json.loads(response.data.decode('utf-8')) == expected_result


def test_people_grants(test_client):
    queries = {
        'just_grants': {
            'query_params': {"grants": {"grantList": ["5U54GM088491.5077", "1642174"]}},
            'expected_result': [ 
                    {
                        "id": "https://midasnetwork.us/people/derek-cummings/",
                        "name": "Derek A.T. Cummings"
                    }
                ]
        },
        'dates_and_grants': {
            'query_params': {"grants": {"dates": {"start": 2019, "end": 2021}, "grantList": ["5U54GM088558.5069"]}},
            'expected_result': [
                    {
                        "id": "https://midasnetwork.us/people/william-hanage/",
                        "name": "William Hanage"
                    }
            ]
        }
    }

    for test_type, details in queries.items():
        print('Test for ' + test_type.replace('_',' '))
        response = run_people_pull(test_client, details['query_params'])
        assert response.status_code == 200
        assert json.loads(response.data.decode('utf-8')) == details['expected_result']


def test_people_keywords(test_client):
    query_params = {"keywords": ["Family planning"]}
    expected_result = [
            {
                "id": "https://midasnetwork.us/people/ramzi-alsallaq/",
                "name": "Ramzi A Alsallaq"
            },
            {
                "id": "https://midasnetwork.us/people/tadele-adal/",
                "name": "Tadele Girum Adal"
            }
    ]

    response = run_people_pull(test_client, query_params)
    assert response.status_code == 200
    assert json.loads(response.data.decode('utf-8')) == expected_result


def test_people_org(test_client):
    query_params = {"organizations": ["https://midasnetwork.us/organizations/594"]}
    expected_result = [
            {
                "id": "https://midasnetwork.us/people/mohammad-al-mamun/",
                "name": "Mohammad A Al-Mamun"
            },
            {
                "id": "https://midasnetwork.us/people/natallia-katenka/",
                "name": "Natallia V. Katenka"
            }
    ]

    response = run_people_pull(test_client, query_params)
    assert response.status_code == 200
    assert json.loads(response.data.decode('utf-8')) == expected_result


def test_people_papers(test_client):
    queries = {
        'just_papers': {
            'query_params': {"papers": {"paperList": ["https://midasnetwork.us/papers/A-new-framework-and-software-to-estimate-time-varying-reproduction-numbers-during-epidemics",
                                                      "https://midasnetwork.us/papers/Unraveling-the-drivers-of-MERS-CoV-transmission",
                                                      "https://midasnetwork.us/papers/Estimating-Dengue-Transmission-Intensity-from-Case-Notification-Data-from-Multiple-Countries"]}},
            'expected_result': [
                    {
                        "id": "https://midasnetwork.us/people/simon-cauchemez/",
                        "name": "Simon Cauchemez"
                    },
                    {
                        "id": "https://midasnetwork.us/people/neil-ferguson/",
                        "name": "Neil M Ferguson"
                    }
            ]
        },
        'dates_and_papers': {
            'query_params': {"papers": {"dates": {"start": 2021}, "paperList": ["https://midasnetwork.us/papers/A-new-framework-and-software-to-estimate-time-varying-reproduction-numbers-during-epidemics"]}},
            'expected_result': [
                    {
                        "id": "https://midasnetwork.us/people/simon-cauchemez/",
                        "name": "Simon Cauchemez"
                    },
                    {
                        "id": "https://midasnetwork.us/people/neil-ferguson/",
                        "name": "Neil M Ferguson"
                    },
                    {
                        "id": "https://midasnetwork.us/people/christophe-fraser/",
                        "name": "Christophe Fraser"
                    }
            ]
        }
    }

    for test_type, details in queries.items():
        print('Test for ' + test_type.replace('_',' '))
        response = run_people_pull(test_client, details['query_params'])
        assert response.status_code == 200
        assert json.loads(response.data.decode('utf-8')) == details['expected_result']

