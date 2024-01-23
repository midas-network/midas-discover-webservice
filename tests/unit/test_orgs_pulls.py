import project.queries.routes as routes

import json


def run_orgs_pull(test_client, query_params):
    response = test_client.post('/intersection/organizations/',
                    json=query_params)
    return response

def test_orgs_grants(test_client):
    queries = {
        'just_grants': {
            'query_params': {"grants": {"grantList": ["1R01AI151176-01"]}},
            'expected_result': [
                {
                    "id": "https://midasnetwork.us/organizations/229",
                    "name": "College of Natural Sciences"
                },
                {
                    "id": "https://midasnetwork.us/organizations/230",
                    "name": "Department of Integrative Biology"
                },
                {
                    "id": "https://midasnetwork.us/organizations/228",
                    "name": "University of Texas at Austin"
                }
            ]
        },
        'dates_and_grants': {
            'query_params': {"grants": {"dates": {"start": 2017, "end": 2019}, "grantList": ["1R01GM123007"]}},
            'expected_result': [
                {
                    "id": "https://midasnetwork.us/organizations/56",
                    "name": "Department of Biology"
                },
                {
                    "id": "https://midasnetwork.us/organizations/291",
                    "name": "Epidemiology and Biostatistics"
                },
                {
                    "id": "https://midasnetwork.us/organizations/289",
                    "name": "Indiana University Bloomington"
                },
                {
                    "id": "https://midasnetwork.us/organizations/172",
                    "name": "Odum School of Ecology"
                },
                {
                    "id": "https://midasnetwork.us/organizations/55",
                    "name": "Georgetown University"
                },
                {
                    "id": "https://midasnetwork.us/organizations/177",
                    "name": "Rohani Lab"
                },
                {
                    "id": "https://midasnetwork.us/organizations/168",
                    "name": "University of Georgia"
                },
                {
                    "id": "https://midasnetwork.us/organizations/290",
                    "name": "School of Public Health"
                }
            ]
        }
    }

    for test_type, details in queries.items():
        print('Test for ' + test_type.replace('_',' '))
        response = run_orgs_pull(test_client, details['query_params'])
        assert response.status_code == 200
        assert json.loads(response.data.decode('utf-8')) == details['expected_result']

def test_orgs_keywords(test_client):
    query_params = {"keywords": ["Pertussis", "Cells, Cultured", "Algorithms", "Policy Making"]}
    expected_result = [
            {
                "id": "https://midasnetwork.us/organizations/58",
                "name": "Harvard University"
            },
            {
                "id": "https://midasnetwork.us/organizations/61",
                "name": "Harvard T.H. Chan School of Public Health"
            },
            {
                "id": "https://midasnetwork.us/organizations/64",
                "name": "Department of Epidemiology"
            }
    ]

    response = run_orgs_pull(test_client, query_params)
    assert response.status_code == 200
    assert json.loads(response.data.decode('utf-8')) == expected_result

def test_orgs_papers(test_client):
    queries = {
        'just_papers': {
            'query_params': {"papers": {"paperList": ["https://midasnetwork.us/papers/Flying-phones-and-flu-Anonymized-call-records-suggest-that-Keflavik-International-Airport-introduced-pandemic-H1N1-into-Iceland-in-2009"]}},
            'expected_result': [
                {
                    "id": "https://midasnetwork.us/organizations/126",
                    "name": "U.S. Department of Health and Human Services"
                }
            ]
        },
        'dates_and_papers': {
            'query_params': {"papers": {"dates": {"start": 2017, "end": 2020}, "paperList": ["https://midasnetwork.us/papers/Flying-phones-and-flu-Anonymized-call-records-suggest-that-Keflavik-International-Airport-introduced-pandemic-H1N1-into-Iceland-in-2009"]}},
            'expected_result': [
                {
                    "id": "https://midasnetwork.us/organizations/126",
                    "name": "U.S. Department of Health and Human Services"
                }
            ]
        }
    }

    for test_type, details in queries.items():
        print('Test for ' + test_type.replace('_',' '))
        response = run_orgs_pull(test_client, details['query_params'])
        assert response.status_code == 200
        assert json.loads(response.data.decode('utf-8')) == details['expected_result']


def test_orgs_person(test_client):
    query_params = {"people": ["https://midasnetwork.us/people/nao-yamamoto/"]}
    expected_result = [
            {
                "id": "https://midasnetwork.us/organizations/287",
                "name": "Arizona State University"
            }
    ]

    response = run_orgs_pull(test_client, query_params)
    assert response.status_code == 200
    assert json.loads(response.data.decode('utf-8')) == expected_result