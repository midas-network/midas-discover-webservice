import project.queries.routes as routes

import json


def run_grants_pull(test_client, query_params):
    response = test_client.post('/intersection/grants/',
                    json=query_params)
    return response

def test_grants_people(test_client):
    query_params = {"people": ["https://midasnetwork.us/people/katriona-shea/", "https://midasnetwork.us/people/matthew-ferrari/"]}
    expected_result = [
            {
                "id": "1514704",
                "name": "Value Of Information And Structured Decision-Making For Management Of Ebola"
            },
            {
                "id": "1R01TW009500-01",
                "name": "Linking Models and Policy: Using Active Adaptive Management for Optimal Control of Disease Outbreaks"
            },
            {
                "id": "1911962",
                "name": "Us-Uk COLLAB: Adaptive Surveillance and Control for Endemic Disease Elimination"
            }
    ]

    response = run_grants_pull(test_client, query_params)
    assert response.status_code == 200
    assert json.loads(response.data.decode('utf-8')) == expected_result

def test_grants_keywords(test_client):
    query_params = {"keywords": ["Spike Glycoprotein, Coronavirus"]}
    expected_result = [
        {
            "id": "5R01GM120624",
            "name": "Computational Studies Of Virus-Host Interactions Using METAGENOMICS Data And Applications"
        }
    ]

    response = run_grants_pull(test_client, query_params)
    assert response.status_code == 200
    assert json.loads(response.data.decode('utf-8')) == expected_result

def test_grants_orgs(test_client):
    query_params = {"organizations": ["https://midasnetwork.us/organizations/172", "https://midasnetwork.us/organizations/177"]}
    expected_result = [
            {
                "id": "1R01GM123007",
                "name": "Vaccine Hesitancy And Erosion Of Herd Immunity: Harnessing Big Data To Forecast Disease Re-Emergence"
            }
    ]

    response = run_grants_pull(test_client, query_params)
    assert response.status_code == 200
    assert json.loads(response.data.decode('utf-8')) == expected_result

def test_grants_papers(test_client):
    queries = {
        'just_papers': {
            'query_params': {"papers": {"paperList": ["https://midasnetwork.us/papers/Using-sero-epidemiology-to-monitor-disparities-in-vaccination-and-infection-with-SARS-CoV-2"]}},
            'expected_result': [
                    {
                        "id": "MIDASNI2020-5",
                        "name": "SERO-Epidemiological Modeling for COVID-19 Control"
                    }
            ]
        },
        'dates_and_papers': {
            'query_params': {"papers": {"dates": {"start": 2018, "end": 2021}, "paperList": ["https://midasnetwork.us/papers/Using-sero-epidemiology-to-monitor-disparities-in-vaccination-and-infection-with-SARS-CoV-2"]}},
            'expected_result': [
                    {
                        "id": "MIDASNI2020-5",
                        "name": "SERO-Epidemiological Modeling for COVID-19 Control"
                    }
            ]
        }
    }

    for test_type, details in queries.items():
        print('Test for ' + test_type.replace('_',' '))
        response = run_grants_pull(test_client, details['query_params'])
        assert response.status_code == 200
        assert json.loads(response.data.decode('utf-8')) == details['expected_result']