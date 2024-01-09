import project.queries.routes as routes

import json


def run_papers_pull(test_client, query_params):
    response = test_client.post('/intersection/papers/',
                    json=query_params)
    return response

def test_paper_authors(test_client):
    query_params = {"authors": ["https://midasnetwork.us/people/rachel-slayton/", "https://midasnetwork.us/people/pragati-prasad/"]}
    expected_result = [
            {
                "id": "https://midasnetwork.us/papers/Modeling-strategies-for-the-allocation-of-SARS-CoV-2-vaccines-in-the-United-States",
                "name": "Modeling strategies for the allocation of SARS-CoV-2 vaccines in the United States."
            },
            {
                "id": "https://midasnetwork.us/papers/SARS-CoV-2-Transmission-From-People-Without-COVID-19-Symptoms",
                "name": "SARS-CoV-2 Transmission From People Without COVID-19 Symptoms."
            },
            {
                "id": "https://midasnetwork.us/papers/Cruise-ship-travel-in-the-era-of-COVID-19-A-summary-of-outbreaks-and-a-model-of-public-health-interventions",
                "name": "Cruise ship travel in the era of COVID-19: A summary of outbreaks and a model of public health interventions."
            }
    ]

    response = run_papers_pull(test_client, query_params)
    assert response.status_code == 200
    assert json.loads(response.data.decode('utf-8')) == expected_result

def test_paper_orgs(test_client):
    query_params = {"orgs": ["https://midasnetwork.us/organizations/666"]}
    expected_result = [
            {
                "id": "https://midasnetwork.us/papers/System-Dynamics-Modeling-of-Within-Host-Viral-Kinetics-of-Coronavirus-SARS-CoV-2",
                "name": "System Dynamics Modeling of Within-Host Viral Kinetics of Coronavirus (SARS CoV-2)"
            },
            {
                "id": "https://midasnetwork.us/papers/Formalization-of-an-environmental-model-using-formal-concept-analysis---FCA",
                "name": "Formalization of an environmental model using formal concept analysis - FCA"
            },
            {
                "id": "https://midasnetwork.us/papers/A-Simple-Mechanism-of-the-Biogenic-Spreading-Among-Extrasolar-Systems",
                "name": "A Simple Mechanism of the Biogenic Spreading Among Extrasolar Systems"
            }
    ]

    response = run_papers_pull(test_client, query_params)
    assert response.status_code == 200
    assert json.loads(response.data.decode('utf-8')) == expected_result

def test_paper_keywords(test_client):
    query_params = {"keywords": ["Uterine Cervical Neoplasms","Trichomonas Vaginitis"]}
    expected_result = [
            {
                "id": "https://midasnetwork.us/papers/The-dawn-of-novel-STI-prevention-methods-modelling-potential-unintended-effects-of-changes-in-cervical-cancer-screening-guidelines-on-trichomoniasis",
                "name": "The dawn of novel STI prevention methods: modelling potential unintended effects of changes in cervical cancer screening guidelines on trichomoniasis."
            }
    ]

    response = run_papers_pull(test_client, query_params)
    assert response.status_code == 200
    assert json.loads(response.data.decode('utf-8')) == expected_result

def test_paper_grants(test_client):
    queries = {
        'just_grants': {
            'query_params': {"grants": {"grantList": ["5R01DA012831"]}},
            'expected_result': [
                    {
                        "id": "https://midasnetwork.us/papers/ergmuserterms-A-Template-Package-for-Extending-statnet",
                        "name": "ergm.userterms: A Template Package for Extending statnet."
                    }
            ]
        },
        'dates_and_grants': {
            'query_params': {"grants": {"dates": {"end": 2021}, "grantList": ["5R01DA012831"]}},
            'expected_result': [
                    {
                        "id": "https://midasnetwork.us/papers/ergmuserterms-A-Template-Package-for-Extending-statnet",
                        "name": "ergm.userterms: A Template Package for Extending statnet."
                    }
            ]
        }
    }

    for test_type, details in queries.items():
        print('Test for ' + test_type.replace('_',' '))
        response = run_papers_pull(test_client, details['query_params'])
        assert response.status_code == 200
        assert json.loads(response.data.decode('utf-8')) == details['expected_result']
