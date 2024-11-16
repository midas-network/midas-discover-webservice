import requests
import json

url = "http://localhost:5000/orgHierarchy"
# check to see if the endpoint is active

try:
    response = requests.get(url)
    data = response.json()

    # make a duplicate dictionary, but remove https://midasnetwork.us/organizations/ from the keys
    new_data = {}
    for key in data:
        new_key = key.replace("https://midasnetwork.us/organizations/", "")
        new_data[new_key] = data[key]

    # write to midas-viz-data
    with open("midas-viz-data/HIERARCHY.json", "w") as outfile:
        json.dump(new_data, outfile)
except requests.exceptions.RequestException as e:
    print(e)
    exit()


