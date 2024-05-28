import json
import os.path

import requests

"""
This creates the .json files in the midas-api-data, and -viz-data directories

Copy the midas-viz-data files to the visualizer/data/webservice.  

Run this before running update_database.

"""
BASE_URL = "https://members.midasnetwork.us/midascontacts/query/"

VISUALIZER_SUFFIX = "/visualizer/all"

ENDPOINTS = {
    "PEOPLE_ENDPOINT": "people",
    "PAPERS_ENDPOINT": "papers",
    "GRANTS_ENDPOINT": "projects",
    "ORGANIZATIONS_ENDPOINT": "organizations"
}

ENDPOINT_KEYS = {
    "PEOPLE_ENDPOINT": "uri",
    "PAPERS_ENDPOINT": "uri",
    "GRANTS_ENDPOINT": "grantID",
    "ORGANIZATIONS_ENDPOINT": "uri"
}

API_KEY = ""
VIZ_OUTPUT_DIR = "midas-viz-data"
MIDAS_API_OUTPUT_DIR = "midas-api-data"

def fetch_api_key():
    if os.path.isfile("key.txt"):
        with open("key.txt") as keyFile:
            return keyFile.read()
    else:
        print("No key.txt file found.")


def fetch_visualizer_data(endpoint):
    return requests.get(BASE_URL + endpoint + VISUALIZER_SUFFIX + "?apiKey=" + fetch_api_key())

def json_array_to_dict_for_viz(endpoint, midasdata):
    if midasdata.status_code == 200:
        processed_data = {}
        for entry in midasdata.json():
            processed_data[str(entry[ENDPOINT_KEYS[endpoint]])] = entry
        return processed_data
    else:
        return None



def main():
    for endpoint in ENDPOINTS:
        data = fetch_visualizer_data(ENDPOINTS[endpoint])
        with open(os.path.join(MIDAS_API_OUTPUT_DIR, endpoint + ".json"), "w", encoding="utf-8") as outfile:
            json.dump(data.json(), outfile, indent=4, ensure_ascii=True)
        data = json_array_to_dict_for_viz(endpoint, data)
        if data is not None:
            with open(os.path.join(VIZ_OUTPUT_DIR, endpoint + ".json"), "w", encoding="utf-8") as outfile:
                json.dump(data, outfile, indent=4, ensure_ascii=True)



if __name__ == '__main__':
    main()
